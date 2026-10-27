package games

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"rccmanager/internal/config"
	"rccmanager/internal/rcc"
)

type Server struct {
	JobID     string    `json:"jobId"`
	PlaceID   int64     `json:"placeId"`
	Key       string    `json:"key,omitempty"`
	Port      int       `json:"port"`
	Address   string    `json:"address"`
	Ready     bool      `json:"ready"`
	Players   int       `json:"players"`
	StartedAt time.Time `json:"startedAt"`

	// saved, not shown
	LastSeen  time.Time `json:"lastSeen"`
	LeasedAt  time.Time `json:"leasedAt"`
	PlayersAt time.Time `json:"playersAt"`
}

var (
	ErrNoFreePort = errors.New("no free game server port in range")
	ErrNotFound   = errors.New("no such server")
)

type Manager struct {
	cfg *config.Config
	rcc *rcc.Client

	mu      sync.Mutex
	servers map[string]*Server
	rccUp   bool
	rccErr  string

	// Background polling and the reaper share count refreshes.
	playersMu sync.Mutex

	// one state file write at a time
	saveMu sync.Mutex
}

func New(cfg *config.Config, client *rcc.Client) *Manager {
	m := &Manager{
		cfg:     cfg,
		rcc:     client,
		servers: map[string]*Server{},
	}
	if err := m.load(); err != nil {
		log.Printf("cant load %s: %v", cfg.StateFile, err)
	}
	// was still starting when we died
	m.mu.Lock()
	for id, s := range m.servers {
		if !s.Ready {
			go m.waitReady(id)
		}
	}
	m.mu.Unlock()
	return m
}

func (m *Manager) Get(jobID string) (Server, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.servers[jobID]
	if !ok {
		return Server{}, false
	}
	return *s, true
}

func (m *Manager) List() []Server {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Server, 0, len(m.servers))
	for _, s := range m.servers {
		out = append(out, *s)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].StartedAt.Before(out[j].StartedAt)
	})
	return out
}

func (m *Manager) Count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.servers)
}

// what the last reaper pass saw
func (m *Manager) RCCStatus() (bool, string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.rccUp, m.rccErr
}

func (m *Manager) RCCURL() string {
	return m.rcc.URL
}

// a key makes a private job (vip-<id>, rsv-<id>), "" is the public one
type StartOpts struct {
	Key     string
	OwnerID int64
}

func (m *Manager) find(placeID int64, key string) *Server {
	for _, s := range m.servers {
		if s.PlaceID == placeID && s.Key == key {
			return s
		}
	}
	return nil
}

func (m *Manager) FindOrCreate(ctx context.Context, placeID int64, opts StartOpts) (Server, error) {
	m.mu.Lock()
	if s := m.find(placeID, opts.Key); s != nil {
		s.LastSeen = time.Now()
		out := *s
		m.mu.Unlock()
		return out, nil
	}

	port, err := m.freePortLocked()
	if err != nil {
		m.mu.Unlock()
		return Server{}, err
	}
	now := time.Now()
	prefix := "place" + strconv.FormatInt(placeID, 10) + "-"
	if opts.Key != "" {
		prefix += opts.Key + "-"
	}
	s := &Server{
		JobID:     prefix + rcc.RandomHex(8),
		PlaceID:   placeID,
		Key:       opts.Key,
		Port:      port,
		Address:   m.cfg.SitePublicAddress,
		StartedAt: now,
		LastSeen:  now,
		LeasedAt:  now,
	}
	// in the map before OpenJob or two play clicks grab the same port
	m.servers[s.JobID] = s
	m.mu.Unlock()

	script, err := m.gameServerScript()
	if err != nil {
		m.forget(s.JobID)
		return Server{}, err
	}

	octx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	_, err = m.rcc.OpenJob(octx, s.JobID, "gameserver", script, m.cfg.Lease,
		rcc.Number(placeID), rcc.Number(int64(port)), rcc.String(m.cfg.SiteInternalURL), rcc.String(opts.Key), rcc.Number(opts.OwnerID))
	if err != nil {
		m.forget(s.JobID)
		return Server{}, err
	}
	log.Printf("started %s on port %d", s.JobID, port)
	m.save()

	go m.waitReady(s.JobID)

	out, _ := m.Get(s.JobID)
	return out, nil
}

// gameserver.txt only defines start(), the global tells us the port got bound
func (m *Manager) gameServerScript() (string, error) {
	if m.cfg.GameServerScript == "" {
		return "", errors.New("GAMESERVER_SCRIPT is not set, this manager only renders")
	}
	lua, err := os.ReadFile(m.cfg.GameServerScript)
	if err != nil {
		return "", fmt.Errorf("cannot read the game server script: %w", err)
	}
	return string(lua) + "\r\nstart(...)\r\n_G.__nonerevReady = true\r\n", nil
}

// the client only polls PlaceLauncher 5 times, 2s apart
func (m *Manager) waitReady(jobID string) {
	deadline := time.Now().Add(m.cfg.ReadyTimeout)
	for time.Now().Before(deadline) {
		if _, ok := m.Get(jobID); !ok {
			return // closed meanwhile
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		vals, err := m.rcc.Execute(ctx, jobID, "ready", `return tostring(_G.__nonerevReady == true)`)
		cancel()
		if err == nil {
			if v, _ := rcc.FirstString(vals); v == "true" {
				m.mu.Lock()
				s, ok := m.servers[jobID]
				if ok {
					s.Ready = true
				}
				m.mu.Unlock()
				if ok {
					m.save()
					log.Printf("%s is ready", jobID)
				}
				return
			}
		} else if rcc.IsFault(err) && !m.rccHasJob(jobID) {
			// rcc dropped it, probably died loading the place
			log.Printf("%s died while starting: %v", jobID, err)
			m.forget(jobID)
			m.save()
			return
		}
		time.Sleep(time.Second)
	}
	log.Printf("%s took too long to start, closing it", jobID)
	_ = m.Close(context.Background(), jobID)
}

// unreachable counts as yes
func (m *Manager) rccHasJob(jobID string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	live, err := m.rcc.GetAllJobs(ctx)
	if err != nil {
		return true
	}
	for _, id := range live {
		if id == jobID {
			return true
		}
	}
	return false
}

func (m *Manager) Close(ctx context.Context, jobID string) error {
	if _, ok := m.Get(jobID); !ok {
		return ErrNotFound
	}
	cctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	if err := m.rcc.CloseJob(cctx, jobID); err != nil && !rcc.IsFault(err) {
		// rcc may still have it, keep it so the reaper retries
		return err
	}
	m.forget(jobID)
	m.save()
	log.Printf("closed %s", jobID)
	return nil
}

func (m *Manager) forget(jobID string) {
	m.mu.Lock()
	delete(m.servers, jobID)
	m.mu.Unlock()
}

// Cached counts include every job of the place, private ones too.
func (m *Manager) Players(placeIDs []int64) map[int64]int {
	out := make(map[int64]int, len(placeIDs))
	for _, id := range placeIDs {
		out[id] = 0
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	for _, s := range m.servers {
		if _, ok := out[s.PlaceID]; ok {
			out[s.PlaceID] += s.Players
		}
	}
	return out
}

// HTTP reads never wait for RCC, even while OpenJob or a render holds its slot.
func (m *Manager) RunPlayerCounts(ctx context.Context) {
	interval := m.cfg.PlayerCountTTL
	if interval <= 0 {
		interval = time.Second
	}
	t := time.NewTimer(0)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		for _, s := range m.List() {
			if ctx.Err() != nil {
				return
			}
			if s.Ready {
				// A failed refresh keeps the last successful count.
				_, _ = m.refreshPlayers(ctx, s.JobID, interval)
			}
		}
		t.Reset(interval)
	}
}

func (m *Manager) refreshPlayers(ctx context.Context, jobID string, maxAge time.Duration) (int, error) {
	m.playersMu.Lock()
	defer m.playersMu.Unlock()
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s, ok := m.Get(jobID)
	if !ok {
		return 0, ErrNotFound
	}
	if maxAge > 0 && time.Since(s.PlayersAt) < maxAge {
		return s.Players, nil
	}

	ectx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	vals, err := m.rcc.Execute(ectx, jobID, "players", `return #game:GetService("Players"):GetPlayers()`)
	if err != nil {
		return 0, err
	}
	n := 0
	if v, ok := rcc.FirstString(vals); ok {
		f, _ := strconv.ParseFloat(strings.TrimSpace(v), 64)
		if f > 0 {
			n = int(f)
		}
	}
	m.mu.Lock()
	if s, ok := m.servers[jobID]; ok {
		s.Players = n
		s.PlayersAt = time.Now()
		if n > 0 {
			s.LastSeen = time.Now()
		}
	}
	m.mu.Unlock()
	return n, nil
}

// GetAllJobs is the truth
func (m *Manager) RunReaper(ctx context.Context) {
	m.reap(ctx)
	t := time.NewTicker(m.cfg.ReapEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			m.reap(ctx)
		}
	}
}

func (m *Manager) reap(ctx context.Context) {
	jctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	live, err := m.rcc.GetAllJobs(jctx)
	cancel()

	m.mu.Lock()
	if err != nil {
		m.rccUp, m.rccErr = false, err.Error()
		m.mu.Unlock()
		return // dont mass close on a blip
	}
	m.rccUp, m.rccErr = true, ""
	liveSet := make(map[string]bool, len(live))
	for _, id := range live {
		liveSet[id] = true
	}

	var gone, idle, renew []string
	now := time.Now()
	for id, s := range m.servers {
		switch {
		case !liveSet[id]:
			// OpenJob may still be in flight
			if now.Sub(s.StartedAt) > 45*time.Second {
				gone = append(gone, id)
			}
		case s.Ready && now.Sub(s.LastSeen) > m.cfg.IdleTimeout:
			idle = append(idle, id)
		}
		if liveSet[id] && now.Sub(s.LeasedAt) > m.cfg.Lease/2 {
			renew = append(renew, id)
		}
	}
	for _, id := range gone {
		log.Printf("%s is gone, port %d is free again", id, m.servers[id].Port)
		delete(m.servers, id)
	}
	m.mu.Unlock()

	for _, id := range idle {
		// ask first, dont kick people on a blip
		n, err := m.refreshPlayers(ctx, id, 0)
		if err == nil && n == 0 {
			log.Printf("%s is empty, closing it", id)
			_ = m.Close(ctx, id)
		}
	}
	for _, id := range renew {
		rctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := m.rcc.RenewLease(rctx, id, m.cfg.Lease)
		cancel()
		if err != nil {
			log.Printf("couldnt renew %s: %v", id, err)
			continue
		}
		m.mu.Lock()
		if s, ok := m.servers[id]; ok {
			s.LeasedAt = time.Now()
		}
		m.mu.Unlock()
	}
	if len(gone)+len(idle) > 0 {
		m.save()
	}
}

// a crashed rcc leaves ports half dead, so really bind it. caller holds m.mu
func (m *Manager) freePortLocked() (int, error) {
	taken := make(map[int]bool, len(m.servers))
	for _, s := range m.servers {
		taken[s.Port] = true
	}
	for p := m.cfg.PortMin; p <= m.cfg.PortMax; p++ {
		if taken[p] {
			continue
		}
		if udpPortFree(p) {
			return p, nil
		}
	}
	return 0, ErrNoFreePort
}

func udpPortFree(port int) bool {
	c, err := net.ListenPacket("udp4", net.JoinHostPort("0.0.0.0", strconv.Itoa(port)))
	if err != nil {
		return false
	}
	c.Close()
	return true
}
