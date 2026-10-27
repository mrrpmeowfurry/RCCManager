package games

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"rccmanager/internal/config"
	"rccmanager/internal/rcc"
)

func testManager(t *testing.T, url string, ttl time.Duration) *Manager {
	t.Helper()
	return New(&config.Config{
		StateFile:      filepath.Join(t.TempDir(), "state.json"),
		PlayerCountTTL: ttl,
	}, rcc.NewClient("test", url, "secret"))
}

func countResponse(w http.ResponseWriter, count int) {
	w.Header().Set("Content-Type", "text/xml")
	fmt.Fprintf(w, `<Envelope><Body><ExecuteResponse><ExecuteResult><LuaValue><type>LUA_TNUMBER</type><value>%d</value></LuaValue></ExecuteResult></ExecuteResponse></Body></Envelope>`, count)
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for !condition() {
		select {
		case <-deadline.C:
			t.Fatal("condition did not become true")
		case <-tick.C:
		}
	}
}

func TestRunPlayerCountsRefreshesOnlyReadyServers(t *testing.T) {
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			JobID string `xml:"Body>Execute>jobID"`
		}
		if err := xml.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode Execute: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		calls.Add(1)
		switch req.JobID {
		case "public":
			countResponse(w, 3)
		case "private":
			countResponse(w, 0)
		default:
			t.Errorf("polled unexpected job %q", req.JobID)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer upstream.Close()
	m := testManager(t, upstream.URL, time.Hour)
	old := time.Now().Add(-2 * time.Hour)
	m.servers = map[string]*Server{
		"public":  {JobID: "public", PlaceID: 10, Ready: true, Players: 1, PlayersAt: old, LastSeen: old},
		"private": {JobID: "private", PlaceID: 10, Key: "vip-1", Ready: true, Players: 2, PlayersAt: old, LastSeen: old},
		"loading": {JobID: "loading", PlaceID: 20, PlayersAt: old},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		m.RunPlayerCounts(ctx)
	}()
	defer func() {
		cancel()
		<-done
	}()

	waitFor(t, func() bool {
		public, _ := m.Get("public")
		private, _ := m.Get("private")
		return public.PlayersAt.After(old) && private.PlayersAt.After(old)
	})
	counts := m.Players([]int64{10, 10, 20, 999})
	if counts[10] != 3 || counts[20] != 0 || counts[999] != 0 || len(counts) != 3 {
		t.Fatalf("cached totals = %v", counts)
	}
	public, _ := m.Get("public")
	private, _ := m.Get("private")
	if !public.LastSeen.After(old) || !private.LastSeen.Equal(old) {
		t.Fatalf("last seen changed incorrectly: public=%v private=%v", public.LastSeen, private.LastSeen)
	}
	if n, err := m.refreshPlayers(ctx, "public", time.Hour); err != nil || n != 3 {
		t.Fatalf("fresh count = %d, %v", n, err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("Execute calls = %d, want 2", got)
	}
}

func TestConcurrentPlayerRefreshesShareFreshCount(t *testing.T) {
	var calls atomic.Int32
	started := make(chan struct{}, 20)
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		started <- struct{}{}
		<-release
		countResponse(w, 7)
	}))
	defer upstream.Close()
	defer unblock()
	m := testManager(t, upstream.URL, time.Hour)
	m.servers["job"] = &Server{JobID: "job", PlaceID: 10, Ready: true}
	results := make(chan error, 20)
	for i := 0; i < cap(results); i++ {
		go func() {
			n, err := m.refreshPlayers(context.Background(), "job", time.Hour)
			if err == nil && n != 7 {
				err = fmt.Errorf("count = %d, want 7", n)
			}
			results <- err
		}()
	}
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("refresh never reached RCC")
	}
	unblock()
	for i := 0; i < cap(results); i++ {
		select {
		case err := <-results:
			if err != nil {
				t.Errorf("refresh: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("refresh did not finish")
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("Execute calls = %d, want 1", got)
	}
}

func TestPlayerCountFailurePreservesCacheAndRetries(t *testing.T) {
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprint(w, `<Envelope><Body><Fault><faultstring>temporarily unavailable</faultstring></Fault></Body></Envelope>`)
			return
		}
		countResponse(w, 4)
	}))
	defer upstream.Close()
	m := testManager(t, upstream.URL, 20*time.Millisecond)
	old := time.Now().Add(-time.Hour)
	m.servers["job"] = &Server{JobID: "job", PlaceID: 10, Ready: true, Players: 9, PlayersAt: old}
	if _, err := m.refreshPlayers(context.Background(), "job", 0); err == nil {
		t.Fatal("expected failed refresh")
	}
	s, _ := m.Get("job")
	if s.Players != 9 || !s.PlayersAt.Equal(old) {
		t.Fatalf("failed refresh changed cache: %+v", s)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		m.RunPlayerCounts(ctx)
	}()
	defer func() {
		cancel()
		<-done
	}()
	waitFor(t, func() bool { return m.Players([]int64{10})[10] == 4 })
	first, _ := m.Get("job")
	waitFor(t, func() bool {
		current, _ := m.Get("job")
		return current.PlayersAt.After(first.PlayersAt)
	})
}

func TestReaperVerifiesLiveCountBeforeClosingIdleServer(t *testing.T) {
	var counts atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Header.Get("SOAPAction") {
		case "http://roblox.com/GetAllJobs":
			fmt.Fprint(w, `<Envelope><Body><GetAllJobsResponse><GetAllJobsResult><Job><id>job</id></Job></GetAllJobsResult></GetAllJobsResponse></Body></Envelope>`)
		case "http://roblox.com/Execute":
			counts.Add(1)
			countResponse(w, 1)
		default:
			t.Errorf("unexpected RCC action %q", r.Header.Get("SOAPAction"))
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer upstream.Close()
	m := testManager(t, upstream.URL, time.Hour)
	m.cfg.IdleTimeout = time.Minute
	m.cfg.Lease = time.Hour
	now := time.Now()
	m.servers["job"] = &Server{
		JobID: "job", PlaceID: 10, Ready: true,
		Players: 0, PlayersAt: now, LastSeen: now.Add(-time.Hour),
		StartedAt: now.Add(-time.Hour), LeasedAt: now,
	}
	m.reap(context.Background())
	s, ok := m.Get("job")
	if !ok || s.Players != 1 || counts.Load() != 1 {
		t.Fatalf("reaper did not verify the live count: server=%+v exists=%v calls=%d", s, ok, counts.Load())
	}
}

func TestRunPlayerCountsCancelsStalledRefresh(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started <- struct{}{}
		<-release
		countResponse(w, 0)
	}))
	defer upstream.Close()
	defer close(release)
	m := testManager(t, upstream.URL, time.Hour)
	m.servers["job"] = &Server{JobID: "job", PlaceID: 10, Ready: true, Players: 9}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		m.RunPlayerCounts(ctx)
	}()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("refresh never reached RCC")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("count worker did not cancel its stalled RCC call")
	}
	if got := m.Players([]int64{10})[10]; got != 9 {
		t.Fatalf("canceled refresh changed count to %d", got)
	}
}
