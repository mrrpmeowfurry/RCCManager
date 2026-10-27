package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"rccmanager/internal/config"
	"rccmanager/internal/games"
	"rccmanager/internal/rcc"
)

func TestCountEndpointsStayResponsiveWhileRCCIsBlocked(t *testing.T) {
	var calls atomic.Int32
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		calls.Add(1)
		started <- struct{}{}
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		fmt.Fprint(w, `<Envelope><Body><ExecuteResponse><ExecuteResult><LuaValue><type>LUA_TNUMBER</type><value>11</value></LuaValue></ExecuteResult></ExecuteResponse></Body></Envelope>`)
	}))
	defer upstream.Close()
	cfg := &config.Config{
		Password:       "test-token",
		StateFile:      filepath.Join(t.TempDir(), "state.json"),
		PlayerCountTTL: time.Hour,
	}
	data, err := json.Marshal(map[string]any{"servers": []games.Server{
		{JobID: "public", PlaceID: 10, Ready: true, Players: 3},
		{JobID: "private", PlaceID: 10, Key: "vip-1", Ready: true, Players: 7},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cfg.StateFile, data, 0600); err != nil {
		t.Fatal(err)
	}
	m := games.New(cfg, rcc.NewClient("test", upstream.URL, "secret"))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		m.RunPlayerCounts(ctx)
	}()
	defer func() {
		cancel()
		unblock()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("count worker did not stop")
		}
	}()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("count worker never reached RCC")
	}

	server := httptest.NewServer(New(cfg, m, nil, nil, "test").Handler())
	defer server.Close()
	client := &http.Client{Timeout: time.Second}
	results := make(chan error, 100)
	start := time.Now()
	for i := 0; i < cap(results); i++ {
		path := "/servers"
		if i%2 == 0 {
			path = "/players?placeIds=10,10,999"
		}
		go func(path string) {
			req, err := http.NewRequest(http.MethodGet, server.URL+path, nil)
			if err != nil {
				results <- err
				return
			}
			req.Header.Set("Authorization", "Bearer "+cfg.Password)
			resp, err := client.Do(req)
			if err != nil {
				results <- err
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				results <- fmt.Errorf("%s: HTTP %d", path, resp.StatusCode)
				return
			}
			if path == "/servers" {
				var list []serverView
				err = json.NewDecoder(resp.Body).Decode(&list)
				if err == nil {
					counts := make(map[string]int)
					for _, s := range list {
						counts[s.JobID] = s.Players
					}
					if len(list) != 2 || counts["public"] != 3 || counts["private"] != 7 {
						err = fmt.Errorf("server counts = %v", counts)
					}
				}
			} else {
				var counts map[string]int
				err = json.NewDecoder(resp.Body).Decode(&counts)
				if err == nil && (len(counts) != 2 || counts["10"] != 10 || counts["999"] != 0) {
					err = fmt.Errorf("place counts = %v", counts)
				}
			}
			results <- err
		}(path)
	}
	for i := 0; i < cap(results); i++ {
		if err := <-results; err != nil {
			t.Error(err)
		}
	}
	t.Logf("100 concurrent count requests completed in %s with RCC blocked", time.Since(start).Round(time.Millisecond))
	if got := calls.Load(); got != 1 {
		t.Fatalf("RCC calls during request burst = %d, want 1", got)
	}

	// Once RCC recovers, the next reads see the refreshed counts.
	unblock()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for m.Players([]int64{10})[10] != 22 {
		select {
		case <-deadline.C:
			t.Fatal("counts did not refresh after RCC recovered")
		case <-tick.C:
		}
	}
}
