// the one thing that talks to rccservice, http in soap out
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"rccmanager/internal/api"
	"rccmanager/internal/config"
	"rccmanager/internal/games"
	"rccmanager/internal/rcc"
	"rccmanager/internal/render"
	"rccmanager/internal/storage"
)

// -ldflags "-X main.version=..."
var version = "dev"

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatal(err)
	}

	gameClient := rcc.NewClient("game", cfg.GameURL, cfg.GameSecret)
	var renderClients []*rcc.Client
	for i, u := range cfg.RenderURLs {
		renderClients = append(renderClients, rcc.NewClient("render-"+strconv.Itoa(i), u, cfg.RenderSecret))
	}

	store, err := storage.New(cfg)
	if err != nil {
		log.Fatal(err)
	}

	g := games.New(cfg, gameClient)
	q := render.NewQueue(cfg, renderClients, store)
	srv := api.New(cfg, g, q, renderClients, version)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go g.RunReaper(ctx)
	go g.RunPlayerCounts(ctx)
	go q.Run(ctx)

	httpSrv := &http.Server{
		Addr:              cfg.Listen,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		// findOrCreate can sit on OpenJob for 30s
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	go func() {
		<-ctx.Done()
		log.Print("shutting down")
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(sctx)
	}()

	log.Printf("listening on %s", cfg.Listen)
	if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
	// servers keep running in rcc, the state file picks them up next start
	g.Save()
	log.Print("bye")
}
