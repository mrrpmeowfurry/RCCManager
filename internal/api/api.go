package api

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"rccmanager/internal/config"
	"rccmanager/internal/games"
	"rccmanager/internal/rcc"
	"rccmanager/internal/render"
)

var privateKey = regexp.MustCompile(`^(vip|rsv)-[0-9]{1,12}$`)

type Server struct {
	cfg     *config.Config
	games   *games.Manager
	renders *render.Queue
	render  []*rcc.Client
	version string
}

func New(cfg *config.Config, g *games.Manager, q *render.Queue, renderClients []*rcc.Client, version string) *Server {
	return &Server{
		cfg:     cfg,
		games:   g,
		renders: q,
		render:  renderClients,
		version: version,
	}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /servers/{placeId}", s.findOrCreate)
	mux.HandleFunc("GET /servers/{jobId}", s.getServer)
	mux.HandleFunc("GET /servers", s.listServers)
	mux.HandleFunc("DELETE /servers/{jobId}", s.closeServer)
	mux.HandleFunc("GET /players", s.players)

	mux.HandleFunc("POST /render/avatar/{id}", s.enqueueRender(render.Avatar, "appearanceUrl"))
	mux.HandleFunc("POST /render/place/{id}", s.enqueueRender(render.Place, "placeUrl"))
	mux.HandleFunc("POST /render/item/{id}", s.enqueueRender(render.Item, "appearanceUrl"))
	mux.HandleFunc("POST /render/outfit/{id}", s.enqueueRender(render.Outfit, "appearanceUrl"))
	mux.HandleFunc("GET /render/{kind}/{id}", s.renderStatus)

	mux.HandleFunc("GET /health", s.health)

	return s.auth(logRequests(mux))
}

func (s *Server) auth(next http.Handler) http.Handler {
	want := []byte(s.cfg.Password)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || subtle.ConstantTimeCompare([]byte(got), want) != 1 {
			w.Header().Set("WWW-Authenticate", `Bearer realm="rccmanager"`)
			writeError(w, http.StatusUnauthorized, "bad or missing bearer token")
			return
		}
		next.ServeHTTP(w, r)
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sw := &statusWriter{
			ResponseWriter: w,
			status:         200,
		}
		start := time.Now()
		next.ServeHTTP(sw, r)
		if r.URL.Path != "/health" || sw.status >= 400 {
			log.Printf("%s %s %d %s", r.Method, r.URL.RequestURI(), sw.status, time.Since(start).Round(time.Millisecond))
		}
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func writeFailure(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, games.ErrNoFreePort), errors.Is(err, render.ErrQueueFull):
		writeError(w, http.StatusServiceUnavailable, err.Error())
	case errors.Is(err, games.ErrNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case rcc.IsError(err):
		writeError(w, http.StatusBadGateway, err.Error())
	default:
		writeError(w, http.StatusInternalServerError, err.Error())
	}
}

func pathID(r *http.Request, name string) (int64, bool) {
	n, err := strconv.ParseInt(r.PathValue(name), 10, 64)
	return n, err == nil && n > 0
}

type serverView struct {
	JobID     string    `json:"jobId"`
	PlaceID   int64     `json:"placeId"`
	Key       string    `json:"key"`
	Port      int       `json:"port"`
	Address   string    `json:"address"`
	Ready     bool      `json:"ready"`
	Players   int       `json:"players"`
	StartedAt time.Time `json:"startedAt"`
}

func view(s games.Server) serverView {
	return serverView{
		JobID:     s.JobID,
		PlaceID:   s.PlaceID,
		Key:       s.Key,
		Port:      s.Port,
		Address:   s.Address,
		Ready:     s.Ready,
		Players:   s.Players,
		StartedAt: s.StartedAt,
	}
}

func (s *Server) findOrCreate(w http.ResponseWriter, r *http.Request) {
	placeID, ok := pathID(r, "placeId")
	if !ok {
		writeError(w, http.StatusBadRequest, "bad placeId")
		return
	}
	var body struct {
		Key     string `json:"key"`
		OwnerID int64  `json:"ownerId"`
	}
	if r.ContentLength != 0 {
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<10)).Decode(&body); err != nil {
			writeError(w, http.StatusBadRequest, "bad json body")
			return
		}
	}
	if body.Key != "" && !privateKey.MatchString(body.Key) {
		writeError(w, http.StatusBadRequest, "bad key")
		return
	}
	srv, err := s.games.FindOrCreate(r.Context(), placeID, games.StartOpts{
		Key:     body.Key,
		OwnerID: body.OwnerID,
	})
	if err != nil {
		writeFailure(w, err)
		return
	}
	writeJSON(w, http.StatusOK, view(srv))
}

func (s *Server) getServer(w http.ResponseWriter, r *http.Request) {
	srv, ok := s.games.Get(r.PathValue("jobId"))
	if !ok {
		writeError(w, http.StatusNotFound, "no such server")
		return
	}
	writeJSON(w, http.StatusOK, view(srv))
}

func (s *Server) listServers(w http.ResponseWriter, r *http.Request) {
	list := s.games.List()
	out := make([]serverView, 0, len(list))
	for _, srv := range list {
		out = append(out, view(srv))
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) closeServer(w http.ResponseWriter, r *http.Request) {
	if err := s.games.Close(r.Context(), r.PathValue("jobId")); err != nil {
		writeFailure(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) players(w http.ResponseWriter, r *http.Request) {
	var ids []int64
	for _, part := range strings.Split(r.URL.Query().Get("placeIds"), ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		n, err := strconv.ParseInt(part, 10, 64)
		if err != nil {
			writeError(w, http.StatusBadRequest, "bad placeIds")
			return
		}
		ids = append(ids, n)
	}
	counts := s.games.Players(ids)
	out := make(map[string]int, len(counts))
	for id, n := range counts {
		out[strconv.FormatInt(id, 10)] = n
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) enqueueRender(kind render.Kind, field string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, ok := pathID(r, "id")
		if !ok {
			writeError(w, http.StatusBadRequest, "bad id")
			return
		}
		var body map[string]string
		if r.ContentLength != 0 {
			if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(&body); err != nil {
				writeError(w, http.StatusBadRequest, "bad json body")
				return
			}
		}
		if s.renders.Workers() == 0 {
			writeError(w, http.StatusServiceUnavailable, "no render instance configured")
			return
		}
		if err := s.renders.Enqueue(kind, id, body[field], body["r15"] == "1"); err != nil {
			writeFailure(w, err)
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]bool{"queued": true})
	}
}

func (s *Server) renderStatus(w http.ResponseWriter, r *http.Request) {
	kind, ok := render.ParseKind(r.PathValue("kind"))
	if !ok {
		writeError(w, http.StatusNotFound, "unknown render kind")
		return
	}
	id, ok := pathID(r, "id")
	if !ok {
		writeError(w, http.StatusBadRequest, "bad id")
		return
	}
	j, ok := s.renders.Job(kind, id)
	if !ok {
		writeError(w, http.StatusNotFound, "no render requested for that id")
		return
	}
	out := map[string]any{"status": j.Status}
	if j.Error != "" {
		out["error"] = j.Error
	}
	if len(j.Files) > 0 {
		out["files"] = j.Files
	}
	if len(j.Outputs) > 0 {
		out["outputs"] = j.Outputs
	}
	if len(j.Warnings) > 0 {
		out["warnings"] = j.Warnings
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	gameUp, gameErr := s.games.RCCStatus()

	renders := make([]map[string]any, 0, len(s.render))
	for _, c := range s.render {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		err := c.Ping(ctx)
		cancel()
		entry := map[string]any{
			"name": c.Name,
			"url":  c.URL,
			"up":   err == nil,
		}
		if err != nil {
			entry["error"] = err.Error()
		}
		renders = append(renders, entry)
	}

	rccState := "up"
	if !gameUp {
		rccState = "down"
	}
	out := map[string]any{
		"rcc":     rccState,
		"jobs":    s.games.Count(),
		"queue":   s.renders.Len(),
		"storage": s.renders.Store().String(),
		"version": s.version,
		"game": map[string]any{
			"url":   s.games.RCCURL(),
			"up":    gameUp,
			"error": gameErr,
		},
		"render": renders,
	}
	status := http.StatusOK
	if !gameUp {
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, out)
}
