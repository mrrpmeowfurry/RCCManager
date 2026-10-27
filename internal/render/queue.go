// one worker per render rcc, one render at a time each, rcc is single threaded for this
package render

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"rccmanager/internal/config"
	"rccmanager/internal/rcc"
	"rccmanager/internal/storage"
)

type Kind string

const (
	Avatar Kind = "avatar"
	Place  Kind = "place"
	Item   Kind = "item"
	Outfit Kind = "outfit"
)

func ParseKind(s string) (Kind, bool) {
	switch k := Kind(s); k {
	case Avatar, Place, Item, Outfit:
		return k, true
	}
	return "", false
}

type Status string

const (
	Queued  Status = "queued"
	Running Status = "running"
	Done    Status = "done"
	Failed  Status = "failed"
)

type Job struct {
	Kind       Kind           `json:"kind"`
	ID         int64          `json:"id"`
	Status     Status         `json:"status"`
	Error      string         `json:"error,omitempty"`
	Files      []string       `json:"files,omitempty"`
	Outputs    []renderedFile `json:"outputs,omitempty"`
	Warnings   []string       `json:"warnings,omitempty"`
	QueuedAt   time.Time      `json:"queuedAt"`
	FinishedAt time.Time      `json:"finishedAt,omitempty"`

	url   string
	r15   bool
	again bool // asked again while running
}

var ErrQueueFull = errors.New("render queue is full")

type Queue struct {
	cfg     *config.Config
	clients []*rcc.Client
	store   storage.Store

	mu    sync.Mutex
	jobs  map[string]*Job
	queue chan *Job
}

func NewQueue(cfg *config.Config, clients []*rcc.Client, store storage.Store) *Queue {
	return &Queue{
		cfg:     cfg,
		clients: clients,
		store:   store,
		jobs:    map[string]*Job{},
		queue:   make(chan *Job, 4096),
	}
}

func key(kind Kind, id int64) string {
	return string(kind) + "/" + strconv.FormatInt(id, 10)
}

// already running = once more after, the avatar may have changed again
func (q *Queue) Enqueue(kind Kind, id int64, url string, r15 bool) error {
	if url == "" {
		url = q.defaultURL(kind, id)
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	k := key(kind, id)
	j, ok := q.jobs[k]
	if ok {
		j.url = url
		j.r15 = r15
		switch j.Status {
		case Queued:
			return nil
		case Running:
			j.again = true
			return nil
		}
	} else {
		j = &Job{
			Kind: kind,
			ID:   id,
			url:  url,
			r15:  r15,
		}
		q.jobs[k] = j
	}
	return q.pushLocked(j)
}

func (q *Queue) pushLocked(j *Job) error {
	j.Status = Queued
	j.Error = ""
	j.QueuedAt = time.Now()
	j.FinishedAt = time.Time{}
	select {
	case q.queue <- j:
		return nil
	default:
		j.Status = Failed
		j.Error = ErrQueueFull.Error()
		return ErrQueueFull
	}
}

func (q *Queue) defaultURL(kind Kind, id int64) string {
	switch kind {
	case Avatar:
		return q.cfg.SiteInternalURL + "/Asset/CharacterFetch.ashx?userId=" + strconv.FormatInt(id, 10)
	case Place:
		return q.cfg.SiteInternalURL + "/asset/?id=" + strconv.FormatInt(id, 10)
	case Outfit:
		return q.cfg.SiteInternalURL + "/Asset/CharacterFetch.ashx?outfitId=" + strconv.FormatInt(id, 10)
	}
	// items need the asset url from the site, item id != asset id
	return ""
}

func (q *Queue) Job(kind Kind, id int64) (Job, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	j, ok := q.jobs[key(kind, id)]
	if !ok {
		return Job{}, false
	}
	out := *j
	out.Files = append([]string(nil), j.Files...)
	out.Outputs = append([]renderedFile(nil), j.Outputs...)
	out.Warnings = append([]string(nil), j.Warnings...)
	return out, true
}

func (q *Queue) Len() int {
	return len(q.queue)
}

func (q *Queue) Workers() int {
	return len(q.clients)
}

func (q *Queue) Store() storage.Store {
	return q.store
}

func (q *Queue) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for _, c := range q.clients {
		wg.Add(1)
		go func(c *rcc.Client) {
			defer wg.Done()
			q.worker(ctx, c)
		}(c)
	}
	go q.sweep(ctx)
	wg.Wait()
}

func (q *Queue) worker(ctx context.Context, c *rcc.Client) {
	for {
		var j *Job
		select {
		case <-ctx.Done():
			return
		case j = <-q.queue:
		}

		q.mu.Lock()
		j.Status = Running
		j.again = false
		url := j.url
		r15 := j.r15
		q.mu.Unlock()

		start := time.Now()
		files, outputs, warnings, err := q.render(ctx, c, j.Kind, j.ID, url, r15)

		q.mu.Lock()
		j.FinishedAt = time.Now()
		if err != nil {
			j.Status = Failed
			j.Error = err.Error()
			log.Printf("%s %d failed: %v", j.Kind, j.ID, err)
		} else {
			j.Status = Done
			j.Error = ""
			j.Files = files
			j.Outputs = outputs
			j.Warnings = warnings
			log.Printf("rendered %s %d in %s", j.Kind, j.ID, time.Since(start).Round(time.Millisecond))
		}
		if j.again {
			_ = q.pushLocked(j)
		}
		q.mu.Unlock()
	}
}

// so the map doesnt grow forever
func (q *Queue) sweep(ctx context.Context) {
	t := time.NewTicker(10 * time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			cutoff := time.Now().Add(-time.Hour)
			q.mu.Lock()
			for k, j := range q.jobs {
				if (j.Status == Done || j.Status == Failed) && j.FinishedAt.Before(cutoff) {
					delete(q.jobs, k)
				}
			}
			q.mu.Unlock()
		}
	}
}

// render everything before storing anything, no fresh body next to a stale head
func (q *Queue) render(ctx context.Context, c *rcc.Client, kind Kind, id int64, url string, r15 bool) ([]string, []renderedFile, []string, error) {
	idStr := strconv.FormatInt(id, 10)
	site := q.cfg.SiteInternalURL

	type out struct {
		key  string
		data []byte
		mime string
	}
	var outs []out
	var warnings []string

	switch kind {
	case Avatar:
		full, err := q.png(ctx, c, "avatar-"+idStr, avatarScript(site, id, url, avatarSize, avatarSize, false, r15), 60*time.Second)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("full: %w", err)
		}
		head, err := q.png(ctx, c, "headshot-"+idStr, avatarScript(site, id, url, headshotSize, headshotSize, true, r15), 60*time.Second)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("headshot: %w", err)
		}
		outs = []out{
			{"avatars/" + idStr + ".png", full, "image/png"},
			{"avatars/" + idStr + "-headshot.png", head, "image/png"},
		}
		// old builds have no OBJ export, the pngs still go out
		if scene, err := q.json(ctx, c, "avatar3d-"+idStr, avatar3DScript(site, id, url, r15), 60*time.Second); err != nil {
			log.Printf("no 3d for avatar %d: %v", id, err)
			warnings = append(warnings, "no 3d: "+err.Error())
		} else {
			outs = append(outs, out{"avatars/" + idStr + "-3d.json", scene, "application/json"})
		}

	case Place:
		// the game icon is the creator's own upload, a crop of this would just repeat the thumbnail
		thumb, err := q.png(ctx, c, "thumb-"+idStr, placeScript(site, url, thumbW, thumbH, true), 120*time.Second)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("thumbnail: %w", err)
		}
		outs = []out{
			{"thumbnails/" + idStr + ".png", thumb, "image/png"},
		}

	case Item:
		if url == "" {
			return nil, nil, nil, errors.New("item render needs appearanceUrl (CharacterFetch.ashx?assetId=...)")
		}
		png, err := q.png(ctx, c, "item-"+idStr, itemScript(site, url, itemSize, itemSize), 60*time.Second)
		if err != nil {
			return nil, nil, nil, err
		}
		outs = []out{
			{"catalog/" + idStr + ".png", png, "image/png"},
		}
		if scene, err := q.json(ctx, c, "item3d-"+idStr, item3DScript(site, url), 60*time.Second); err != nil {
			log.Printf("no 3d for item %d: %v", id, err)
			warnings = append(warnings, "no 3d: "+err.Error())
		} else {
			outs = append(outs, out{"catalog/" + idStr + "-3d.json", scene, "application/json"})
		}

	// the outfit id is not a user id, the appearance url carries everything
	case Outfit:
		png, err := q.png(ctx, c, "outfit-"+idStr, avatarScript(site, 0, url, avatarSize, avatarSize, false, r15), 60*time.Second)
		if err != nil {
			return nil, nil, nil, err
		}
		outs = []out{
			{"outfits/" + idStr + ".png", png, "image/png"},
		}

	default:
		return nil, nil, nil, fmt.Errorf("unknown render kind %q", kind)
	}

	urls := make([]string, 0, len(outs))
	files := make([]renderedFile, 0, len(outs))
	for _, o := range outs {
		u, err := q.store.Put(ctx, o.key, o.data, o.mime)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("store %s: %w", o.key, err)
		}
		urls = append(urls, u)
		files = append(files, renderedFile{
			Key:  o.key,
			Size: len(o.data),
		})
	}
	// off the worker, a dead site must not hold up the next render
	go q.notify(context.Background(), kind, id, files)
	return urls, files, warnings, nil
}

func (q *Queue) png(ctx context.Context, c *rcc.Client, name, script string, timeout time.Duration) ([]byte, error) {
	jctx, cancel := context.WithTimeout(ctx, timeout+10*time.Second)
	defer cancel()
	vals, err := c.BatchJob(jctx, name+"-"+rcc.RandomHex(6), name, script, timeout)
	if err != nil {
		return nil, err
	}
	if len(vals) == 0 || vals[0].Type != "LUA_TSTRING" {
		return nil, errors.New("rcc did not return an image")
	}
	png, err := base64.StdEncoding.DecodeString(strings.TrimSpace(vals[0].Value))
	if err != nil || !bytes.HasPrefix(png, []byte("\x89PNG")) {
		return nil, errors.New("rcc returned something that is not a png")
	}
	return png, nil
}

func (q *Queue) json(ctx context.Context, c *rcc.Client, name, script string, timeout time.Duration) ([]byte, error) {
	jctx, cancel := context.WithTimeout(ctx, timeout+10*time.Second)
	defer cancel()
	vals, err := c.BatchJob(jctx, name+"-"+rcc.RandomHex(6), name, script, timeout)
	if err != nil {
		return nil, err
	}
	if len(vals) == 0 || vals[0].Type != "LUA_TSTRING" {
		return nil, errors.New("rcc did not return a scene")
	}
	scene := []byte(strings.TrimSpace(vals[0].Value))
	if !bytes.HasPrefix(scene, []byte("{")) {
		return nil, errors.New("rcc returned something that is not json")
	}
	return scene, nil
}
