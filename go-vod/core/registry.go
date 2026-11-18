package core

import (
	"context"
	"errors"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pulsejet/memories/go-vod/config"
	"golang.org/x/sync/singleflight"
)

type Registry struct {
	cfg       *config.Config
	idle      chan IdleEvent
	mu        sync.Mutex
	members   map[string]*Manager
	gen       atomic.Uint64
	sf        singleflight.Group
	uses      map[string]*cacheUse
	creating  sync.WaitGroup
	closed    bool
	closeOnce sync.Once
}

type cacheUse struct {
	active        int
	accessed      time.Time
	reservedBytes int64
	initializing  context.Context
	cancel        context.CancelFunc
}

type CachedManager struct {
	ReservedBytes int64
	ID            string
	Directory     string
	Generation    uint64
	Active        int
	Accessed      time.Time
}

type IdleEvent struct {
	ID         string
	Generation uint64
}

// ManagerParams identifies a manager for a file and client. Token is
// per-request auth material, refreshed on hits, never identity.
type ManagerParams struct {
	URL             string
	StreamID        string
	FileID          int64
	Etag            string
	ServiceToken    string
	PlayableCodecs  []string
	TConfig         config.TCfg
	UsesTemp        bool
	CloseBridgeFile func()
	// CacheReservationBytes pins an embedding service's conservative byte budget
	// before native probing knows the exact stream layout.
	CacheReservationBytes int64
}

// PinnedManager separates synchronous cache admission from native initialization.
// Wait consumes the pin; callers abandoning it must call Release.
type PinnedManager struct {
	registry *Registry
	params   ManagerParams
	use      *cacheUse
	release  func()
}

func NewRegistry(cfg *config.Config, idle chan IdleEvent) *Registry {
	return &Registry{
		cfg:     cfg,
		idle:    idle,
		members: make(map[string]*Manager),
		uses:    make(map[string]*cacheUse),
	}
}

// Acquire pins a manager through a complete HTTP read, including initialization.
// Expiry is external; this method starts no cache timer or scheduler.
func (r *Registry) Acquire(p ManagerParams) (*Manager, func(), error) {
	return r.AcquireContext(context.Background(), p)
}

func (r *Registry) AcquireContext(ctx context.Context, p ManagerParams) (*Manager, func(), error) {
	pinned, err := r.Pin(p)
	if err != nil {
		return nil, nil, err
	}
	return pinned.Wait(ctx)
}

// Pin publishes active ownership and its reservation without starting a probe.
// The caller may therefore release its admission lock before Wait does any I/O.
func (r *Registry) Pin(p ManagerParams) (*PinnedManager, error) {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil, errors.New("registry closed")
	}
	use := r.uses[p.StreamID]
	if use == nil {
		initializing, cancel := context.WithCancel(context.Background())
		use = &cacheUse{initializing: initializing, cancel: cancel}
		r.uses[p.StreamID] = use
	}
	use.active++
	use.reservedBytes = max(use.reservedBytes, p.CacheReservationBytes)
	use.accessed = time.Now()
	r.mu.Unlock()
	var once sync.Once
	release := func() {
		once.Do(func() {
			r.mu.Lock()
			defer r.mu.Unlock()
			use.active--
			use.accessed = time.Now()
			if m := r.members[p.StreamID]; m != nil {
				_ = os.Chtimes(m.tempDir, use.accessed, use.accessed)
			}
			if use.active == 0 && r.members[p.StreamID] == nil {
				use.cancel()
				delete(r.uses, p.StreamID)
			}
		})
	}
	return &PinnedManager{registry: r, params: p, use: use, release: release}, nil
}

func (p *PinnedManager) Release() { p.release() }

func (p *PinnedManager) Wait(ctx context.Context) (*Manager, func(), error) {
	if err := ctx.Err(); err != nil {
		p.Release()
		return nil, nil, err
	}
	m, err := p.registry.getOrCreateContext(ctx, p.use.initializing, p.params)
	if err != nil {
		p.Release()
		return nil, nil, err
	}
	return m, p.release, nil
}

func (r *Registry) CacheSnapshot() []CachedManager {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([]CachedManager, 0, len(r.uses))
	for id, use := range r.uses {
		item := CachedManager{ID: id, Active: use.active, Accessed: use.accessed, ReservedBytes: use.reservedBytes}
		if m := r.members[id]; m != nil {
			item.Generation = m.generation
			item.Directory = m.tempDir
			if m.tc.BoundedWindow {
				bitrate := m.tc.WindowBitrateKbps(m.probe.AudioOnly)
				item.ReservedBytes = int64(bitrate) * 1000 * int64((r.cfg.GoalBufferMax+3)*m.tc.ChunkSize) / 8 * 2
			}
		}
		result = append(result, item)
	}
	return result
}

// Evict rejects active readers and stale cleanup snapshots. Destroy joins the
// encoder before its own files are removed. A later request can recreate it.
func (r *Registry) Evict(expected CachedManager) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	m, use := r.members[expected.ID], r.uses[expected.ID]
	if m == nil || use == nil || use.active != 0 || m.generation != expected.Generation || !use.accessed.Equal(expected.Accessed) {
		return false
	}
	m.Destroy()
	use.cancel()
	delete(r.members, expected.ID)
	delete(r.uses, expected.ID)
	return true
}

func (r *Registry) Close() {
	r.closeOnce.Do(func() {
		r.mu.Lock()
		r.closed = true
		for _, use := range r.uses {
			use.cancel()
		}
		r.mu.Unlock()
		r.creating.Wait()
		r.mu.Lock()
		defer r.mu.Unlock()
		for id, m := range r.members {
			m.Destroy()
			delete(r.members, id)
		}
	})
}

func (r *Registry) GetOrCreate(p ManagerParams) (*Manager, error) {
	return r.getOrCreateContext(context.Background(), context.Background(), p)
}

func (r *Registry) getOrCreateContext(waitCtx, initializationCtx context.Context, p ManagerParams) (*Manager, error) {
	if m := r.get(p); m != nil {
		m.refreshServiceToken(p.ServiceToken)
		return m, nil
	}

	result := r.sf.DoChan(p.StreamID+"\x00"+p.URL, func() (any, error) {
		if m := r.get(p); m != nil {
			return m, nil
		}
		return r.create(initializationCtx, p)
	})
	select {
	case <-waitCtx.Done():
		return nil, waitCtx.Err()
	case result := <-result:
		if result.Err != nil {
			return nil, result.Err
		}
		m := result.Val.(*Manager)
		m.refreshServiceToken(p.ServiceToken)
		return m, nil
	}
}

func (r *Registry) get(p ManagerParams) *Manager {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}

	m := r.members[p.StreamID]
	if m == nil || m.url != p.URL {
		return nil
	}
	if m.usesTemp != p.UsesTemp {
		return nil
	}
	if p.Etag != "" && m.etag != "" && m.etag != p.Etag {
		return nil
	}
	return m
}

func (r *Registry) create(ctx context.Context, p ManagerParams) (*Manager, error) {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil, errors.New("registry closed")
	}
	r.creating.Add(1)
	r.mu.Unlock()
	defer r.creating.Done()
	manager, err := newManager(ctx, NewManagerArgs{
		ManagerParams: p,
		C:             r.cfg,
		Generation:    r.gen.Add(1),
		Idle:          r.idle,
	})
	if err != nil {
		log.Println("Error creating manager", err)
		return nil, err
	}

	r.mu.Lock()
	if r.closed || ctx.Err() != nil {
		r.mu.Unlock()
		manager.Destroy()
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, errors.New("registry closed")
	}
	old := r.members[p.StreamID]
	r.members[p.StreamID] = manager
	r.mu.Unlock()

	// Outside the lock: Destroy waits on each stream's mutex.
	if old != nil {
		old.Destroy()
	}
	return manager, nil
}

func (r *Registry) Remove(streamID string, generation uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if m := r.members[streamID]; m == nil || m.generation != generation {
		return
	}
	delete(r.members, streamID)
}
