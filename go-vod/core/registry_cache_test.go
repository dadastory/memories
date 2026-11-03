package core

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pulsejet/memories/go-vod/config"
	"github.com/stretchr/testify/require"
)

func gatedRegistryProbe(t *testing.T) (*Registry, string, string) {
	t.Helper()
	root := t.TempDir()
	started, resume := filepath.Join(root, "started"), filepath.Join(root, "resume")
	probe := filepath.Join(root, "ffprobe")
	script := fmt.Sprintf("#!/bin/sh\nprintf 'started\\n' >> %q\nwhile [ ! -f %q ]; do sleep 0.01; done\nprintf '%%s\\n' '{\"streams\":[{\"codec_type\":\"audio\",\"codec_name\":\"aac\",\"channels\":2,\"sample_rate\":\"48000\"}],\"format\":{\"duration\":\"60\"}}'\n", started, resume)
	require.NoError(t, os.WriteFile(probe, []byte(script), 0700))
	cfg := config.Defaults("test")
	cfg.TempDir, cfg.FFprobe, cfg.ManagerIdleTime = filepath.Join(root, "cache"), probe, 0
	r := NewRegistry(cfg, nil)
	t.Cleanup(r.Close)
	t.Cleanup(func() { _ = os.WriteFile(resume, nil, 0600) })
	return r, started, resume
}

func TestPinnedInitializationReservesCapacityAndSurvivesOneCanceledWaiter(t *testing.T) {
	r, started, resume := gatedRegistryProbe(t)
	p := ManagerParams{URL: "http://localhost/input.m4a", StreamID: "synthetic", CacheReservationBytes: 1024}
	first, err := r.Pin(p)
	require.NoError(t, err)
	second, err := r.Pin(p)
	require.NoError(t, err)
	defer first.Release()
	defer second.Release()
	snapshot := r.CacheSnapshot()
	require.Len(t, snapshot, 1)
	require.Equal(t, 2, snapshot[0].Active)
	require.Equal(t, int64(1024), snapshot[0].ReservedBytes)
	require.Empty(t, snapshot[0].Directory, "pin must not create or probe a manager")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	firstResult := make(chan error, 1)
	go func() { _, _, err := first.Wait(ctx); firstResult <- err }()
	type acquired struct {
		manager *Manager
		err     error
	}
	secondResult := make(chan acquired, 1)
	go func() { m, _, err := second.Wait(t.Context()); secondResult <- acquired{m, err} }()
	require.Eventually(t, func() bool { _, err := os.Stat(started); return err == nil }, time.Second, 10*time.Millisecond)
	cancel()
	select {
	case err := <-firstResult:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("canceled waiter did not return")
	}
	select {
	case result := <-secondResult:
		t.Fatalf("shared probe ended with a live waiter: %v", result.err)
	default:
	}
	require.NoError(t, os.WriteFile(resume, nil, 0600))
	select {
	case result := <-secondResult:
		require.NoError(t, result.err)
		require.NotNil(t, result.manager)
	case <-time.After(time.Second):
		t.Fatal("remaining waiter did not receive the initialized manager")
	}
	payload, err := os.ReadFile(started)
	require.NoError(t, err)
	require.Equal(t, 1, strings.Count(string(payload), "started\n"), "concurrent waiters must share one native probe")
}

func TestLastCanceledWaiterStopsNativeInitialization(t *testing.T) {
	r, started, _ := gatedRegistryProbe(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, _, err := r.AcquireContext(ctx, ManagerParams{URL: "http://localhost/input.m4a", StreamID: "synthetic"})
		result <- err
	}()
	require.Eventually(t, func() bool { _, err := os.Stat(started); return err == nil }, time.Second, 10*time.Millisecond)
	cancel()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("canceled waiter did not return")
	}
	joined := make(chan struct{})
	go func() { r.creating.Wait(); close(joined) }()
	select {
	case <-joined:
	case <-time.After(time.Second):
		t.Fatal("last cancellation left native probe running")
	}
	require.Empty(t, r.CacheSnapshot())
	r.mu.Lock()
	defer r.mu.Unlock()
	require.Empty(t, r.members)
}

func TestRegistryEvictionProtectsReadersAndNewerAccess(t *testing.T) {
	cfg := config.Defaults("test")
	cfg.TempDir = t.TempDir()
	cfg.FFprobe = stubProbe(t)
	cfg.ManagerIdleTime = 0
	r := NewRegistry(cfg, nil)
	defer r.Close()
	p := ManagerParams{URL: "http://localhost/input.mp4", StreamID: "synthetic"}
	m, release, err := r.Acquire(p)
	require.NoError(t, err)
	snapshot := r.CacheSnapshot()[0]
	require.False(t, r.Evict(snapshot), "active HTTP reader must pin its native generation")
	release()
	release()
	require.False(t, r.Evict(snapshot), "stale cleanup snapshot must not remove newly accessed data")
	snapshot = r.CacheSnapshot()[0]
	require.True(t, r.Evict(snapshot))
	require.NoDirExists(t, m.tempDir)
	next, done, err := r.Acquire(p)
	require.NoError(t, err)
	done()
	require.NotEqual(t, m.generation, next.generation)
	require.False(t, r.Evict(snapshot))
	require.DirExists(t, next.tempDir)
}
