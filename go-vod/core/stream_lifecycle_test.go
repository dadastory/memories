package core

import (
	"context"
	"net/http/httptest"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/pulsejet/memories/go-vod/config"
	"github.com/stretchr/testify/require"
)

func TestStopCoderKeepsReadyCacheAndWaitsExactlyOnce(t *testing.T) {
	s, _ := testStream(t)
	s.chunks[0] = NewChunk(0)
	s.chunks[0].signal(true)
	require.NoError(t, os.WriteFile(s.getTsPath(0), []byte("ready"), 0600))
	s.coder = exec.Command("sleep", "30")
	require.NoError(t, s.coder.Start())
	coder := s.coder
	done := make(chan struct{})
	s.coderDone = done
	go s.monitorExit(coder, done)
	s.mutex.Lock()
	s.stopCoder()
	s.mutex.Unlock()
	require.Nil(t, s.coder)
	require.FileExists(t, s.getTsPath(0))
	require.True(t, s.chunks[0].done)
	select {
	case <-done:
	default:
		t.Fatal("encoder has not been joined")
	}
	s.mutex.Lock()
	s.clear()
	s.clear()
	s.mutex.Unlock()
	require.NoFileExists(t, s.getTsPath(0))
}

func TestWaitForChunkCancellationDoesNotWaitForTimeout(t *testing.T) {
	s, _ := testStream(t)
	chunk := NewChunk(2)
	s.chunks[2] = chunk
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		s.waitForChunkContext(ctx, httptest.NewRecorder(), chunk)
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("disconnected request remained blocked")
	}
}

func TestManagerReplacementDoesNotDeleteNewGeneration(t *testing.T) {
	cfg := config.Defaults("test")
	cfg.TempDir = t.TempDir()
	cfg.FFprobe = stubProbe(t)
	reg := NewRegistry(cfg, make(chan IdleEvent, 8))
	first, err := reg.GetOrCreate(ManagerParams{URL: "http://localhost/input.mp4", StreamID: "same", Etag: "old"})
	require.NoError(t, err)
	defer first.Destroy()
	second, err := reg.GetOrCreate(ManagerParams{URL: "http://localhost/input.mp4", StreamID: "same", Etag: "new"})
	require.NoError(t, err)
	defer second.Destroy()
	require.NotEqual(t, first.tempDir, second.tempDir)
	require.DirExists(t, second.tempDir)
	first.Destroy()
	require.DirExists(t, second.tempDir)
}
