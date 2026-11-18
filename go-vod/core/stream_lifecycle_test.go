package core

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/pulsejet/memories/go-vod/config"
	"github.com/pulsejet/memories/go-vod/ffmpeg"
	"github.com/stretchr/testify/require"
)

func TestStopCoderKeepsReadyCacheAndWaitsExactlyOnce(t *testing.T) {
	s, _ := testStream(t)
	s.chunks[0] = NewChunk(0)
	s.chunks[0].signal(true)
	require.NoError(t, os.WriteFile(s.getSegmentPath(0), []byte("ready"), 0600))
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
	require.FileExists(t, s.getSegmentPath(0))
	require.True(t, s.chunks[0].done)
	select {
	case <-done:
	default:
		t.Fatal("encoder has not been joined")
	}
	// A killed muxer can leave a segment whose playlist entry was not consumed.
	require.NoError(t, os.WriteFile(s.getSegmentPath(1), []byte("unlisted"), 0600))
	s.mutex.Lock()
	s.clear()
	s.clear()
	s.mutex.Unlock()
	require.NoFileExists(t, s.getSegmentPath(0))
	require.NoFileExists(t, s.getSegmentPath(1))
}

func TestInterruptedInitRunsKeepOnlyRetainedInit(t *testing.T) {
	for _, test := range []struct {
		name     string
		retained bool
		literal  bool
	}{
		{name: "unready"},
		{name: "retained", retained: true},
		{name: "literal-unready", literal: true},
		{name: "literal-retained", retained: true, literal: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			m := newCopyManager(t, hevcProbeJSON, "", false, nil)
			if test.literal {
				m.tempDir = filepath.Join(m.tempDir, "literal[cache]")
				require.NoError(t, os.MkdirAll(m.tempDir, 0700))
			}
			bin, _ := stubFFmpeg(t, `while [ $# -gt 0 ]; do [ "$1" = -hls_fmp4_init_filename ] && p="$2"; shift; done
printf init > "$p"
exec sleep 30`)
			m.c.FFmpeg = bin
			s := m.streams[QUALITY_MAX]
			unannounced := s.getSegmentPath(99)
			require.NoError(t, os.WriteFile(unannounced, []byte("partial"), 0600))
			otherRendition := ffmpeg.SegmentPath(m.tempDir, "480p", 7)
			require.NoError(t, os.WriteFile(otherRendition, []byte("other-rendition"), 0600))
			var reader *os.File
			if test.retained {
				s.initPath = ffmpeg.InitPath(m.tempDir, QUALITY_MAX, 0)
				require.NoError(t, os.WriteFile(s.initPath, []byte("ready-init"), 0600))
				var err error
				reader, err = os.Open(s.initPath)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, reader.Close()) })
			}
			for index, id := range []int{0, 10, 2} {
				ctx, cancel := context.WithCancel(t.Context())
				t.Cleanup(cancel)
				done := make(chan struct{})
				go func() {
					m.ServeChunkContext(ctx, httptest.NewRecorder(), QUALITY_MAX, id)
					close(done)
				}()
				path := ffmpeg.InitPath(m.tempDir, QUALITY_MAX, index+1)
				require.Eventually(t, func() bool { _, err := os.Stat(path); return err == nil }, time.Second, time.Millisecond)
				cancel()
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Fatal("interrupted request remained blocked")
				}
			}
			s.mutex.Lock()
			s.clear()
			s.mutex.Unlock()
			entries, err := os.ReadDir(m.tempDir)
			require.NoError(t, err)
			var files []string
			for _, entry := range entries {
				files = append(files, filepath.Join(m.tempDir, entry.Name()))
			}
			wanted := []string{otherRendition}
			if test.retained {
				wanted = append(wanted, s.initPath)
			}
			t.Logf("cache files after three interrupted runs: %d; wanted: %d; retained: %t; literal: %t", len(files), len(wanted), test.retained, test.literal)
			require.ElementsMatch(t, wanted, files, "cleanup must remove partial media and unused init files while preserving other renditions")
			otherData, err := os.ReadFile(otherRendition)
			require.NoError(t, err)
			require.Equal(t, "other-rendition", string(otherData))
			if !test.retained {
				return
			}
			data, err := io.ReadAll(reader)
			require.NoError(t, err)
			require.Equal(t, "ready-init", string(data))
			response := httptest.NewRecorder()
			require.True(t, m.ServeInit(response, QUALITY_MAX))
			require.Equal(t, http.StatusOK, response.Code)
			require.Equal(t, "ready-init", response.Body.String())
		})
	}
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

func TestWaitForInitCancellationDoesNotWaitForTimeout(t *testing.T) {
	s, _ := testStream(t)
	s.initChunk = NewChunk(-1)
	s.coder = &exec.Cmd{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		s.ServeInitContext(ctx, httptest.NewRecorder())
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("disconnected initialization request remained blocked")
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
