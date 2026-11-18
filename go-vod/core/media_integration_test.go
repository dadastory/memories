package core

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pulsejet/memories/go-vod/config"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"
)

// Native dependency check, explicitly enabled with synthetic fixture files.
// No browser, database, production originals or complete restored input is used.
func TestMediaHTTPPlayback(t *testing.T) {
	fixtures := os.Getenv("FLASHYUN_MEDIA_FIXTURES")
	if fixtures == "" {
		t.Skip("set FLASHYUN_MEDIA_FIXTURES to the synthetic FFmpeg fixture directory")
	}
	origin := httptest.NewServer(http.FileServer(http.Dir(fixtures)))
	defer origin.Close()
	for _, name := range []string{"video.mkv", "audio.m4a", "bounded-video.mkv", "bounded-audio.m4a"} {
		t.Run(name, func(t *testing.T) {
			bounded := strings.HasPrefix(name, "bounded-")
			name = strings.TrimPrefix(name, "bounded-")
			cfg := config.Defaults("test")
			cfg.TempDir = t.TempDir()
			cfg.FFmpeg = "ffmpeg"
			cfg.FFprobe = "ffprobe"
			cfg.ManagerIdleTime = 0
			if bounded {
				cfg.EncoderSlots = semaphore.NewWeighted(1)
			}
			reg := NewRegistry(cfg, nil)
			params := ManagerParams{URL: origin.URL + "/" + name, StreamID: "synthetic", TConfig: config.TCfg{ChunkSize: 4, QF: 24}}
			if bounded {
				params.TConfig.MaxHeight = 360
				params.TConfig.VideoBitrateKbps = 400
				params.TConfig.AudioBitrateKbps = 64
				params.TConfig.Threads = 2
				params.TConfig.BoundedWindow = true
			}
			m, err := reg.GetOrCreate(params)
			require.NoError(t, err)
			defer m.Destroy()
			quality := QUALITY_MAX
			if bounded {
				quality = "preview"
			}
			if name == "audio.m4a" {
				quality = "audio"
			}
			var initialPTS float64
			for _, id := range []int{0, 10} {
				w := httptest.NewRecorder()
				require.True(t, m.ServeChunkContext(t.Context(), w, quality, id))
				require.Equal(t, http.StatusOK, w.Code)
				segment := filepath.Join(t.TempDir(), "segment.mp4")
				init := httptest.NewRecorder()
				require.True(t, m.ServeInitContext(t.Context(), init, quality))
				require.Equal(t, http.StatusOK, init.Code)
				require.NoError(t, os.WriteFile(segment, append(init.Body.Bytes(), w.Body.Bytes()...), 0600))
				out, err := exec.CommandContext(t.Context(), "ffprobe", "-v", "error", "-show_entries", "format=duration:stream=codec_type,start_time", "-of", "json", segment).Output()
				require.NoError(t, err)
				var probe struct {
					Format  struct{ Duration string }
					Streams []struct {
						CodecType string `json:"codec_type"`
						StartTime string `json:"start_time"`
					}
				}
				require.NoError(t, json.Unmarshal(out, &probe))
				duration, err := strconv.ParseFloat(probe.Format.Duration, 64)
				require.NoError(t, err)
				require.NotEmpty(t, probe.Streams)
				pts, err := strconv.ParseFloat(probe.Streams[0].StartTime, 64)
				require.NoError(t, err)
				require.InDelta(t, 4, duration-pts, .5)
				if id == 0 {
					initialPTS = pts
				} else {
					require.InDelta(t, 40, pts-initialPTS, .5)
				}
				if name == "audio.m4a" {
					require.Len(t, probe.Streams, 1)
					require.Equal(t, "audio", probe.Streams[0].CodecType)
				}
			}
			// Wait for upstream encoder pacing: even a 60s fast synthetic file
			// must not be fully encoded after requesting one four-second window.
			time.Sleep(100 * time.Millisecond)
			files, err := filepath.Glob(filepath.Join(m.tempDir, quality+"-[0-9]*.m4s"))
			require.NoError(t, err)
			require.LessOrEqual(t, len(files), cfg.GoalBufferMax+3)
			s := m.streams[quality]
			s.mutex.Lock()
			s.stopCoder()
			s.mutex.Unlock()
			cached, err := os.ReadFile(s.getSegmentPath(10))
			require.NoError(t, err)
			w := httptest.NewRecorder()
			m.ServeChunk(w, quality, 10)
			require.Equal(t, http.StatusOK, w.Code)
			require.Equal(t, cached, w.Body.Bytes())
			// Concurrent readers reuse the same native ready segment.
			var wg sync.WaitGroup
			for range 8 {
				wg.Go(func() {
					response := httptest.NewRecorder()
					m.ServeChunkContext(t.Context(), response, quality, 10)
					if response.Code != http.StatusOK {
						t.Errorf("concurrent response: %d", response.Code)
					}
				})
			}
			wg.Wait()
			if bounded {
				require.Eventually(t, func() bool { return cfg.EncoderSlots.TryAcquire(1) }, 2*time.Second, 10*time.Millisecond, "completed finite window must release the process budget")
				cfg.EncoderSlots.Release(1)
			}
			oldDir := m.tempDir
			m.Destroy()
			require.NoDirExists(t, oldDir)
			reg.Remove(params.StreamID, m.generation)
			next, err := reg.GetOrCreate(params)
			require.NoError(t, err)
			defer next.Destroy()
			w = httptest.NewRecorder()
			next.ServeChunk(w, quality, 0)
			require.Equal(t, http.StatusOK, w.Code, fmt.Sprint(w.Body.Len()))
			// An expired generation cannot delete the rebuilt cache.
			m.Destroy()
			require.DirExists(t, next.tempDir)
		})
	}
}
