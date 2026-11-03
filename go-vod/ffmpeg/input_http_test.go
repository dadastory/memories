package ffmpeg

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
)

func TestRestrictedHTTPProbeRejectsNestedPlaylists(t *testing.T) {
	if os.Getenv("FLASHYUN_MEDIA_FIXTURES") == "" {
		t.Skip("explicit native FFmpeg checks only")
	}
	var nested atomic.Int64
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/nested.ts" {
			nested.Add(1)
			return
		}
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		_, _ = fmt.Fprintf(w, "#EXTM3U\n#EXT-X-TARGETDURATION:4\n#EXT-X-MEDIA-SEQUENCE:0\n#EXTINF:4,\nhttp://%s/nested.ts\n#EXT-X-ENDLIST\n", r.Host)
	}))
	defer origin.Close()
	_, err := ProbeWithFormats(t.Context(), "ffprobe", origin.URL+"/source.m3u8", "", "aac,flac,matroska,mov,mp3,mpeg,mpegts,ogg,wav")
	if err == nil {
		t.Fatal("playlist source admitted as an ordinary container")
	}
	if nested.Load() != 0 {
		t.Fatal("untrusted source issued a nested HTTP request")
	}
}
