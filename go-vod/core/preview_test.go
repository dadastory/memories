package core

import (
	"github.com/pulsejet/memories/go-vod/ffmpeg"
	"testing"
	"time"

	"github.com/pulsejet/memories/go-vod/config"
)

func TestAdaptivePreviewRespectsSourcePolicyAndSharedReservation(t *testing.T) {
	for _, item := range []struct {
		name                                    string
		width, height, rotation, ceiling, count int
	}{
		{"landscape", 1920, 1080, 0, 360, 3},
		{"portrait", 1080, 1920, 0, 360, 3},
		{"rotated", 1920, 1080, 90, 360, 3},
		{"small source", 160, 90, 0, 360, 1},
		{"policy ceiling", 1920, 1080, 0, 240, 2},
		{"nonstandard source", 640, 320, 0, 360, 3},
	} {
		t.Run(item.name, func(t *testing.T) {
			m := &Manager{c: &config.Config{}, tc: config.TCfg{AdaptivePreview: true, MaxHeight: item.ceiling, VideoBitrateKbps: 400, AudioBitrateKbps: 64, Threads: 2, BoundedWindow: true, ChunkSize: 4},
				probe: &ProbeVideoData{Width: item.width, Height: item.height, Rotation: item.rotation}, streams: make(map[string]*Stream)}
			if err := m.addAdaptivePreviewStreams(); err != nil {
				t.Fatal(err)
			}
			if len(m.streams) != item.count {
				t.Fatalf("renditions=%d want=%d", len(m.streams), item.count)
			}
			width, height := item.width, item.height
			if item.rotation%180 != 0 {
				width, height = height, width
			}
			reserved := 0
			for _, stream := range m.streams {
				if stream.width > width || stream.height > height || stream.height > item.ceiling || stream.width%2 != 0 || stream.height%2 != 0 {
					t.Fatalf("invalid rendition dimensions: %dx%d", stream.width, stream.height)
				}
				spec := stream.spec(0, true)
				if spec.VideoBitrateKbps < 1 || spec.VideoBitrateKbps > m.tc.VideoBitrateKbps || spec.VideoBitrateKbps+spec.AudioBitrateKbps != stream.bitrate/1000 {
					t.Fatal("manifest and encoder bitrate differ")
				}
				if stream.coder != nil || len(stream.chunks) != 0 {
					t.Fatal("a descriptor started encoding")
				}
				reserved += stream.bitrate / 1000
			}
			if m.tc.WindowBitrateKbps(false) < reserved {
				t.Fatal("shared reservation undercounts renditions")
			}
		})
	}
}

func TestPreviewCopyRequiresKnownBudgetAndUniformCachedGrid(t *testing.T) {
	for _, test := range []struct {
		name                   string
		bitrate                int
		cached, irregular, hdr bool
		want                   bool
	}{
		{"qualified", 200000, true, false, false, true}, {"high bitrate", 900000, true, false, false, false}, {"unknown bitrate", 0, true, false, false, false}, {"no cached index", 200000, false, false, false, false}, {"irregular GOP", 200000, true, true, false, false}, {"HDR", 200000, true, false, true, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			m := &Manager{c: &config.Config{TempDir: t.TempDir()}, tc: config.TCfg{AdaptivePreview: true, MaxHeight: 360, VideoBitrateKbps: 400, AudioBitrateKbps: 64, BoundedWindow: true, ChunkSize: 4}, probe: &ProbeVideoData{Width: 640, Height: 360, CodecName: "h264", BitRate: test.bitrate, HDR: test.hdr, Duration: 8 * time.Second}, copyEligible: true, streams: make(map[string]*Stream)}
			if test.cached {
				m.srcSegments = []ffmpeg.Segment{{Start: 0, Duration: 4}, {Start: 4, Duration: 4}}
				if test.irregular {
					m.srcSegments[0].Duration = 5
				}
			}
			if err := m.addAdaptivePreviewStreams(); err != nil {
				t.Fatal(err)
			}
			_, got := m.streams[QUALITY_DIRECT]
			if got != test.want {
				t.Fatalf("remux=%v want=%v", got, test.want)
			}
		})
	}
}
func TestAutomaticPreviewCeilingNeverUpscales(t *testing.T) {
	m := &Manager{c: &config.Config{}, tc: config.TCfg{AdaptivePreview: true, MaxHeight: 0, VideoBitrateKbps: 400, AudioBitrateKbps: 64, BoundedWindow: true, ChunkSize: 4}, probe: &ProbeVideoData{Width: 640, Height: 360}, streams: make(map[string]*Stream)}
	if err := m.addAdaptivePreviewStreams(); err != nil {
		t.Fatal(err)
	}
	for _, stream := range m.streams {
		if stream.height > 360 || stream.width > 640 {
			t.Fatal("automatic preview upscaled the source")
		}
	}
}
