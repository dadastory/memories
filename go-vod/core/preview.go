package core

import (
	"errors"
	"strconv"

	"github.com/pulsejet/memories/go-vod/config"
)

// addAdaptivePreviewStreams only describes native streams. Run starts their
// supervisors, while FFmpeg is still created on the first requested fragment.
func (m *Manager) addAdaptivePreviewStreams() error {
	width, height := m.probe.Width, m.probe.Height
	if m.probe.Rotation%180 != 0 {
		width, height = height, width
	}
	if width < 2 || height < 2 {
		return errors.New("invalid preview dimensions")
	}
	ceiling := min(height, m.tc.PreviewMaxHeight()) / 2 * 2
	for _, target := range config.PreviewHeights(ceiling) {
		scaledWidth := int(float64(width)*float64(target)/float64(height)) / 2 * 2
		if scaledWidth < 2 {
			continue
		}
		videoBitrate := min(m.tc.VideoBitrateKbps, max(128, int(float64(m.tc.VideoBitrateKbps)*float64(target*target)/float64(ceiling*ceiling))))
		quality := strconv.Itoa(target) + "p"
		if target == height && scaledWidth == width && m.previewCopyWithinBudget(videoBitrate) {
			quality = QUALITY_DIRECT
		}
		m.streams[quality] = &Stream{c: m.c, m: m, quality: quality,
			width: scaledWidth, height: target, bitrate: (videoBitrate + m.tc.AudioBitrateKbps) * 1000}
	}
	if len(m.streams) == 0 {
		return errors.New("invalid preview dimensions")
	}
	return nil
}

// Reuse only an existing verified uniform keyframe grid. Never scan the whole
// source at preview admission to manufacture a remux optimization. The fixed
// grid also preserves finite-window time/space admission and fragment IDs.
func (m *Manager) previewCopyWithinBudget(videoBitrate int) bool {
	if !m.copyEligible || m.probe.CodecName != "h264" || m.probe.HDR || m.probe.BitDepth > 8 || m.probe.Rotation != 0 || m.probe.BitRate < 1 || m.probe.BitRate > videoBitrate*1000 {
		return false
	}
	segments, ok := m.TryCacheCopySegments()
	if !ok {
		return false
	}
	for index, segment := range segments {
		start := float64(index * m.tc.ChunkSize)
		duration := min(float64(m.tc.ChunkSize), m.probe.Duration.Seconds()-start)
		if duration <= 0 || segment.Start != start || segment.Duration != duration {
			return false
		}
	}
	return len(segments) > 0
}
