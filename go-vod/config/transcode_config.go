package config

import (
	"errors"
)

type TCfg struct {
	// Preview confines low-bandwidth renditions and a finite native window.
	AdaptivePreview  bool `json:"adaptivePreview,omitempty"`
	MaxHeight        int  `json:"maxHeight,omitempty"`
	VideoBitrateKbps int  `json:"videoBitrateKbps,omitempty"`
	AudioBitrateKbps int  `json:"audioBitrateKbps,omitempty"`
	Threads          int  `json:"threads,omitempty"`
	BoundedWindow    bool `json:"boundedWindow,omitempty"`
	ChunkSize        int  `json:"chunkSize" validate:"gte=1"`
	QF               int  `json:"qf"`

	VAAPI         bool   `json:"vaapi"`
	VAAPILowPower bool   `json:"vaapiLowPower"`
	VAAPIDevice   string `json:"vaapiDevice"`

	NVENC           bool   `json:"nvenc"`
	NVENCTemporalAQ bool   `json:"nvencTemporalAQ"`
	NVENCScale      string `json:"nvencScale" validate:"required_if=NVENC true,omitempty,oneof=cuda npp"`

	UseTranspose     bool `json:"useTranspose"`
	ForceSwTranspose bool `json:"forceSwTranspose"`
	UseGopSize       bool `json:"useGopSize"`
}

func (c *TCfg) Validate() error {
	if err := validate.Struct(c); err != nil {
		return err
	}
	if c.VAAPI && c.NVENC {
		return errors.New("vaapi and nvenc are mutually exclusive")
	}
	if c.AdaptivePreview && (!c.BoundedWindow || c.MaxHeight < 0 || c.MaxHeight == 1 || c.VideoBitrateKbps < 1) {
		return errors.New("adaptive preview requires bounded windows, height and bitrate")
	}
	return nil
}

// PreviewHeights includes standard lower rungs and the exact even source/policy
// ceiling. It never invents an upscaled rendition for a small source.
func PreviewHeights(ceiling int) []int {
	ceiling = ceiling / 2 * 2
	if ceiling < 2 {
		return nil
	}
	var heights []int
	for _, height := range []int{144, 240, 360, 480, 720} {
		if height < ceiling {
			heights = append(heights, height)
		}
	}
	return append(heights, ceiling)
}

// WindowBitrateKbps reserves every possible preview rung at its bitrate ceiling
// before probing, so concurrent rendition requests share the same cache budget.
func (c TCfg) WindowBitrateKbps(audioOnly bool) int {
	if audioOnly {
		return c.AudioBitrateKbps
	}
	streams := 1
	if c.AdaptivePreview {
		streams = len(PreviewHeights(c.PreviewMaxHeight()))
	}
	return streams * (c.VideoBitrateKbps + c.AudioBitrateKbps)
}

// Zero is automatic: the native adaptive ladder remains capped at 720p and
// never upscales the source. Bitrate and encoder budgets still apply.
func (c TCfg) PreviewMaxHeight() int {
	if c.MaxHeight == 0 {
		return 720
	}
	return c.MaxHeight
}
