package core

import (
	"bufio"
	"context"
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/pulsejet/memories/go-vod/config"
	"github.com/pulsejet/memories/go-vod/ffmpeg"
)

// CODEC_H264 matches ffprobe's codec_name for H.264 streams.
const CODEC_H264 = "h264"

// copySeekEpsilon nudges copy seeks past timestamp rounding, so -ss lands
// on the grid keyframe instead of the one before it.
const copySeekEpsilon = 0.001

// chunkWait caps per-request chunk waits; the transcode continues.
var chunkWait = 10 * time.Second

type Stream struct {
	c       *config.Config
	m       *Manager
	quality string
	order   int
	height  int
	width   int
	bitrate int

	goal int

	// mutex guards supervision state only: chunks map, goal, coder,
	// inactive. Serving a file and waiting for completion never hold it.
	mutex  sync.Mutex
	chunks map[int]*Chunk

	coder     *exec.Cmd
	coderDone chan struct{}

	tonemapOnce sync.Once
	vaapiOpenCL bool

	inactive int
	stop     chan bool
	stopped  chan struct{}
}

func (s *Stream) init() {
	s.chunks = make(map[int]*Chunk)
	s.stop = make(chan bool, 1)
	s.stopped = make(chan struct{})
}

func (s *Stream) Run() {
	defer close(s.stopped)
	// run every 5s
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			s.mutex.Lock()
			// Prune chunks
			for id := range s.chunks {
				if id < s.goal-s.c.GoalBufferMax {
					s.pruneChunk(id)
				}
			}

			s.inactive++

			// Nothing done for 2 minutes
			if s.inactive >= s.c.StreamIdleTime/5 && s.coder != nil {
				s.stopCoder()
			}
			s.mutex.Unlock()

		case <-s.stop:
			t.Stop()
			s.mutex.Lock()
			s.clear()
			s.mutex.Unlock()
			return
		}
	}
}

func (s *Stream) clear() {
	log.Printf("%s-%s: stopping stream", s.m.id, s.quality)

	s.stopCoder()
	for _, chunk := range s.chunks {
		chunk.signal(false)
		os.Remove(s.getTsPath(chunk.id))
	}

	s.chunks = make(map[int]*Chunk)
	s.goal = 0

}

// stopCoder releases CPU and input connections without invalidating ready cache.
// The caller holds mutex. Exactly one monitor goroutine owns Cmd.Wait.
func (s *Stream) stopCoder() {
	if s.coder != nil {
		if s.coder.Process != nil {
			s.coder.Process.Kill()
		}
		if s.coderDone != nil {
			<-s.coderDone
		} else {
			// A killed encoder is expected to return an exit error.
			_ = s.coder.Wait()
		}
		s.coder = nil
		s.coderDone = nil
	}
}

func (s *Stream) Stop() {
	select {
	case s.stop <- true:
	default:
	}
	if s.stopped != nil {
		<-s.stopped
	}
}

func (s *Stream) ServeChunk(w http.ResponseWriter, id int) error {
	return s.ServeChunkContext(context.Background(), w, id)
}

func (s *Stream) ServeChunkContext(ctx context.Context, w http.ResponseWriter, id int) error {
	s.mutex.Lock()
	select {
	case <-s.stopped:
		s.mutex.Unlock()
		w.WriteHeader(http.StatusGone)
		return nil
	default:
	}
	s.inactive = 0
	if s.m.tc.BoundedWindow && s.coderDone != nil {
		select {
		case <-s.coderDone:
			s.stopCoder()
		default:
		}
	}
	s.checkGoal(id)

	chunk, ok := s.chunks[id]
	if !ok || (!chunk.done && s.coder == nil) {
		// Restart unless a nearby chunk shows the transcode is close
		restart := true
		for i := id - 1; i >= id-s.c.LookBehind && i >= 0; i-- {
			if _, ok := s.chunks[i]; ok {
				restart = false
				break
			}
		}
		if restart || s.coder == nil {
			s.clear()
			chunk = s.createChunk(id)
			s.goal = id + s.c.GoalBufferMax
			if err := s.transcode(id); err != nil {
				chunk.signal(false)
				s.mutex.Unlock()
				w.WriteHeader(http.StatusServiceUnavailable)
				return err
			}
		} else {
			chunk = s.createChunk(id)
		}
	}
	s.mutex.Unlock()

	s.waitForChunkContext(ctx, w, chunk)
	return nil
}

func (s *Stream) ServeFullVideo(w http.ResponseWriter, r *http.Request) error {
	if (s.quality == QUALITY_MAX || s.quality == QUALITY_DIRECT) &&
		IsCodecPlayable(s.m.probe.CodecName, s.m.playableCodecs) {
		// Already on disk, serve temp file directly.
		if s.m.usesTemp {
			log.Printf("%s-%s: serving temp file directly", s.m.id, s.quality)
			w.Header().Set("Content-Type", "video/mp4")
			http.ServeFile(w, r, s.m.ffmpegInput())
			return nil
		}

		// Full quality in a playable codec - signal PHP to download directly.
		log.Printf("%s-%s: playable, signalling direct stream", s.m.id, s.quality)
		w.Header().Set(StreamOriginalHeader, "1")
		w.WriteHeader(http.StatusNoContent)
		return nil
	}
	args := ffmpeg.MP4Args(s.transcodeSpec(0, false))

	coder := exec.Command(s.c.FFmpeg, args...)
	log.Printf("%s-%s: %s", s.m.id, s.quality, strings.Join(ffmpeg.RedactArgs(coder.Args), " "))

	cmdStdOut, err := coder.StdoutPipe()
	if err != nil {
		log.Printf("FATAL: ffmpeg command stdout failed with %s\n", err)
	}

	cmdStdErr, err := coder.StderrPipe()
	if err != nil {
		log.Printf("FATAL: ffmpeg command stdout failed with %s\n", err)
	}

	err = coder.Start()
	if err != nil {
		log.Printf("FATAL: ffmpeg command failed with %s\n", err)
	}
	go s.monitorStderr(cmdStdErr)

	// Write to response
	defer cmdStdOut.Close()
	stdoutReader := bufio.NewReader(cmdStdOut)

	// Write mov headers
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Server does not support Flusher!",
			http.StatusInternalServerError)
		return nil
	}
	w.Header().Set("Content-Type", "video/mp4")
	w.WriteHeader(http.StatusOK)

	// Write data, flusing every 1MB
	buf := make([]byte, 1024*1024)
	for {
		n, err := stdoutReader.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Printf("FATAL: ffmpeg command failed with %s\n", err)
			break
		}

		_, err = w.Write(buf[:n])
		if err != nil {
			log.Printf("%s-%s: client closed connection", s.m.id, s.quality)
			log.Println(err)
			break
		}
		flusher.Flush()
	}

	// Terminate ffmpeg process
	if coder.Process != nil {
		coder.Process.Kill()
	}
	coder.Wait()

	return nil
}

func (s *Stream) createChunk(id int) *Chunk {
	if c, ok := s.chunks[id]; ok {
		return c
	} else {
		s.chunks[id] = NewChunk(id)
		return s.chunks[id]
	}
}

func (s *Stream) pruneChunk(id int) {
	delete(s.chunks, id)

	// Remove file
	filename := s.getTsPath(id)
	os.Remove(filename)
}

func (s *Stream) returnChunk(w http.ResponseWriter, id int) {
	filename := s.getTsPath(id)
	s.mutex.Lock()
	f, err := os.Open(filename)
	s.mutex.Unlock()
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer f.Close()
	w.Header().Set("Content-Type", "video/MP2T")
	io.Copy(w, f)
}

func (s *Stream) waitForChunk(w http.ResponseWriter, chunk *Chunk) {
	s.waitForChunkContext(context.Background(), w, chunk)
}

func (s *Stream) waitForChunkContext(ctx context.Context, w http.ResponseWriter, chunk *Chunk) {
	s.mutex.Lock()
	done := chunk.done
	doneCh := chunk.doneCh
	coder := s.coder
	s.mutex.Unlock()

	if done {
		s.returnChunk(w, chunk.id)
		return
	}

	select {
	case <-ctx.Done():
		return
	case <-doneCh:
	case <-time.After(chunkWait):
	}

	// Re-fetch: a restart may have replaced the chunk object meanwhile.
	// A completed replacement is served instead of failing the request.
	s.mutex.Lock()
	current, ok := s.chunks[chunk.id]
	finished := ok && current.done
	restarted := coder != s.coder
	s.mutex.Unlock()

	if finished {
		s.returnChunk(w, chunk.id)
		return
	}

	// Check if coder was changed
	if restarted {
		w.WriteHeader(http.StatusConflict)
		return
	}

	// Return timeout error
	w.WriteHeader(http.StatusRequestTimeout)
}

func (s *Stream) spec(startAt float64, isHls bool) ffmpeg.Spec {
	// Stream copy needs the keyframe grid for HLS segmentation. Without it
	// direct falls back to re-encoding (like max); progressive MP4 copies
	// fine without keyframes, so it always copies.
	_, grid := s.m.CopySegments()
	videoBitrate := s.m.tc.VideoBitrateKbps
	if s.m.tc.AdaptivePreview && !s.m.probe.AudioOnly {
		videoBitrate = s.bitrate/1000 - s.m.tc.AudioBitrateKbps
	}
	return ffmpeg.Spec{
		AudioOnly:        s.m.probe.AudioOnly,
		AudioBitrateKbps: s.m.tc.AudioBitrateKbps,
		VideoBitrateKbps: videoBitrate,
		Threads:          s.m.tc.Threads,
		Bin:              s.c.FFmpeg,
		Input:            s.m.ffmpegInput(),
		Headers:          s.m.inputHeaders(),
		InputFormats:     s.m.c.InputFormats,
		StartAt:          startAt,
		HLS:              isHls,

		DisplayDimensions: s.m.tc.AdaptivePreview,
		Quality:           s.quality,
		Width:             s.width,
		Height:            s.height,
		QF:                s.m.tc.QF,
		FrameRate:         s.m.probe.FrameRate,
		Rotation:          s.m.probe.Rotation,
		HDR:               s.m.probe.HDR,
		BitDepth:          s.m.probe.BitDepth,
		Audio:             s.m.probe.Audio,
		ChunkSize:         s.m.tc.ChunkSize,
		Copy:              s.quality == QUALITY_DIRECT && (!isHls || grid),

		VAAPI:           s.m.tc.VAAPI,
		VAAPILowPower:   s.m.tc.VAAPILowPower,
		VAAPIDevice:     s.m.tc.VAAPIDevice,
		NVENC:           s.m.tc.NVENC,
		NVENCTemporalAQ: s.m.tc.NVENCTemporalAQ,
		NVENCScale:      s.m.tc.NVENCScale,

		UseTranspose:     s.m.tc.UseTranspose,
		ForceSwTranspose: s.m.tc.ForceSwTranspose,
		UseGopSize:       s.m.tc.UseGopSize,
	}
}

func (s *Stream) transcode(startID int) error {
	var startNumber int
	var startAt float64

	if segs, ok := s.m.CopySegments(); ok && s.quality == QUALITY_DIRECT {
		if startID >= len(segs) {
			startID = len(segs) - 1
		}
		startNumber = startID
		startAt = 0
		if startID > 0 {
			startAt = segs[startID].Start + copySeekEpsilon
		}
	} else if startID > 0 {
		// Start one frame before.
		// This ensures that the keyframes are aligned.
		startNumber = startID - 1
		startAt = float64(startNumber * s.m.tc.ChunkSize)
	}

	spec := s.transcodeSpec(startAt, true)
	if s.m.tc.BoundedWindow {
		spec.Duration = float64((s.goal - startNumber + 1) * s.m.tc.ChunkSize)
	}
	args := ffmpeg.SegmentArgs(spec, startNumber, s.getTsPath(-1))
	if s.c.EncoderSlots != nil && !s.c.EncoderSlots.TryAcquire(1) {
		return errors.New("encoder budget busy")
	}
	started := false
	defer func() {
		if !started && s.c.EncoderSlots != nil {
			s.c.EncoderSlots.Release(1)
		}
	}()

	// Start the process
	s.coder = exec.Command(s.c.FFmpeg, args...)

	// Log command, quoting the args as needed
	log.Printf("%s-%s: %s", s.m.id, s.quality, ffmpeg.QuoteForLog(ffmpeg.RedactArgs(s.coder.Args)))

	cmdStdOut, err := s.coder.StdoutPipe()
	if err != nil {
		s.coder = nil
		return err
	}

	cmdStdErr, err := s.coder.StderrPipe()
	if err != nil {
		s.coder = nil
		return errors.Join(err, cmdStdOut.Close())
	}

	err = s.coder.Start()
	if err != nil {
		s.coder = nil
		return errors.Join(err, cmdStdOut.Close(), cmdStdErr.Close())
	}
	started = true

	coder := s.coder
	done := make(chan struct{})
	s.coderDone = done
	go s.monitorTranscodeOutput(cmdStdOut, coder)
	go s.monitorStderr(cmdStdErr)
	go s.monitorExit(coder, done)
	return nil
}

func (s *Stream) checkGoal(id int) {
	goal := id + s.c.GoalBufferMin
	if goal > s.goal {
		s.goal = id + s.c.GoalBufferMax

		// resume encoding
		if s.coder != nil {
			log.Printf("%s-%s: resuming transcoding", s.m.id, s.quality)
			s.coder.Process.Signal(syscall.SIGCONT)
		}
	}
}

func (s *Stream) getTsPath(id int) string {
	if id == -1 {
		return ffmpeg.SegmentPattern(s.m.tempDir, s.quality)
	}
	return ffmpeg.SegmentPath(s.m.tempDir, s.quality, id)
}

// Separate goroutine
func (s *Stream) monitorTranscodeOutput(cmdStdOut io.ReadCloser, coder *exec.Cmd) {

	defer cmdStdOut.Close()
	stdoutReader := bufio.NewReader(cmdStdOut)
	seen := make(map[int]bool)

	for {
		s.mutex.Lock()
		current := s.coder
		s.mutex.Unlock()
		if current != coder {
			break
		}

		line, err := stdoutReader.ReadBytes('\n')
		if err == io.EOF {
			if len(line) == 0 {
				break
			}
		} else if err != nil {
			log.Println(err)
			break
		} else {
			line = line[:(len(line) - 1)]
		}

		l := string(line)

		// Skip repeats: the muxer reprints the whole playlist per segment.
		if segQuality, id, ok := ffmpeg.ParseSegmentLine(l); ok && segQuality == s.quality {
			if seen[id] {
				continue
			}
			seen[id] = true

			// Debug
			log.Printf("%s-%s: recv %s", s.m.id, s.quality, l)

			s.mutex.Lock()
			// The coder has changed; do nothing
			if s.coder == coder {
				s.createChunk(id).signal(true)

				// Check goal satisfied
				if id >= s.goal && !s.m.tc.BoundedWindow {
					log.Printf("%s-%s: goal satisfied: %d", s.m.id, s.quality, s.goal)
					s.coder.Process.Signal(syscall.SIGSTOP)
				}
			}
			s.mutex.Unlock()
		}
	}
}

func (s *Stream) monitorStderr(cmdStdErr io.ReadCloser) {
	stderrReader := bufio.NewReader(cmdStdErr)

	for {
		line, err := stderrReader.ReadBytes('\n')
		if err == io.EOF {
			if len(line) == 0 {
				break
			}
		} else if err != nil {
			log.Println(err)
			break
		} else {
			line = line[:(len(line) - 1)]
		}
		log.Println("ffmpeg-error:", string(line))
	}
}

func (s *Stream) monitorExit(coder *exec.Cmd, done chan struct{}) {
	// Exactly one goroutine joins each encoder. clear waits for this signal
	// before reusing supervision state; close before acquiring s.mutex.
	err := coder.Wait()
	if s.c.EncoderSlots != nil {
		s.c.EncoderSlots.Release(1)
	}
	close(done)

	// Try to get exit status
	if exitError, ok := err.(*exec.ExitError); ok {
		exitcode := exitError.ExitCode()
		log.Printf("%s-%s: ffmpeg exited with status: %d", s.m.id, s.quality, exitcode)

		s.mutex.Lock()
		defer s.mutex.Unlock()

		// If error code is >0, there was an error in transcoding
		if exitcode > 0 && s.coder == coder {
			// Wake all outstanding chunks; they re-evaluate
			// (409 on coder change, else 408) instead of waiting out
			for _, chunk := range s.chunks {
				chunk.signal(false)
			}
		}
	}
}
