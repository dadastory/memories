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
	"path/filepath"
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

	// coder is the current transcode. It stays set after exiting, which
	// keeps the Manager alive until the stream idles out; exited marks that.
	coder  *exec.Cmd
	exited bool
	// closed is set by Stop; a stopped stream never starts another coder.
	closed bool

	// run numbers transcode runs; each writes its own init file.
	run int
	// initPath is the first init a run finished; runs write identical inits.
	initPath string
	// initChunk signals the current run's init, like a segment chunk.
	initChunk *Chunk
	coderDone chan struct{}
	coderStop chan struct{}

	tonemapOnce sync.Once
	vaapiOpenCL bool
	nvencCUDA   bool

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
			if s.inactive >= idleTicks(s.c.StreamIdleTime) && s.coder != nil {
				s.stopCoder()
			}
			s.mutex.Unlock()

		case <-s.stop:
			return
		}
	}
}

func (s *Stream) clear() {
	log.Printf("%s-%s: stopping stream", s.m.id, s.quality)

	s.stopCoder()
	for _, chunk := range s.chunks {
		chunk.signal(false)
	}
	// A killed muxer may have written a segment before its playlist was consumed.
	entries, _ := os.ReadDir(s.m.tempDir)
	for _, entry := range entries {
		if quality, _, err := ffmpeg.ParseSegmentName(entry.Name()); err == nil && quality == s.quality {
			os.Remove(filepath.Join(s.m.tempDir, entry.Name()))
		}
	}

	s.chunks = make(map[int]*Chunk)
	s.goal = 0

}

// stopCoder releases CPU and input connections without invalidating ready cache.
// The caller holds mutex. Exactly one monitor goroutine owns Cmd.Wait.
func (s *Stream) stopCoder() {
	if s.coder != nil {
		if s.coderStop != nil {
			close(s.coderStop)
		}
		if s.initChunk != nil {
			s.initChunk.signal(false)
		}
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
		s.coderStop = nil
	}
	// Interrupted muxers can write an init before announcing their first segment.
	if path := ffmpeg.InitPath(s.m.tempDir, s.quality, s.run); path != s.initPath {
		os.Remove(path)
	}
}

// Stop kills the transcode right away, so it can't outlive the Manager.
func (s *Stream) Stop() {
	s.mutex.Lock()
	s.closed = true
	s.clear()
	s.mutex.Unlock()

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
	if s.closed {
		s.mutex.Unlock()
		w.WriteHeader(http.StatusConflict)
		return nil
	}
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
		for i := id - 1; s.coder != nil && !s.exited && i >= id-s.c.LookBehind && i >= 0; i-- {
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

// ServeInit serves the init segment, starting a run when none can write it.
// Runs write identical inits, so a waiter follows restarts instead of failing.
func (s *Stream) ServeInit(w http.ResponseWriter) {
	s.ServeInitContext(context.Background(), w)
}

func (s *Stream) ServeInitContext(ctx context.Context, w http.ResponseWriter) {
	deadline := time.After(chunkWait)
	for {
		s.mutex.Lock()
		s.inactive = 0
		if s.initPath == "" && (s.coder == nil || s.exited) {
			if s.closed {
				s.mutex.Unlock()
				w.WriteHeader(http.StatusConflict)
				return
			}
			// Register chunk 0 so its request follows this run.
			s.clear()
			s.createChunk(0)
			s.goal = s.c.GoalBufferMax
			if err := s.transcode(0); err != nil {
				s.mutex.Unlock()
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
		}
		path, init := s.initPath, s.initChunk
		s.mutex.Unlock()

		if path != "" {
			s.serveFile(w, path)
			return
		}

		select {
		case <-ctx.Done():
			return
		case <-init.doneCh:
		case <-deadline:
			w.WriteHeader(http.StatusRequestTimeout)
			return
		}

		// Our run died without writing an init and nothing replaced it.
		s.mutex.Lock()
		failed := s.initPath == "" && s.initChunk == init
		s.mutex.Unlock()
		if failed {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}
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

	coder := ffmpeg.Command(context.Background(), s.c.FFmpeg, args...)
	log.Printf("%s-%s: %s", s.m.id, s.quality, strings.Join(ffmpeg.RedactArgs(coder.Args), " "))

	cmdStdOut, cmdStdErr, err := startCoder(coder)
	if err != nil {
		log.Printf("%s-%s: ffmpeg failed to start: %v", s.m.id, s.quality, err)
		w.WriteHeader(http.StatusInternalServerError)
		return err
	}
	stderrDone := make(chan struct{})
	go func() {
		s.monitorStderr(cmdStdErr)
		close(stderrDone)
	}()

	// Write to response
	defer cmdStdOut.Close()
	stdoutReader := bufio.NewReader(cmdStdOut)

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
		w.(http.Flusher).Flush()
	}

	// Terminate ffmpeg; Wait closes the pipes, so drain stderr first
	coder.Process.Kill()
	<-stderrDone
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
	filename := s.getSegmentPath(id)
	os.Remove(filename)
}

func (s *Stream) returnChunk(w http.ResponseWriter, id int) {
	s.serveFile(w, s.getSegmentPath(id))
}

func (s *Stream) serveFile(w http.ResponseWriter, filename string) {
	s.mutex.Lock()
	f, err := os.Open(filename)
	s.mutex.Unlock()
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer f.Close()
	w.Header().Set("Content-Type", "video/mp4")
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
	restarted := coder != s.coder && s.coder != nil
	s.mutex.Unlock()

	if finished {
		s.returnChunk(w, chunk.id)
		return
	}

	// Check if coder was replaced, not merely exited or cleared
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
		CodecName:         s.m.probe.CodecName,

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
		startNumber = max(startID-1, 0)
		startAt = 0
		if startNumber > 0 {
			startAt = segs[startNumber].Start + copySeekEpsilon
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
	if s.c.EncoderSlots != nil && !s.c.EncoderSlots.TryAcquire(1) {
		return errors.New("encoder budget busy")
	}
	started := false
	defer func() {
		if !started && s.c.EncoderSlots != nil {
			s.c.EncoderSlots.Release(1)
		}
	}()
	s.run++
	initPath := ffmpeg.InitPath(s.m.tempDir, s.quality, s.run)
	s.initChunk = NewChunk(-1)

	args := ffmpeg.SegmentArgs(spec, startNumber, s.getSegmentPath(-1), initPath)
	coder := ffmpeg.Command(context.Background(), s.c.FFmpeg, args...)

	// Log command, quoting the args as needed
	log.Printf("%s-%s: %s", s.m.id, s.quality, ffmpeg.QuoteForLog(ffmpeg.RedactArgs(coder.Args)))

	cmdStdOut, cmdStdErr, err := startCoder(coder)
	if err != nil {
		log.Printf("%s-%s: ffmpeg failed to start: %v", s.m.id, s.quality, err)
		s.failChunks()
		return err
	}
	s.coder = coder
	s.exited = false

	started = true
	done := make(chan struct{})
	stopping := make(chan struct{})
	s.coderDone = done
	s.coderStop = stopping
	go s.supervise(coder, initPath, cmdStdOut, cmdStdErr, done, stopping)
	return nil
}

// startCoder starts the command with stdout and stderr piped.
func startCoder(cmd *exec.Cmd) (stdout, stderr io.ReadCloser, err error) {
	if stdout, err = cmd.StdoutPipe(); err != nil {
		return nil, nil, err
	}
	if stderr, err = cmd.StderrPipe(); err != nil {
		return nil, nil, errors.Join(err, stdout.Close())
	}
	if err := cmd.Start(); err != nil {
		return nil, nil, errors.Join(err, stdout.Close(), stderr.Close())
	}
	return stdout, stderr, nil
}

// failChunks wakes and drops unfinished chunks, so waiters fail fast and
// retries restart the transcode instead of waiting on a dead one.
// Caller must hold s.mutex.
func (s *Stream) failChunks() {
	if s.initChunk != nil {
		s.initChunk.signal(false)
	}
	for id, chunk := range s.chunks {
		if !chunk.done {
			chunk.signal(false)
			delete(s.chunks, id)
		}
	}
}

func (s *Stream) checkGoal(id int) {
	goal := id + s.c.GoalBufferMin
	if goal > s.goal {
		s.goal = id + s.c.GoalBufferMax

		// resume encoding
		if s.coder != nil && !s.exited {
			log.Printf("%s-%s: resuming transcoding", s.m.id, s.quality)
			s.coder.Process.Signal(syscall.SIGCONT)
		}
	}
}

func (s *Stream) getSegmentPath(id int) string {
	if id == -1 {
		return ffmpeg.SegmentPattern(s.m.tempDir, s.quality)
	}
	return ffmpeg.SegmentPath(s.m.tempDir, s.quality, id)
}

// Separate goroutine
func (s *Stream) monitorTranscodeOutput(coder *exec.Cmd, initPath string, cmdStdOut io.ReadCloser) {
	defer cmdStdOut.Close()
	stdoutReader := bufio.NewReader(cmdStdOut)
	seen := make(map[int]bool)
	initSeen := false

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
				// The muxer writes the init before listing any segment. Runs
				// write identical inits, so keep the first: replacing it could
				// delete a file ServeInit is about to open.
				if !initSeen {
					initSeen = true
					if s.initPath == "" {
						s.initPath = initPath
					} else {
						os.Remove(initPath)
					}
					s.initChunk.signal(true)
				}
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

// supervise reads the coder's output to EOF before reaping it, since Wait
// closes the pipes and would drop the final segment lines.
func (s *Stream) supervise(coder *exec.Cmd, initPath string, stdout, stderr io.ReadCloser, done, stopping chan struct{}) {
	stderrDone := make(chan struct{})
	go func() {
		s.monitorStderr(stderr)
		close(stderrDone)
	}()
	stdoutDone := make(chan struct{})
	go func() {
		s.monitorTranscodeOutput(coder, initPath, stdout)
		close(stdoutDone)
	}()
	select {
	case <-stdoutDone:
	case <-stopping:
		// A killed run cannot produce more cache; do not wait on its reader's mutex.
		_ = stdout.Close()
	}
	<-stderrDone
	s.monitorExit(coder, done)
}

func (s *Stream) monitorExit(coder *exec.Cmd, done chan struct{}) {
	err := coder.Wait()
	if s.c.EncoderSlots != nil {
		s.c.EncoderSlots.Release(1)
	}
	close(done)

	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Our own kills clear or replace s.coder first; nothing to do then.
	if s.coder != coder {
		return
	}
	if err != nil {
		log.Printf("%s-%s: ffmpeg exited: %v", s.m.id, s.quality, err)
	}

	// Chunks still pending will never be produced, even on a clean exit
	// (e.g. past EOF). Wake them, and let the next request restart.
	s.exited = true
	s.failChunks()
}
