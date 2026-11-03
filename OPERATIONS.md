# FlashYun media adapter

Upstream: https://github.com/pulsejet/memories, Memories `v10.2.0`, commit
`d59c33d94d4e9627565e2023eb3c5c798788ebe2`; go-vod `0.6.3`, Apache-2.0.
FlashYun uses `go-vod/core`, `config`, `ffmpeg` and playlist formatting only.
The Nextcloud HTTP server, database/catalog, frontend and numeric file identities
are not part of the FlashYun runtime. Preserve upstream licensing and notices.

Maintained branch: `flashyun-media-adapter`. The parent repository pins the
reviewed local fork commit. Commit and remote publication are separate steps;
the bootstrap URL remains upstream until an authorized fork publication. Do not
claim that the local commits are available from the upstream remote.

Maintained differences:

- Recognize pure audio and produce AAC HLS with an audio-only rendition.
- Preserve MPEG-TS timestamps across seek restarts and normalize HLS audio rather
  than copying pre-seek packets. These use native FFmpeg muxer/encoder options.
- One goroutine waits for each encoder; synchronous, idempotent destruction joins
  encoders before removing their generation-specific directory.
- Encoder idle stop retains completed cache. `ManagerIdleTime=0` delegates expiry
  to the embedding service; FlashYun Temporal owns retention/capacity cleanup.
- Context cancellation releases segment request waiters; one disconnected client
  does not kill a shared encoder needed by another client.
- Failed process startup returns immediately instead of waiting for a chunk that
  can never arrive.
- Optional bounded adaptive previews reuse native stream descriptors and master
  playlists. The source's display dimensions (including rotation) and policy
  cap determine the even-sized ladder; each selected rendition is encoded lazily.
  Adaptive streams pass those exact display dimensions to the native scaler,
  swapping axes before explicit transpose only; no second square-box rounding
  may turn an even 480p rendition into an unencodable 479-pixel height.
  Per-rendition bitrate stays within policy, and pending-window reservations
  conservatively cover all available renditions without pre-encoding them.
- Optional height/bitrate policy, decoder/encoder thread limits and
  finite input windows reuse native FFmpeg options. A finite input-side -t is
  required with seeked copyts; output-side -t may otherwise produce no content.
  Shared native semaphores bound probes and encoder processes; completed windows
  release their process budget without waiting for the idle supervisor.
- Optional ordinary-container input format and HTTP(S) protocol allowlists prevent
  nested playlists from issuing arbitrary source requests. Upstream defaults
  remain unchanged when the embedding service leaves this restriction empty.
- Registry Pin/Wait separates atomic capacity reservation from context-aware
  native initialization. Unrelated cache hits do not wait for a cold FFprobe;
  shared initialization stops only after the final waiting pin is released.
- Registry Acquire/release pins active requests. CacheSnapshot/Evict reject
  active readers and stale cleanup snapshots; no persistent cache scheduler or
  database is added inside the fork.

Use full immutable resource/profile identities, not truncated Nextcloud file IDs.
Keep `UsesTemp=false`; original HTTP inputs must support authorized Range reads.
Do not call `api.NewServer`, which clears its whole temporary root at startup.
Do not unlink cached segments behind the core registry. Expire a whole inactive
manager generation, join it, remove its registry entry, then permit recreation.
An embedding service must protect active HTTP readers during expiry.

Checks (run from `go-vod` with the repository-pinned Go environment):

```sh
GOWORK=off GOMAXPROCS=2 go test -race -p 2 ./ffmpeg ./core ./api ./config
FLASHYUN_MEDIA_FIXTURES=/path/to/synthetic-fixtures \
  GOWORK=off GOMAXPROCS=2 go test -race -p 2 ./core \
  -run '^TestMediaHTTPPlayback$' -count=1
```

The explicit native fixture test needs `video.mkv` (60 seconds, 160×90, 10fps,
MPEG4 video/AAC audio) and `audio.m4a` (60 seconds AAC). It checks duration and
timestamp deltas, unencoded seek, bounded segment count, idle cache reuse,
8 concurrent readers, destruction and recreation. It is not a browser or load
test. Update upstream stable first, reapply only needed differences, rerun native
checks and review the complete diff before updating the parent gitlink.
