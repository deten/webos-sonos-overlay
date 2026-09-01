# v0.5.0

- TV-leads volume sync: the TV's own counter is the source of truth and the Sonos
  is corrected to match at `TV x 2`.
- Removed optimistic rendering, step learning, and the CEC-liveness fallback.
- External Sonos changes are adopted into the TV rather than reverted.
- Absolute volume ceiling plus a per-correction raise cap.
- Input reader restarts with backoff and abandons a device that never emits.
- Setup app: player discovery, test, and boot-hook install.
- Overlay drawn in a floating window so it shows over video.
