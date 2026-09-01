# v0.7.0

- Add a Compatibility table to the README listing TV models confirmed working,
  starting with the LG C1 (OLED65C1PUB) on webOS 6.4.0.
- Diagnostics reports now open with a single DEVICE line carrying the model,
  webOS release, TV Node version, and tested status, so a submitted report can
  be triaged at a glance and the model added to the table.
- webOS release and model detection at startup, reported as tested or untested.
  Advisory only; an untested release still runs.
- Startup probes for TV Node, compositor volume QML, boot hook, iptables, input
  devices, the Sonos connection, and the audio Luna service.
- Persistent, size-capped diagnostics log under /var/lib, plus a downloadable
  report at /api/diagnostics and a Diagnostics screen in the Setup app.
- Addresses masked in the report; no serial number, device id, or MAC is read.
