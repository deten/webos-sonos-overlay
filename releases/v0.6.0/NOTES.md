# v0.6.0

- Identify the webOS release and TV model at startup and report whether this
  build has been tested against it. Advisory only — an untested release still
  runs, and the Setup app shows a banner rather than blocking.
- Probe each version-sensitive dependency at startup: TV Node version, the
  compositor volume QML, the boot hook, iptables, input devices, the Sonos
  connection, and the audio Luna service.
- Add a persistent, size-capped diagnostics log under /var/lib, which survives
  the reboot a bug report needs it to survive.
- Add a Diagnostics screen to the Setup app and a downloadable plain-text report
  at /api/diagnostics, with instructions for sending it in.
- Mask network addresses in the report; read no serial number, device id, or MAC.
- lib/input.js: expose which devices are open and which have produced events.
