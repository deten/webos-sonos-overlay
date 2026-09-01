# v0.8.1

Prompted by a tester's diagnostics report on webOS 6.5.0.

- Identify the TV even when the nyx json files are missing. /run is a tmpfs and
  those files are written during boot, so the boot hook can start this service
  before they exist, which made a report come back with "unknown model". nyx-cmd
  reads the same data directly and does not depend on them.
- Re-read the platform and re-run the dependency probes when a diagnostics report
  is generated. They were frozen at startup, so one report claimed no volume keys
  had been seen while also showing a keypress 187 seconds earlier.
- Record OLED55C14LB on webOS 6.5.0 as tested.
