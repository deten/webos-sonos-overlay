# webos-sonos-overlay

An LG webOS background service that keeps a Sonos soundbar's volume locked to the
TV's own volume, and draws a matching on-screen volume indicator.

Problem: When a TV is set to `external_arc` sound output, webOS suppresses its own volume
OSD and hands control to HDMI-CEC. The remote still changes the volume, but there
is no number on screen and the two devices drift apart over time. **This project
restores the indicator and keeps the two scales in agreement.**

## Key features

- **On-screen volume indicator** over any content, including full-screen video.
- **The TV leads.** The TV's own counter is the source of truth; the Sonos is
  corrected to match, never the other way around.
- **2:1 mapping.** The TV steps 1 per press and the Sonos steps 2 per CEC press,
  so Sonos volume is held at exactly `TV x 2` and a normal press needs no
  correction at all.
- **Drift correction** 400 ms after the last keypress and every 10 s.
- **External changes adopted, not reverted.** A change made from the Sonos app is
  folded back into the TV's counter, so phone control keeps working.
- **Absolute volume ceiling** plus a per-correction raise cap, so no single
  correction can produce a large jump.
- **Setup app** for discovering the player, testing it, and installing the
  boot-time hook — no manual config editing.
- **Survives cold boot.** The TV fully cold-boots on every power-on; the service
  retries with backoff through Wi-Fi association and DHCP.

## Requirements

- LG TV running webOS 6.x, rooted, with the Homebrew Channel installed
- A Sonos player on the same network
- TV sound output set to HDMI-ARC / eARC with CEC volume control enabled
- Node.js and `ssh`/`scp` on the build machine

## How it works

| Piece | Role |
|---|---|
| `tv-service.js` | Background service running on the TV under its own Node runtime |
| `lib/ssdp.js`, `lib/soap.js` | Sonos discovery and `GetVolume` / `SetVolume` / `GetTransportInfo` |
| `lib/gena.js`, `lib/parse.js` | UPnP event subscription to `RenderingControl` and `LastChange` parsing |
| `lib/input.js` | Raw `/dev/input` reader for remote volume keys, with restart and give-up |
| `lib/correlator.js` | Matches a keypress to the Sonos event it caused |
| `overlay/` | Transparent floating web app that draws the indicator |
| `setup/` | Web app for discovery, testing, and installing the boot hook |
| `index.js` | Standalone latency harness, runs on the build machine |

The service listens on three ports: `7474` for UPnP events, `7475` for the
overlay WebSocket, and `7476` for the setup API.

## Build and deploy

Deployment targets are host-specific and are not committed. Copy the example
config and fill in your own values:

```
cp deploy.config.example.json deploy.config.json
```

| Key | Meaning |
|---|---|
| `tvIp` | Address of the TV |
| `tvUser` | SSH user, normally `root` |
| `sshKey` | Path to the private key for the TV |
| `appId` | Package id to install |

`TV_IP`, `TV_USER`, and `TV_SSH_KEY` override the file if set in the environment.

```
npm install
npm run build-ipk       # bundle the service and package the apps
npm run open-ports      # allow 7474-7476 through the TV's iptables (once)
npm run deploy-ipk      # package and install over ssh
npm run deploy          # push just the service bundle
npm run launch-overlay  # start the overlay app
```

## Releases

`npm run release` snapshots the current build into `releases/v<version>/` —
the service bundle, the `.ipk`, a manifest with a SHA-256 per file, and a notes
file. Bump `version` in `package.json` first; the script will not overwrite an
existing folder without `--force`.

The folders are snapshots, not build inputs. Nothing reads from them, so old
ones can be deleted whenever they stop being useful. See `releases/README.md`.

## Configuration

Written by the setup app to `/var/lib/com.brineandbuild.sonosoverlay/config.json`:

| Key | Meaning |
|---|---|
| `sonosIp`, `sonosPort` | Player address; re-resolved by discovery if unreachable |
| `sonosUuid`, `sonosName`, `sonosModel` | Used to re-find the player after a DHCP change |
| `maxVolume` | Absolute ceiling on the Sonos scale (default 70) |

## Notes

- `/var/log` on webOS is a ramfs and is wiped on every boot. Keep service logging
  quiet; a retry loop can consume real memory.
- Input device numbering shifts between boots, so a configured device that no
  longer exists is normal. The reader abandons a device that never emits.
- Never measure the TV's native volume step while anything is writing to the TV —
  the writes read back as the step.
