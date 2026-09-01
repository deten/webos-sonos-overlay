# webos-sonos-overlay

> **This is in testing. Use at your own risk.**
>
> It requires a rooted TV, patches a system file belonging to the compositor, and
> changes the volume of real speakers. It has been tested on one TV so far (see
> [Compatibility](#compatibility)). Expect rough edges, and do not install it on a
> TV you are not willing to troubleshoot.

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
  boot-time hook, no manual config editing.
- **Survives cold boot.** The TV fully cold-boots on every power-on; the service
  retries with backoff through Wi-Fi association and DHCP.

## Installing

The TV must already be rooted with Homebrew Channel installed. Either method
below gives you the same package: the service, the overlay, and the setup app.

### Option A: add the repository (no PC needed)

Recommended. Homebrew Channel then handles updates for you.

1. On the TV, open **Homebrew Channel** and go to **Settings**.
2. Choose **Add repository** and enter:

   ```
   https://raw.githubusercontent.com/deten/webos-sonos-overlay/main/repo/
   ```

3. Return to the app list. **Sonos Volume Overlay** appears there. Install it.
4. Launch it once. It finds your Sonos, tests the connection, and offers to
   install the boot hook so it starts automatically from then on.

When a new version is published, Homebrew Channel shows it as an update in the
same list.

### Option B: install the ipk directly

Use this if you would rather not add a repository. Homebrew Channel has no
"install from file" option, so this needs a computer. Updates are manual: repeat
these steps for each new version.

Download the latest `.ipk` from the
[Releases page](https://github.com/deten/webos-sonos-overlay/releases), then use
whichever of these you already have.

**webOS Dev Manager** (graphical, easiest)

Add your TV in the app, then use *Install app* and pick the downloaded `.ipk`.

**ares-install** (from the official webOS TV SDK)

```
ares-setup-device            # once, to register the TV
ares-install --device <name> com.brineandbuild.sonosoverlay_<version>_all.ipk
```

**ssh** (if you already have shell access to the TV)

```
scp com.brineandbuild.sonosoverlay_<version>_all.ipk root@<tv-ip>:/tmp/app.ipk
ssh root@<tv-ip> "luna-send -n 50 luna://com.webos.appInstallService/dev/install '{\"id\":\"com.brineandbuild.sonosoverlay\",\"ipkUrl\":\"/tmp/app.ipk\",\"subscribe\":true}'"
```

If you have this repository checked out, `npm run deploy-ipk` does the same thing
using the settings in `deploy.config.json`.

Whichever method you use, launch the app once afterwards to run setup.

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

## Publishing a release

1. Bump `version` in `package.json`.
2. `npm run release` builds and snapshots into `releases/v<version>/`.
3. `npm run build-repo` regenerates `repo/apps.json` and the manifest, filling in
   the new version, the ipk sha256 and size, and the release asset URL. It also
   writes `dist/<id>_<version>_all.ipk` under the name the manifest expects.
4. Create a GitHub release tagged `v<version>` and attach that ipk.
5. Commit and push `repo/`. Homebrew Channel picks up the new version on its next
   refresh.

The repository URL that users add points at `repo/` on the default branch, so the
listing updates as soon as step 5 lands.

## Releases

`npm run release` snapshots the current build into `releases/v<version>/`:
the service bundle, the `.ipk`, a manifest with a SHA-256 per file, and a notes
file. Bump `version` in `package.json` first; the script will not overwrite an
existing folder without `--force`.

The folders are snapshots, not build inputs. Nothing reads from them, so old
ones can be deleted whenever they stop being useful. See `releases/README.md`.

## Compatibility

Nothing is version-locked. The service runs on any release and reports what it
finds rather than refusing to start.

### Tested devices

| TV model | webOS | TV Node | Status |
|---|---|---|---|
| `OLED65C1PUB` (LG C1) | 6.4.0 | v8.12.0 | Working. Overlay, volume sync, and cold-boot survival all confirmed. |

If your TV is not listed, it is not known to fail, only unverified. Send a
diagnostics report (see below) and it will be added to this table. The report's
`DEVICE:` line carries everything needed: model, webOS release, and TV Node
version.

Several things it depends on are internal to webOS and can move between releases:

| Dependency | Why it can break |
|---|---|
| Compositor volume QML | The overlay works by removing an `external_arc` guard from `StarfishVolume.qml`. A different path or a rewritten file means no indicator. |
| `com.webos.service.audio` | Reading and writing the TV's volume. If the service name or reply shape changes, sync stops. |
| TV-side Node version | The bundle targets Node 8. An older runtime will not parse it. |
| `/dev/input` event devices | Device numbering already shifts between boots on a single TV; a different input layout changes which device carries volume keys. |
| Homebrew Channel init hook | `/var/lib/webosbrew/init.d` is what makes the service survive a power cycle. |
| `iptables` | Used to open the service's ports. |

On startup the service identifies the platform and probes each of these, then logs
one line saying whether this release is tested. On an untested release the Setup app
shows an advisory banner and setup continues normally.

## Reporting a problem

The service keeps a diagnostics log at
`/var/lib/com.brineandbuild.sonosoverlay/diagnostics.log`. Unlike `/var/log`, this
survives a reboot, and it is size-capped so it cannot grow without bound.

To send it:

1. On the TV, open the Sonos Overlay app and choose **Diagnostics** on the last screen.
2. It shows an address like `http://<tv-ip>:7476/api/diagnostics`. Open that in a
   browser on your phone or computer, on the same network; it downloads a text file.
3. Attach that file to a new issue on this repository. Say what you expected to
   happen and what happened instead, or just say it works if you are reporting a
   TV that is not in the table yet.

The report contains the webOS version, TV model, the result of each dependency probe,
current volume state, and the service's event log. **Network addresses are masked**
(`192.168.x.x`), and it deliberately reads no serial number, device id, or MAC address.

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
- Never measure the TV's native volume step while anything is writing to the TV;
  the writes read back as the step.
