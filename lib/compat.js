'use strict';
// Platform identification and dependency probes.
//
// This project reaches into several things webOS does not treat as public API:
// the compositor's volume QML, the audio Luna service, raw /dev/input, and the
// Homebrew Channel init hook. Any of them can move between webOS releases.
// Nothing here blocks startup; the point is to say plainly whether the running
// platform is one this build has been tested against, and to leave enough of a
// trail that a failure on an untested release can be reported usefully.

var fs = require('fs');
var cp = require('child_process');

// Releases this build has actually been exercised on. Add an entry only after a
// real end-to-end run: overlay draws, volume tracks, and it survives a cold boot.
// Keep this list in step with the Compatibility table in README.md.
var TESTED = [
  { release: '6.4.0', model: 'OLED65C1PUB', note: 'LG C1, development device' }
];

// Advisory notes for releases known to differ in ways that matter here.
var KNOWN_RISKS = {
  '3': 'Node on the TV predates this bundle’s target; the service may not start.',
  '4': 'The compositor volume QML has a different layout; the overlay patch may not apply.',
  '5': 'The compositor volume QML has a different layout; the overlay patch may not apply.',
  '7': 'Untested. The audio Luna service and QML paths may have moved.',
  '8': 'Untested. The audio Luna service and QML paths may have moved.',
  '9': 'Untested. The audio Luna service and QML paths may have moved.'
};

var QML_PATH  = '/usr/lib/qml/WebOSCompositor/views/volume/StarfishVolume.qml';
var HOOK_PATH = '/var/lib/webosbrew/init.d/sonos-overlay';

// ---------------------------------------------------------------------------
// Platform identification: three sources, cheapest first. Any one suffices.
// ---------------------------------------------------------------------------
function readPlatform() {
  var info = {
    release: null,
    model:   null,
    board:   null,
    name:    null,
    node:    process.version,
    source:  null
  };

  try {
    var os = JSON.parse(fs.readFileSync('/var/run/nyx/os_info.json', 'utf8'));
    info.release = os.webos_release || os.core_os_release || null;
    info.name    = os.webos_name || null;
    if (info.release) info.source = 'nyx/os_info.json';
  } catch (e) { /* fall through */ }

  if (!info.release) {
    try {
      // e.g. "Rockhopper release 6.4.0-1804 (kisscurl-kluane)"
      var rel = fs.readFileSync('/etc/starfish-release', 'utf8');
      var m   = rel.match(/(\d+\.\d+\.\d+)/);
      if (m) { info.release = m[1]; info.source = 'starfish-release'; }
    } catch (e) { /* fall through */ }
  }

  if (!info.release) {
    try {
      var cmd = '/usr/bin/luna-send -n 1 -f ' +
                'luna://com.webos.service.systemservice/osInfo ' +
                '\'{"parameters":["webos_release"]}\' 2>/dev/null';
      var j = JSON.parse(cp.execSync(cmd, { timeout: 4000 }).toString());
      if (j.webos_release) {
        info.release = j.webos_release;
        info.source  = 'systemservice/osInfo';
      }
    } catch (e) { /* give up; unknown is a valid answer */ }
  }

  try {
    var dev = JSON.parse(fs.readFileSync('/var/run/nyx/device_info.json', 'utf8'));
    // product_id is the marketing model ("OLED65C1PUB"); device_name is a board
    // codename ("o20n"). Read nothing else from this file; it also holds the
    // serial number, nduid, and MAC addresses, and this data reaches a report
    // the user uploads.
    info.model = dev.product_id || dev.model_name || null;
    info.board = dev.board_type || null;
  } catch (e) { /* optional */ }

  return info;
}

function majorOf(release) {
  if (!release) return null;
  var m = String(release).match(/^(\d+)/);
  return m ? m[1] : null;
}

// Never blocks. Returns 'tested' | 'untested' | 'unknown' with a message.
function checkPlatform(info) {
  if (!info.release) {
    return {
      status:  'unknown',
      message: 'Could not determine the webOS version. The service will still run.'
    };
  }

  var exact = TESTED.filter(function (t) { return t.release === info.release; })[0];
  if (exact) {
    var msg = 'webOS ' + info.release + ' is a tested version';
    if (exact.model && info.model && exact.model !== info.model) {
      msg += ' (tested on ' + exact.model + ', this is a ' + info.model +
             '; the firmware is what matters, so this should be fine)';
    }
    return { status: 'tested', message: msg + '.' };
  }

  var sameMajor = TESTED.filter(function (t) {
    return majorOf(t.release) === majorOf(info.release);
  })[0];

  var msg = 'webOS ' + info.release + ' has not been tested with this build';
  if (sameMajor) {
    msg += ' (webOS ' + sameMajor.release + ' has been, so it will probably work)';
  }
  msg += '.';

  var risk = KNOWN_RISKS[majorOf(info.release)];
  if (risk) msg += ' ' + risk;

  return {
    status:   'untested',
    message:  msg,
    testedOn: TESTED.map(function (t) { return t.release; })
  };
}

// ---------------------------------------------------------------------------
// Dependency probes. Each answers one question: is the thing we depend on here?
// A probe that cannot answer reports ok:false rather than throwing.
// ---------------------------------------------------------------------------
function probeNode() {
  var major = parseInt(String(process.version).replace(/^v/, '').split('.')[0], 10);
  var ok    = !isNaN(major) && major >= 8;
  return {
    name:   'node',
    ok:     ok,
    detail: 'TV Node ' + process.version +
            (ok ? '' : ' (older than the build target, node8)')
  };
}

function probeQml() {
  var mounted = false;
  try {
    mounted = fs.readFileSync('/proc/mounts', 'utf8').indexOf('StarfishVolume.qml') !== -1;
  } catch (e) { /* ignore */ }

  if (!fs.existsSync(QML_PATH)) {
    return {
      name:   'overlay-qml',
      ok:     false,
      detail: 'Compositor volume QML not found at the expected path. The on-screen ' +
              'indicator will not appear on this webOS version.'
    };
  }

  var hasMarker = false;
  try {
    hasMarker = fs.readFileSync(QML_PATH, 'utf8').indexOf('external_arc') !== -1;
  } catch (e) { /* ignore */ }

  if (!hasMarker && !mounted) {
    return {
      name:   'overlay-qml',
      ok:     false,
      detail: 'Volume QML found, but it has no external_arc guard to remove. The ' +
              'patch does not apply to this version.'
    };
  }

  return {
    name:   'overlay-qml',
    ok:     true,
    detail: mounted ? 'patch applied (bind mount active)'
                    : 'patchable; mount not yet applied'
  };
}

function probeHook() {
  var ok = fs.existsSync(HOOK_PATH);
  return {
    name:   'boot-hook',
    ok:     ok,
    detail: ok ? 'installed'
               : 'not installed. The overlay will not survive a power cycle'
  };
}

function probeIptables() {
  var ok = fs.existsSync('/usr/sbin/iptables') || fs.existsSync('/sbin/iptables');
  return {
    name:   'iptables',
    ok:     ok,
    detail: ok ? 'present'
               : 'not found. Ports may stay closed to the overlay and setup app'
  };
}

function probeAudioService(cb) {
  var cmd = '/usr/bin/luna-send -n 1 -f ' +
            'luna://com.webos.service.audio/master/getVolume \'{}\' 2>/dev/null';
  cp.exec(cmd, { timeout: 5000 }, function (err, stdout) {
    if (err || !stdout) {
      cb({
        name:   'luna-audio',
        ok:     false,
        detail: 'com.webos.service.audio did not respond. The TV volume cannot be ' +
                'read or written on this version, so sync will not work.'
      });
      return;
    }
    var vol = null;
    try {
      var j = JSON.parse(stdout);
      vol = (j.volumeStatus && typeof j.volumeStatus.volume === 'number')
        ? j.volumeStatus.volume
        : (typeof j.volume === 'number' ? j.volume : null);
    } catch (e) { /* fall through */ }

    if (vol === null) {
      cb({
        name:   'luna-audio',
        ok:     false,
        detail: 'Audio service responded in an unexpected shape: ' +
                String(stdout).replace(/\s+/g, ' ').slice(0, 200)
      });
      return;
    }
    cb({ name: 'luna-audio', ok: true, detail: 'TV volume reads as ' + vol });
  });
}

// Runs every probe and hands back the full list.
function runProbes(extra, cb) {
  var results = [probeNode(), probeQml(), probeHook(), probeIptables()];
  if (extra && extra.length) results = results.concat(extra);
  probeAudioService(function (audio) {
    results.push(audio);
    cb(results);
  });
}

module.exports = {
  TESTED:        TESTED,
  QML_PATH:      QML_PATH,
  readPlatform:  readPlatform,
  checkPlatform: checkPlatform,
  runProbes:     runProbes
};
