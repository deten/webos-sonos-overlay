'use strict';

var os      = require('os');
var http    = require('http');
var fs      = require('fs');
var path    = require('path');
var cp      = require('child_process');
var WebSocketServer = require('ws').Server;

var genaLib  = require('./lib/gena');
var soapLib  = require('./lib/soap');
var parseLib = require('./lib/parse');
var inputLib = require('./lib/input');
var corrLib  = require('./lib/correlator');
var compatLib = require('./lib/compat');
var diagLib   = require('./lib/diaglog');
var statsLib  = require('./lib/stats');
var pkg       = require('./package.json');

var startListener         = genaLib.startListener;
var subscribe             = genaLib.subscribe;
var renew                 = genaLib.renew;
var unsubscribe           = genaLib.unsubscribe;
var getVolume             = soapLib.getVolume;
var setSonosVolume        = soapLib.setVolume;
var getTransportState     = soapLib.getTransportState;
var parseLastChange       = parseLib.parseLastChange;
var openInputDevicesMulti = inputLib.openInputDevicesMulti;
var detectInputDevice     = inputLib.detectInputDevice;
var Correlator            = corrLib.Correlator;

var EVENT_PATH        = '/MediaRenderer/RenderingControl/Event';
var REQUESTED_TIMEOUT = 1800;
var RENEW_FACTOR      = 0.80;

var DEFAULT_INPUT_DEV   = '/dev/input/event1';
var DEFAULT_LISTEN_PORT = 7474;
var DEFAULT_WS_PORT     = 7475;
var API_PORT            = 7476;

var CONFIG_DIR  = '/var/lib/com.brineandbuild.sonosoverlay';
var CONFIG_FILE = CONFIG_DIR + '/config.json';

// Patching the compositor's volume QML is what puts the number back on screen.
// The boot hook runs this on every power-on, and setup runs it once so a fresh
// install works without needing a reboot first.
var OVERLAY_APPLY_SH =
  'QML=/usr/lib/qml/WebOSCompositor/views/volume/StarfishVolume.qml\n' +
  'PATCHED=/var/lib/com.brineandbuild.sonosoverlay/StarfishVolume.qml\n' +
  'if [ -f "$QML" ] && grep -q external_arc "$QML" && [ ! -f "$PATCHED" ]; then\n' +
  '  mkdir -p /var/lib/com.brineandbuild.sonosoverlay\n' +
  '  sed \'/external_arc/d\' "$QML" > "$PATCHED"\n' +
  'fi\n' +
  'if [ -f "$PATCHED" ] && ! grep -q StarfishVolume.qml /proc/mounts; then\n' +
  '  mount --bind "$PATCHED" "$QML" 2>/dev/null\n' +
  '  systemctl restart surface-manager-daemon.service 2>/dev/null\n' +
  'fi\n';
// Persistent, unlike /var/log which is a ramfs and is wiped on every boot.
var DIAG_FILE    = CONFIG_DIR + '/diagnostics.log';
var NOTABLE_FILE = CONFIG_DIR + '/notable.log';
var STATS_FILE   = CONFIG_DIR + '/stats.json';

// How long after our own SetVolume the Arc is still expected to be catching
// up. Anything it reports inside this window is our doing, not a person.
var WRITE_ECHO_MS = 4000;

// A player that has stopped answering this quickly is worth recording.
var SLOW_PLAYER_MS = 1500;

// How long the session heartbeat waits between writes. It exists so the next
// run can tell roughly when this one stopped, which a killed process cannot
// record for itself.
var HEARTBEAT_MS = 60000;

// Connect retry backoff. The TV cold-boots on every power-on, so the service
// always races Wi-Fi association and DHCP; a single attempt is not enough.
var RETRY_BASE_MS = 3000;
var RETRY_MAX_MS  = 60000;
var SYNC_INTERVAL_MS = 10000;

// The TV leads. Its own counter is what the OSD draws, so we never overwrite it
// and there is nothing to predict; we read the TV's real value and mirror it to
// the Arc. This removes the whole prediction/step-learning path, and with it the
// wrong-first-digit flash that came from fighting LG's own OSD.
//
// The TV moves 1 per press; the Arc moves 2 per CEC press. Measured 2026-08-21
// from isolated presses with nothing writing the TV (TV 9->8->7 one press each,
// while a 17-press burst moved the TV +17 and the Arc +32). So the Arc's volume
// is exactly twice the TV's, and CEC lands on the correct value by itself;
// under this mapping a normal press needs no correction at all. Under the old
// 1:1 map every press overshot the Arc by 1 and the settle yanked it back.
var TV_TO_SONOS_RATIO = 2;

// Absolute ceiling, expressed on the SONOS scale; that is the side that gets
// loud. With the 2:1 mapping this caps the TV at half as much (70 -> TV 35).
// Nothing this service writes to the Sonos may exceed it, whatever the TV says.
// Override per-install with "maxVolume" in config.json.
var DEFAULT_MAX_VOLUME = 70;

// A single correction may never raise the Sonos by more than this. Downward
// corrections are unrestricted; quieter is always safe. Normal use never trips
// it: CEC moves the Arc live during a burst, so the gap stays small.
var MAX_RAISE_PER_CORRECTION = 20;

// A burst is "settled" once the remote has been quiet this long. Correcting
// before then fights the user's own input; under hold-repeat the unmatched-key
// queue never drains, so it cannot be used as the gate on its own.
var SETTLE_MS      = 400;
var SETTLE_POLL_MS = 700;

// No eARC session means CEC presses never reach the Arc, but the TV still
// moves its own counter, and we mirror that counter to the Arc on every settle.
// The old SOAP-fallback special case is therefore gone: one path covers both.

// App directory (same folder as this script, resolves whether bundled or not)
var APP_DIR = path.dirname(process.argv[1] || __filename);

var state = {
  device:        null,
  config:        null,
  sid:           null,
  seqExpected:   0,
  renewTimer:    null,
  correlator:    null,
  inputHandle:   null,
  retryTimer:    null,
  server:        null,
  wss:           null,
  apiServer:     null,
  lastKeyAt:     0,
  genaReceived:  false,
  connecting:    false,
  platform:      null,   // webOS release / model, read once at startup
  compat:        null,   // tested | untested | unknown
  probes:        [],     // dependency probe results
  diag:          null,   // persistent diagnostics log
  tvVol:           null,  // last value read from the TV, the source of truth
  sonosVol:        null,  // last value the Arc reported, via GENA or SOAP
  pendingSonosWrite: null, // a SetVolume we issued, so its echo is not "external"
  optimisticMuted: false,
  maxVolume:       DEFAULT_MAX_VOLUME,
  settleTimer:     null,
  transportState:  null,
  holding:         false,

  // Instrumentation. None of this changes behaviour; it exists so a report
  // from someone else's TV can be read without guessing.
  notable:       null,   // rare entries, kept whatever else fills the log
  stats:         null,   // counters and the session record, across reboots
  burst:         null,   // the keypress burst in progress
  lastWriteAt:   0,      // when we last issued a SetVolume
  lastWriteVal:  null,   // what we asked for
  lastGenaAt:    0,      // when the player last told us anything
  genaCount:     0,      // events this session, for spotting a silent player
  pollGenaMark:  0,      // genaCount as of the previous poll
};

// ---------------------------------------------------------------------------
// Diagnostics report
// ---------------------------------------------------------------------------
// Addresses are masked. This report is written to be pasted into a public bug
// report, and nothing in it should identify the user's network or hardware
// beyond the TV model and firmware needed to reproduce the problem.
function maskIps(text) {
  return String(text).replace(
    /\b(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})\b/g,
    function (m, a, b) { return a + '.' + b + '.x.x'; });
}

// ---------------------------------------------------------------------------
// Instrumentation helpers.
// detail() is the running commentary. note() is for the handful of entries
// that must survive two days of it, and goes to both files so the detail log
// still reads in order.
// ---------------------------------------------------------------------------
function detail(msg, level) {
  if (state.diag) state.diag.write(level || 'info', msg);
}

function note(msg, level) {
  if (state.notable) state.notable.write(level || 'warn', msg);
  if (state.diag)    state.diag.write(level || 'warn', msg);
}

function bump(key, by) { if (state.stats) state.stats.bump(key, by); }
function peak(key, v)  { if (state.stats) state.stats.max(key, v); }

function agoMs(t) { return t ? (Date.now() - t) : null; }
function agoStr(t) {
  var ms = agoMs(t);
  return ms === null ? 'never' : (ms < 10000 ? ms + 'ms' : Math.round(ms / 1000) + 's');
}

// What the last run left behind. A process killed by a power cut cannot
// record its own ending, so the heartbeat it wrote while alive is the closest
// we get to knowing when the TV went off and whether it shut down properly.
function reportPreviousSession(prev) {
  state.prevSession = prev && prev.sid ? prev : null;
  if (!prev || !prev.startedAt) return;

  var endedAgo = prev.lastSeenAt
    ? Math.round((Date.now() - Date.parse(prev.lastSeenAt)) / 1000) : null;

  if (prev.cleanExit) {
    bump('clean_exits');
    detail('previous session ended cleanly at ' + (prev.exitAt || prev.lastSeenAt));
  } else {
    bump('unclean_exits');
    note('previous session did not shut down cleanly. Last heartbeat ' +
         (prev.lastSeenAt || 'unknown') +
         (endedAgo === null ? '' : ', about ' + endedAgo + 's ago') +
         '. Its subscription may still be held by the player.');
  }
}

// Cancel anything the previous run left on the player, and record whether it
// was still there. This is the one question a log from another house cannot
// otherwise answer: does a power cut really strand a subscription?
async function probeStaleSubscription(device) {
  var prev = state.prevSession;
  if (!prev || !prev.sid) return;
  state.prevSession = null;   // once per run

  var heldFor = prev.lastSeenAt
    ? Math.round((Date.now() - Date.parse(prev.lastSeenAt)) / 1000) : null;

  try {
    var res = await genaLib.unsubscribeStatus(device, prev.sid, EVENT_PATH);
    if (res.statusCode === 200) {
      bump('stale_subs_alive');
      note('STALE SUB: the player was still holding the previous run\'s ' +
           'subscription' +
           (heldFor === null ? '' : ', ' + heldFor + 's after that run stopped') +
           '. Cancelled it now. For that whole gap the player still had this ' +
           'TV listed as a subscriber and was posting events to it.');
    } else {
      detail('previous subscription already released (HTTP ' + res.statusCode + ')');
    }
  } catch (e) {
    detail('stale subscription probe failed: ' + e.message, 'warn');
  }
}

// Written while alive so the next run can see roughly when this one stopped.
function startHeartbeat() {
  setInterval(function () {
    if (!state.stats) return;
    state.stats.setSession({ lastSeenAt: new Date().toISOString() }, true);
  }, HEARTBEAT_MS);
}

function refreshPlatform() {
  if (state.platform && !compatLib.isIncomplete(state.platform)) return;
  var fresh = compatLib.readPlatform();
  if (!compatLib.isIncomplete(fresh) || !state.platform) {
    state.platform = fresh;
    state.compat   = compatLib.checkPlatform(fresh);
  }
}

function buildDiagnosticsReport() {
  var L = [];
  var p = state.platform || {};
  var c = state.compat   || {};

  L.push('Sonos Overlay diagnostics');
  L.push('=========================');
  L.push('generated:   ' + new Date().toISOString());
  L.push('app version: ' + pkg.version);
  L.push('');
  // One line carrying everything needed to decide whether this TV is supported,
  // so a report can be triaged without reading the rest of the file.
  L.push('DEVICE: ' + (p.model || 'unknown model') +
         ' / webOS ' + (p.release || 'unknown') +
         ' / node ' + (p.node || 'unknown') +
         ' / ' + (c.status || 'unknown'));
  L.push('');
  L.push('Platform');
  L.push('--------');
  L.push('webOS release: ' + (p.release || 'unknown') +
         (p.source ? '  (via ' + p.source + ')' : ''));
  L.push('model:         ' + (p.model || 'unknown') +
         (p.board ? '  [' + p.board + ']' : ''));
  L.push('TV node:       ' + (p.node || 'unknown'));
  L.push('compatibility: ' + (c.status || 'unknown').toUpperCase());
  L.push('               ' + (c.message || ''));
  if (c.status === 'untested' && c.testedOn) {
    L.push('               tested releases: ' + c.testedOn.join(', '));
  }
  L.push('');
  L.push('Dependency checks');
  L.push('-----------------');
  if (!state.probes.length) {
    L.push('(not yet run)');
  } else {
    state.probes.forEach(function (r) {
      L.push((r.ok ? '[ ok ] ' : '[FAIL] ') +
             (r.name + '            ').slice(0, 13) + ' ' + r.detail);
    });
  }
  L.push('');
  L.push('Runtime state');
  L.push('-------------');
  L.push('configured:     ' + !!state.config);
  L.push('sonos model:    ' + ((state.config && state.config.sonosModel) || 'n/a'));
  L.push('subscribed:     ' + !!state.sid);
  L.push('gena received:  ' + state.genaReceived);
  L.push('overlay clients:' + (state.wss ? state.wss.clients.size : 0));
  L.push('TV volume:      ' + state.tvVol);
  L.push('Sonos volume:   ' + state.sonosVol +
         '   (expected ' + (state.tvVol === null ? 'n/a' : tvToSonos(state.tvVol)) + ')');
  L.push('ceiling:        ' + state.maxVolume + ' Sonos / ' + maxTvVol() + ' TV');
  L.push('transport:      ' + state.transportState);
  L.push('last key:       ' + (state.lastKeyAt
    ? Math.round((Date.now() - state.lastKeyAt) / 1000) + 's ago' : 'none'));
  L.push('last event:     ' + (state.lastGenaAt
    ? Math.round((Date.now() - state.lastGenaAt) / 1000) + 's ago' : 'none this session'));
  L.push('');
  summaryLines().forEach(function (s) { L.push(s); });
  L.push('');
  L.push('Notable events');
  L.push('--------------');
  L.push(state.notable ? (state.notable.read() || '(none)') : '(unavailable)');
  L.push('');
  L.push('Event log');
  L.push('---------');
  L.push(state.diag ? (state.diag.read() || '(empty)') : '(unavailable)');

  return maskIps(L.join('\n'));
}

// Counts covering every session since the counters were last cleared, so a
// capture spanning several days can be triaged before reading any of the log.
function summaryLines() {
  var L = [];
  L.push('Totals since ' + (state.stats ? state.stats.since() : 'n/a'));
  L.push('-----------------------------------------');
  if (!state.stats) { L.push('(unavailable)'); return L; }

  var c = state.stats.counters();
  function n(k) { return c[k] || 0; }
  function row(label, value, note) {
    L.push((label + '                        ').slice(0, 24) + value +
           (note ? '   ' + note : ''));
  }

  row('sessions',        n('sessions'));
  row('clean shutdowns', n('clean_exits'),
      n('sessions') > 1 ? '(of ' + (n('sessions') - 1) + ' ended)' : '');
  row('stale subs found', n('stale_subs_alive'),
      'subscriptions the player still held at the next start');
  L.push('');
  row('key bursts',      n('bursts'));
  row('keys pressed',    n('keys'));
  row('corrections',     n('corrections'),  'volume writes we sent the player');
  row('correction miss', n('correction_missed'),
      'writes where the player did not end up where asked');
  row('adoptions',       n('adoptions'),    'external changes pulled into the TV');
  row('  while pending', n('adoptions_while_pending'),
      'adopted while our own correction was still settling');
  L.push('');
  row('events received', n('events'));
  row('event gaps',      n('event_gaps'),
      'player volume moved with no event sent');
  row('longest gap',     n('max_gena_gap_s') + 's',
      'between events while the TV was awake');
  row('renew failures',  n('renew_failures'));
  row('resubscribes',    n('resubscribes'));
  row('slow responses',  n('slow_player'),  'over ' + SLOW_PLAYER_MS + 'ms');
  row('slowest response', n('max_player_ms') + 'ms');

  var prev = state.stats.prevSession();
  if (prev && prev.lastSeenAt) {
    L.push('');
    L.push('previous session: last heartbeat ' + prev.lastSeenAt +
           ', exit ' + (prev.cleanExit ? 'clean' : 'not recorded'));
  }
  return L;
}

// ---------------------------------------------------------------------------
// Config helpers
// ---------------------------------------------------------------------------
function readConfig() {
  try {
    return JSON.parse(fs.readFileSync(CONFIG_FILE, 'utf8'));
  } catch (e) { return null; }
}

function saveConfig(cfg) {
  try {
    if (!fs.existsSync(CONFIG_DIR)) fs.mkdirSync(CONFIG_DIR, { recursive: true });
    fs.writeFileSync(CONFIG_FILE, JSON.stringify(cfg, null, 2), 'utf8');
  } catch (e) { console.error('[config] write failed:', e.message); }
}

// ---------------------------------------------------------------------------
// iptables: open ports silently; errors are non-fatal
// ---------------------------------------------------------------------------
function openPorts() {
  [DEFAULT_LISTEN_PORT, DEFAULT_WS_PORT, API_PORT].forEach(function(p) {
    cp.exec('iptables -I INPUT -p tcp --dport ' + p + ' -j ACCEPT 2>/dev/null');
  });
}

// ---------------------------------------------------------------------------
// SSDP scan for Sonos devices
// ---------------------------------------------------------------------------
function scanSonos(timeoutMs) {
  return new Promise(function(resolve) {
    var Client = require('node-ssdp').Client;
    var client = new Client();
    var found  = {};

    client.on('response', function(headers) {
      var loc = headers.LOCATION || headers.location || '';
      var m   = loc.match(/http:\/\/([\d.]+):(\d+)/);
      if (m) {
        var ip = m[1], port = parseInt(m[2], 10);
        if (!found[ip]) found[ip] = { ip: ip, port: port, name: ip, model: '', uuid: '' };
      }
    });

    client.search('urn:schemas-upnp-org:device:ZonePlayer:1');

    setTimeout(function() {
      client.stop();
      var devices = Object.values(found);
      if (!devices.length) { resolve([]); return; }

      var pending = devices.length;
      devices.forEach(function(dev) {
        var req = http.get('http://' + dev.ip + ':' + dev.port + '/xml/device_description.xml', function(res) {
          var data = '';
          res.on('data', function(c) { data += c; });
          res.on('end', function() {
            var rm = data.match(/<roomName>([^<]+)<\/roomName>/);
            var mm = data.match(/<modelName>([^<]+)<\/modelName>/);
            var um = data.match(/<UDN>([^<]+)<\/UDN>/);
            if (rm) dev.name  = rm[1];
            if (mm) dev.model = mm[1];
            // UDN is the only stable identifier; roomName is not unique
            // (Arc, Sub and both Era 300s all report "Family Room").
            if (um) dev.uuid  = um[1];
            if (--pending === 0) resolve(devices);
          });
        });
        req.on('error', function() { if (--pending === 0) resolve(devices); });
        req.setTimeout(3000, function() { req.abort(); });
      });
    }, timeoutMs || 5000);
  });
}

// ---------------------------------------------------------------------------
// HTTP API server (:7476)
// ---------------------------------------------------------------------------
function startApiServer() {
  state.apiServer = http.createServer(function(req, res) {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
    res.setHeader('Content-Type', 'application/json');

    if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }

    var url = req.url.split('?')[0];

    if (url === '/api/status' && req.method === 'GET') {
      res.end(JSON.stringify({
        ok:           true,
        configured:   !!state.config,
        connecting:   state.connecting,
        connected:    !!state.sid,
        sonosIp:      state.config ? state.config.sonosIp   : null,
        sonosName:    state.config ? state.config.sonosName  : null,
        lastVol:      state.correlator ? state.correlator.lastVol   : null,
        lastMuted:    state.correlator ? state.correlator.lastMuted  : null,
        wsClients:    state.wss ? state.wss.clients.size : 0,
        lastKeyAt:    state.lastKeyAt,
        genaReceived: state.genaReceived,
        platform:     state.platform,
        compat:       state.compat,
        probes:       state.probes,
        tvIp:         detectLanIp(),
      }));
      return;
    }

    // Plain text so it can be opened in a browser and saved straight to a file.
    if (url === '/api/diagnostics' && req.method === 'GET') {
      res.setHeader('Content-Type', 'text/plain; charset=utf-8');
      res.setHeader('Content-Disposition',
        'attachment; filename="sonos-overlay-diagnostics.txt"');
      refreshPlatform();
      // Re-probe so the report describes the TV now, not at startup.
      runProbes(function() {
        res.end(buildDiagnosticsReport());
      });
      return;
    }

    if (url === '/api/diagnostics' && req.method === 'DELETE') {
      if (state.diag)    state.diag.clear();
      if (state.notable) state.notable.clear();
      if (state.stats)   state.stats.reset();
      res.end(JSON.stringify({ ok: true }));
      return;
    }

    if (url === '/api/scan' && req.method === 'GET') {
      console.log('[api] SSDP scan starting...');
      scanSonos(5000).then(function(devices) {
        console.log('[api] scan found', devices.length, 'device(s)');
        res.end(JSON.stringify({ ok: true, devices: devices }));
      });
      return;
    }

    if (url === '/api/test' && req.method === 'POST') {
      readBody(req, function(body) {
        var ip   = body.sonosIp;
        var port = body.sonosPort || 1400;
        if (!ip) { res.end(JSON.stringify({ ok: false, error: 'Missing sonosIp' })); return; }
        getVolume({ ip: ip, port: port }).then(function(vol) {
          res.end(JSON.stringify({ ok: true, volume: vol }));
        }).catch(function(e) {
          res.end(JSON.stringify({ ok: false, error: e.message }));
        });
      });
      return;
    }

    if (url === '/api/config' && req.method === 'POST') {
      readBody(req, function(body) {
        if (!body.sonosIp) { res.end(JSON.stringify({ ok: false, error: 'Missing sonosIp' })); return; }
        saveConfig(body);
        state.config = body;
        console.log('[api] config saved, connecting to Sonos at', body.sonosIp);
        connectToSonos(body).catch(function(e) {
          console.error('[api] connect error after config save:', e.message);
        });
        res.end(JSON.stringify({ ok: true }));
      });
      return;
    }

    if (url === '/api/setup-mode' && req.method === 'POST') {
      // Tell any running overlay to hide itself so it doesn't block setup input
      if (state.wss) {
        state.wss.clients.forEach(function(client) {
          if (client.readyState === 1) client.send(JSON.stringify({ type: 'setup' }));
        });
      }
      res.end(JSON.stringify({ ok: true }));
      return;
    }

    // Applies the on-screen indicator now, without waiting for a reboot.
    // Restarting the compositor tears down whatever is on screen, including the
    // setup app itself, so this is only called as the last step of setup.
    if (url === '/api/apply-overlay' && req.method === 'POST') {
      var mounted = false;
      try {
        mounted = fs.readFileSync('/proc/mounts', 'utf8')
                    .indexOf('StarfishVolume.qml') !== -1;
      } catch (e) { /* treat as not mounted */ }

      if (mounted) {
        res.end(JSON.stringify({ ok: true, alreadyApplied: true }));
        return;
      }

      // Answer before restarting; the restart kills the webview waiting on us.
      res.end(JSON.stringify({ ok: true, restarting: true }));
      setTimeout(function() {
        cp.exec(OVERLAY_APPLY_SH, function(err) {
          if (err && state.diag) {
            state.diag.error('apply-overlay failed: ' + err.message);
          }
        });
      }, 600);
      return;
    }

    if (url === '/api/startup-hook' && req.method === 'POST') {
      var hookDir  = '/var/lib/webosbrew/init.d';
      var hookPath = hookDir + '/sonos-overlay';
      // The QML steps come from OVERLAY_APPLY_SH so the boot path and the
      // setup path can never drift apart.
      var script   = '#!/bin/sh\n' +
        'iptables -I INPUT -p tcp --dport 7474 -j ACCEPT 2>/dev/null\n' +
        'iptables -I INPUT -p tcp --dport 7475 -j ACCEPT 2>/dev/null\n' +
        'iptables -I INPUT -p tcp --dport 7476 -j ACCEPT 2>/dev/null\n' +
        OVERLAY_APPLY_SH +
        'pkill -f \'tv-service.bundle.js\' 2>/dev/null\n' +
        'sleep 1\n' +
        'APP=/media/developer/apps/usr/palm/applications/com.brineandbuild.sonosoverlay\n' +
        'nohup /usr/bin/node "$APP/tv-service.bundle.js" >> /var/log/sonos-overlay.log 2>&1 &\n';
      try {
        if (!fs.existsSync(hookDir)) fs.mkdirSync(hookDir, { recursive: true });
        fs.writeFileSync(hookPath, script, 'utf8');
        fs.chmodSync(hookPath, 0o755);
        res.end(JSON.stringify({ ok: true }));
      } catch (e) {
        res.end(JSON.stringify({ ok: false, error: e.message }));
      }
      return;
    }

    res.writeHead(404);
    res.end(JSON.stringify({ ok: false, error: 'Not found' }));
  });

  state.apiServer.listen(API_PORT, function() {
    console.log('[api]  setup server on :' + API_PORT);
  });
}

function readBody(req, cb) {
  var raw = '';
  req.on('data', function(c) { raw += c; });
  req.on('end', function() {
    try { cb(JSON.parse(raw || '{}')); } catch (e) { cb({}); }
  });
}

// ---------------------------------------------------------------------------
// Locate the configured Sonos.
// Tries the stored IP first, then falls back to SSDP and re-identifies the
// player by UDN. DHCP moved the whole subnet once already, which left the
// service permanently dead against a stale address.
// ---------------------------------------------------------------------------
async function resolveDevice(config) {
  var port = config.sonosPort || 1400;

  if (config.sonosIp) {
    try {
      await getVolume({ ip: config.sonosIp, port: port });
      return { ip: config.sonosIp, port: port };
    } catch (e) {
      console.warn('[resolve] stored IP', config.sonosIp, 'unreachable:', e.message);
      if (state.diag) {
        state.diag.info('stored Sonos IP unreachable (' + e.message +
          '), falling back to discovery');
      }
    }
  }

  console.log('[resolve] scanning for the configured player...');
  var devices = await scanSonos(5000);
  if (!devices.length) throw new Error('no Sonos devices found on the network');

  var match = null;

  if (config.sonosUuid) {
    match = devices.filter(function(d) { return d.uuid === config.sonosUuid; })[0] || null;
    if (match) console.log('[resolve] matched stored UUID at', match.ip);
  }

  // Fall back to room name, but only when it identifies exactly one player 
  // several speakers share a room name, so an ambiguous match is a wrong match.
  if (!match && config.sonosName) {
    var byName = devices.filter(function(d) {
      return d.name === config.sonosName &&
             (!config.sonosModel || d.model === config.sonosModel);
    });
    if (byName.length === 1) {
      match = byName[0];
      console.log('[resolve] matched name+model at', match.ip);
    } else if (byName.length > 1) {
      console.warn('[resolve]', byName.length, 'players share room "' + config.sonosName +
        '", cannot disambiguate without a stored UUID');
    }
  }

  if (!match) throw new Error('could not identify the configured Sonos among ' + devices.length + ' device(s)');

  config.sonosIp = match.ip;
  if (match.uuid)  config.sonosUuid  = match.uuid;
  if (match.model) config.sonosModel = match.model;
  saveConfig(config);
  state.config = config;

  return { ip: match.ip, port: port };
}

// ---------------------------------------------------------------------------
// Connect to Sonos (called on startup if configured, or after /api/config)
// Retries with capped exponential backoff, never gives up.
// ---------------------------------------------------------------------------
async function connectToSonos(config, attempt) {
  if (state.connecting) return;
  state.connecting = true;
  attempt = attempt || 0;

  if (state.retryTimer) { clearTimeout(state.retryTimer); state.retryTimer = null; }

  // Tear down existing subscription if re-configuring
  if (state.sid) {
    try { await unsubscribe(state.device, state.sid, EVENT_PATH); } catch (e) {}
    state.sid = null; state.seqExpected = 0; state.genaReceived = false;
  }
  if (state.renewTimer) { clearTimeout(state.renewTimer); state.renewTimer = null; }

  var args = parseArgs();

  try {
    var device = await resolveDevice(config);
    state.device = device;

    var callbackIp  = args.callbackIp || detectLanIp();
    var callbackUrl = 'http://' + callbackIp + ':' + DEFAULT_LISTEN_PORT + '/notify';

    console.log('[main] Sonos:    ', device.ip + ':' + device.port);
    console.log('[main] callback: ', callbackUrl);

    state.maxVolume = (config && typeof config.maxVolume === 'number')
      ? Math.max(1, Math.min(100, config.maxVolume))
      : DEFAULT_MAX_VOLUME;
    console.log('[main] volume ceiling:', state.maxVolume, 'Sonos =', 
      Math.floor(state.maxVolume / TV_TO_SONOS_RATIO), 'TV');

    state.correlator = new Correlator({ windowMs: 3000 });
    var initVol = await getVolume(device);
    state.correlator.lastVol   = initVol;
    state.correlator.lastMuted = false;
    console.log('[main] current volume:', initVol);

    // Boot seed, the one time the Sonos leads. The TV keeps whatever value it
    // booted with, so align it to the Arc once; from here on the TV leads and
    // this is never written again in normal operation. Seeding also guarantees
    // the two agree before the first correction, so switching the direction of
    // control can never produce an audible jump.
    var seed = Math.min(sonosToTv(initVol), maxTvVol());
    state.sonosVol        = initVol;
    state.tvVol           = seed;
    state.optimisticMuted = false;
    console.log('[main] seeding TV to', seed, 'from Arc', initVol, '(2:1)');
    pushTvVolume(seed);
    refreshTransportState();

    // Learn and persist the UUID so a future address change can be resolved.
    if (!config.sonosUuid) {
      scanSonos(5000).then(function(devices) {
        var self = devices.filter(function(d) { return d.ip === device.ip; })[0];
        if (self && self.uuid) {
          config.sonosUuid  = self.uuid;
          config.sonosModel = self.model;
          saveConfig(config);
          console.log('[resolve] learned UUID', self.uuid);
        }
      }).catch(function() {});
    }

    // Before taking a new subscription, clear any the last run left behind,
    // and record whether the player was still holding it.
    await probeStaleSubscription(device);

    var sub = await subscribe(device, callbackUrl, EVENT_PATH, REQUESTED_TIMEOUT);
    state.sid = sub.sid;
    console.log('[gena] subscribed SID:', sub.sid, '| negotiated:', sub.negotiatedSeconds + 's');
    detail('subscribed, timeout ' + sub.negotiatedSeconds + 's (asked for ' +
           REQUESTED_TIMEOUT + 's)');
    if (state.stats) {
      state.stats.setSession({
        startedAt:  state.stats.session().startedAt || new Date().toISOString(),
        sid:        sub.sid,
        lastSeenAt: new Date().toISOString(),
        cleanExit:  false
      }, true);
    }
    scheduleRenew(device, callbackUrl, sub.negotiatedSeconds);

    // Input devices (only open once)
    if (!state.inputHandle) {
      var detected = await detectInputDevice();
      var devPaths = detected.candidates.length > 0
        ? detected.candidates.map(function(c) { return c.path; })
        : [DEFAULT_INPUT_DEV];
      console.log('[input] listening on', devPaths.length, 'device(s):', devPaths.join(', '));
      state.inputHandle = openInputDevicesMulti(devPaths, onInputEvent, function(err) {
        console.error('[input] fatal:', err.message);
        if (state.diag) state.diag.error('input reader fatal: ' + err.message);
      });
    }

    console.log('\n[main] ready, press Vol+/Vol−/Mute on the remote.\n');
  } catch (e) {
    console.error('[main] connect failed (attempt ' + (attempt + 1) + '):', e.message);
    if (state.diag) {
      state.diag.change('connect-fail', e.message,
        'connect to Sonos failed: ' + e.message);
    }
    var delayMs = Math.min(RETRY_BASE_MS * Math.pow(2, attempt), RETRY_MAX_MS);
    console.log('[main] retrying in', Math.round(delayMs / 1000) + 's');
    state.retryTimer = setTimeout(function() {
      connectToSonos(config, attempt + 1);
    }, delayMs);
  } finally {
    state.connecting = false;
  }
}

// ---------------------------------------------------------------------------
// /dev/input event handler
// ---------------------------------------------------------------------------
function onInputEvent(event) {
  state.lastKeyAt = Date.now();
  recordKeyInBurst(event);
  var label = event.value === 1 ? 'down  ' : 'repeat';
  console.log('[input]', label, 'dir=' + event.direction,
    '| kernel=' + event.kernelSec + '.' + pad6(event.kernelUsec),
    '| src=' + (event.sourceDev || '?'));
  if (state.correlator) state.correlator.recordKeypress(event.direction, event.recvAt);
  // value 1 = discrete press, 2 = auto-repeat from holding the button down.
  state.holding = (event.value === 2);
  onVolumeKey(event.direction);
}

// A burst is everything from the first press until the settle that follows it.
// Logging one line per burst rather than per press keeps two days of use
// readable, and the before and after values on both sides are what show
// whether a hold behaves differently from single presses.
function recordKeyInBurst(event) {
  if (!state.burst) {
    state.burst = {
      startedAt: Date.now(),
      tvBefore:  state.tvVol,
      arcBefore: state.sonosVol,
      keys:      0,
      repeats:   0,
      dirs:      {}
    };
    bump('bursts');
  }
  var b = state.burst;
  b.keys++;
  if (event.value === 2) b.repeats++;
  b.dirs[event.direction] = (b.dirs[event.direction] || 0) + 1;
  b.lastAt = Date.now();
  bump('keys');
}

// Called once the burst has settled and any correction has been issued.
function closeBurst(wrote) {
  var b = state.burst;
  if (!b) return;
  state.burst = null;

  var dirs = Object.keys(b.dirs).map(function (d) {
    return d + 'x' + b.dirs[d];
  }).join(' ');

  detail('burst: ' + b.keys + ' key(s) [' + dirs + ']' +
         (b.repeats ? ' incl ' + b.repeats + ' held' : '') +
         ' over ' + (b.lastAt - b.startedAt) + 'ms' +
         ' | TV ' + b.tvBefore + ' -> ' + state.tvVol +
         ' | Arc ' + b.arcBefore + ' -> ' + state.sonosVol +
         (wrote === null ? ' | no correction needed'
                         : ' | corrected Arc to ' + wrote));
}

// The TV has already drawn its own number by the time this runs; that is the
// snappiness, and it now costs nothing because we no longer overwrite it. All
// this does is arm the timer that mirrors the result to the Arc.
function onVolumeKey(direction) {
  if (direction === 'mute') {
    state.optimisticMuted = !state.optimisticMuted;
    return;
  }
  scheduleSettle();
}

function settled() { return (Date.now() - state.lastKeyAt) > SETTLE_MS; }

// Clamp anything bound for the Sonos to the configured ceiling.
function clampVol(v) {
  return Math.max(0, Math.min(state.maxVolume, v));
}

// The two scales. Sonos = TV * 2, so both sides advance in lockstep per press.
function tvToSonos(tvVol)    { return Math.max(0, Math.min(100, tvVol * TV_TO_SONOS_RATIO)); }
function sonosToTv(sonosVol) { return Math.round(sonosVol / TV_TO_SONOS_RATIO); }

// The ceiling on the TV's own scale.
function maxTvVol() { return Math.floor(state.maxVolume / TV_TO_SONOS_RATIO); }

function refreshTransportState() {
  if (!state.device) return;
  getTransportState(state.device).then(function(ts) {
    if (ts && ts !== state.transportState) {
      console.log('[arc] transport state:', state.transportState, '->', ts);
      // Worth recording: a report of the app showing the wrong thing is much
      // easier to read against what the player was actually saying at the time.
      detail('transport: ' + state.transportState + ' -> ' + ts);
    }
    if (ts) state.transportState = ts;
  }).catch(function() {});
}

// After the remote goes quiet, take the Sonos at its word. This is the only
// path that is guaranteed to run; GENA may have delivered its last event
// mid-burst, and during silent playback it does not emit at all.
function scheduleSettle() {
  if (state.settleTimer) clearTimeout(state.settleTimer);
  state.settleTimer = setTimeout(function() {
    state.settleTimer = null;
    if (!settled()) { scheduleSettle(); return; }
    reconcileTvAndSonos();
  }, SETTLE_POLL_MS);
}

// The TV leads, always. Read its counter, mirror it to the Arc. Nothing here
// writes back to the TV except to enforce the ceiling, so the number on screen
// is never yanked out from under the user, which is exactly what the old
// two-way version did on every single press once the two sides had drifted.
function reconcileTvAndSonos() {
  if (!state.device) return;
  readTvVolume(function(tvVol) {
    if (tvVol === null) return;
    state.tvVol = tvVol;

    // Ceiling. Hold the TV at the cap as well, so the number on screen stays
    // honest instead of climbing past a level the Arc will never reach.
    var tvCap = maxTvVol();
    if (tvVol > tvCap) {
      console.warn('[cap] TV at', tvVol, ', holding at TV ceiling', tvCap,
        '(Sonos ' + state.maxVolume + ')');
      pushTvVolume(tvCap);
      state.tvVol = tvCap;
    }

    var askedAt = Date.now();
    getVolume(state.device).then(function(sonosVol) {
      recordPlayerLatency(Date.now() - askedAt);
      checkForMissedEvent(sonosVol);

      state.sonosVol = sonosVol;
      var target = clampVol(tvToSonos(state.tvVol));
      if (target === sonosVol) { closeBurst(null); return; }

      // Never jump the Arc up by a lot in one correction. Going down is always
      // safe so it is unrestricted; the next poll closes any remaining gap.
      if (target > sonosVol + MAX_RAISE_PER_CORRECTION) {
        target = sonosVol + MAX_RAISE_PER_CORRECTION;
        console.warn('[cap] limiting raise to', target, '- TV asked for', state.tvVol);
        note('raise limited to ' + target + ', TV asked for Arc ' +
             tvToSonos(state.tvVol), 'warn');
      }

      console.log('[settle] TV', state.tvVol, '(wants Arc ' + tvToSonos(state.tvVol) + ')',
        '| Arc', sonosVol, '-> setting Arc to', target);
      state.pendingSonosWrite = target;
      state.lastWriteAt  = Date.now();
      state.lastWriteVal = target;
      bump('corrections');
      detail('CORRECT: TV ' + state.tvVol + ' wants Arc ' +
             tvToSonos(state.tvVol) + ', Arc was ' + sonosVol +
             ', writing ' + target +
             ' | last key ' + agoStr(state.lastKeyAt));
      closeBurst(target);
      verifyWrite(target, sonosVol);

      setSonosVolume(state.device, target).catch(function(e) {
        state.pendingSonosWrite = null;
        console.error('[settle] SetVolume failed:', e.message);
        bump('write_failures');
        note('SetVolume to ' + target + ' failed: ' + e.message, 'error');
        if (state.diag) {
          state.diag.change('setvol-fail', e.message, 'SetVolume failed: ' + e.message);
        }
      });
    }).catch(function(e) {
      recordPlayerLatency(Date.now() - askedAt);
      bump('read_failures');
      if (state.diag) {
        state.diag.change('getvol-fail', e && e.message,
          'reading the player volume failed: ' + (e && e.message));
      }
    });
  });
}

// A player that has gone slow to answer is a player in trouble, and it is the
// cheapest health signal we have that does not depend on events arriving.
function recordPlayerLatency(ms) {
  peak('max_player_ms', ms);
  if (ms < SLOW_PLAYER_MS) return;
  bump('slow_player');
  if (state.diag) {
    state.diag.change('slow-player', Math.round(ms / 1000),
      'the player took ' + ms + 'ms to answer a volume read');
  }
}

// The test for a player that has stopped sending events. We poll anyway, so
// if a poll finds the volume somewhere we were never told about, the event
// channel is not delivering. That is the difference between the player being
// quiet because nothing happened and being silent because it is wedged.
function checkForMissedEvent(polledVol) {
  var believed  = state.sonosVol;
  var newEvents = state.genaCount - state.pollGenaMark;
  state.pollGenaMark = state.genaCount;

  if (believed === null || polledVol === believed) return;
  if (newEvents > 0) return;   // it did tell us, we simply polled as well

  bump('event_gaps');
  note('EVENT GAP: player volume moved ' + believed + ' -> ' + polledVol +
       ' and no event was sent (last event ' + agoStr(state.lastGenaAt) +
       ', our last write ' + agoStr(state.lastWriteAt) + ')');
}

// Did the player actually end up where we asked? Skipped when the user has
// touched the remote since, or when a newer write has superseded this one,
// because then a mismatch is expected rather than interesting.
function verifyWrite(target, before) {
  var writeAt = state.lastWriteAt;
  setTimeout(function () {
    if (!state.device) return;
    if (state.lastWriteAt !== writeAt) return;
    if (state.lastKeyAt > writeAt) return;

    getVolume(state.device).then(function (v) {
      if (v === target) return;   // the normal case, and not worth a line
      bump('correction_missed');
      note('VERIFY: asked the player for ' + target + ' (it was ' + before +
           '), ' + Math.round(WRITE_ECHO_MS / 1000) + 's later it is on ' + v);
    }).catch(function () {});
  }, WRITE_ECHO_MS);
}

// GENA is read-only now: it tells us where the Arc is and nothing more, and it
// never writes to the TV on the keypress path.
//
// The old version learned the per-press "step" from these events without ever
// checking that a keypress had caused them. A volume change made in the Sonos
// app therefore taught it a step of 9 or 10, and every later press jumped by
// that much. There is no step to learn any more; we read the TV's real value.
function reconcile(genaVol, genaMuted) {
  if (genaMuted !== null) state.optimisticMuted = genaMuted;
  if (genaVol === null) return;

  var prev = state.sonosVol;
  state.sonosVol = genaVol;

  // Our own SetVolume echoing back.
  if (state.pendingSonosWrite !== null && genaVol === state.pendingSonosWrite) {
    state.pendingSonosWrite = null;
    return;
  }

  // A CEC press is still in flight (GENA lands ~95ms after the key, well inside
  // SETTLE_MS). The settle timer owns that case.
  if (!settled()) return;

  // Nothing we did, and no key was pressed: someone moved it in the Sonos app.
  // Adopt it into the TV rather than reverting it, so app control keeps working
  //, otherwise TV-leads would silently undo every change made from a phone.
  if (state.tvVol !== null && genaVol !== tvToSonos(state.tvVol)) {
    var adopted = Math.min(sonosToTv(genaVol), maxTvVol());
    console.log('[extern] Arc moved', prev, '->', genaVol,
      'with no keypress, adopting into TV as', adopted);

    // Everything needed to judge afterwards whether this adoption was right.
    // Two cases look identical here and are not: a person on their phone, and
    // the player still catching up from presses we just made.
    var sinceWrite = agoMs(state.lastWriteAt);
    var stillSettling = sinceWrite !== null && sinceWrite < WRITE_ECHO_MS;
    bump('adoptions');
    if (stillSettling) bump('adoptions_while_pending');

    (stillSettling ? note : detail)(
      'ADOPT: Arc ' + prev + ' -> ' + genaVol + ', TV ' + state.tvVol +
      ' -> ' + adopted +
      ' | last key ' + agoStr(state.lastKeyAt) +
      ' | our last write ' + agoStr(state.lastWriteAt) +
      (state.lastWriteVal === null ? '' : ' (asked ' + state.lastWriteVal + ')') +
      (stillSettling ? ' | SUSPECT: our own correction was still settling' : ''));

    state.tvVol = adopted;
    pushTvVolume(adopted);
  }
}

// ---------------------------------------------------------------------------
// GENA NOTIFY handler
// ---------------------------------------------------------------------------
function onGenaNotify(headers, rawBody, recvAt) {
  var sid = headers['sid'] || '';
  var seq = parseInt(headers['seq'] || '0', 10);

  if (sid === state.sid) {
    if (seq !== state.seqExpected && !(seq === 0 && state.seqExpected > 0))
      console.warn('[gena] SEQ gap: expected', state.seqExpected, 'got', seq);
    state.seqExpected = seq + 1;
  }

  var entries;
  try { entries = parseLastChange(rawBody); }
  catch (e) { console.error('[parse] error:', e.message); return; }

  var elapsed    = process.hrtime(recvAt);
  var dispatchUs = Math.round((elapsed[0] * 1e9 + elapsed[1]) / 1000);

  var masterVol = null, muted = null;
  entries.forEach(function(e) {
    if (e.name === 'Volume' && e.channel === 'Master') masterVol = Number(e.val);
    if (e.name === 'Mute'   && e.channel === 'Master') muted = (e.val !== 0 && e.val !== '0');
  });

  var summary = entries.map(function(e) { return e.name + '[' + e.channel + ']=' + e.val; }).join(' ');
  console.log('[gena]', 'SEQ=' + seq, '|', summary, '| recv→parsed=' + dispatchUs + 'µs');

  if (masterVol !== null || muted !== null) {
    state.genaReceived = true;
    // Gap between events, measured only while we already had one, so an idle
    // overnight stretch does not count as the player having gone quiet.
    if (state.lastGenaAt) peak('max_gena_gap_s', Math.round(agoMs(state.lastGenaAt) / 1000));
    state.lastGenaAt = Date.now();
    state.genaCount++;
    bump('events');
    if (state.correlator) state.correlator.recordGena(masterVol, muted, recvAt);
    broadcastVolume(masterVol, muted);
  }
}

// ---------------------------------------------------------------------------
// WebSocket broadcast
// ---------------------------------------------------------------------------
// Write the TV's stored volume. This is also what puts the number on screen 
// the native OSD renders in response to this call.
function pushTvVolume(vol) {
  if (vol === null || vol === undefined || isNaN(vol)) return;
  cp.exec(
    '/usr/bin/luna-send -n 1 luna://com.webos.service.audio/master/setVolume \'{"volume":' + vol + '}\' 2>/dev/null',
    function() {}
  );
}

// Read the TV's stored volume. cb(number|null), never throws.
function readTvVolume(cb) {
  cp.exec(
    '/usr/bin/luna-send -n 1 -f luna://com.webos.service.audio/master/getVolume \'{}\' 2>/dev/null',
    { timeout: 5000 },
    function(err, stdout) {
      if (err) { cb(null); return; }
      try {
        var parsed = JSON.parse(stdout);
        var v = parsed && parsed.volumeStatus ? parsed.volumeStatus.volume : null;
        cb(typeof v === 'number' ? v : null);
      } catch (e) { cb(null); }
    }
  );
}

function broadcastVolume(vol, muted) {
  reconcile(vol, muted);

  // Report the TV's own value; it is what the OSD is showing and what the Arc
  // is being driven to. Falls back to the Arc's value before the first TV read.
  var sendVol   = state.tvVol !== null ? state.tvVol
                : state.sonosVol !== null ? state.sonosVol : 0;
  var sendMuted = state.optimisticMuted;

  if (!state.wss || state.wss.clients.size === 0) return;
  var msg = JSON.stringify({ vol: sendVol, muted: sendMuted });
  state.wss.clients.forEach(function(client) {
    if (client.readyState === 1) client.send(msg);
  });
}

// ---------------------------------------------------------------------------
// GENA renewal
// ---------------------------------------------------------------------------
function scheduleRenew(device, callbackUrl, negotiatedSeconds) {
  if (state.renewTimer) clearTimeout(state.renewTimer);
  var delayMs = Math.floor(negotiatedSeconds * RENEW_FACTOR * 1000);
  console.log('[gena] renewal in', Math.round(delayMs / 1000) + 's');

  state.renewTimer = setTimeout(async function() {
    try {
      var result = await renew(device, state.sid, EVENT_PATH, REQUESTED_TIMEOUT);
      console.log('[gena] renewed | negotiated:', result.negotiatedSeconds + 's');
      scheduleRenew(device, callbackUrl, result.negotiatedSeconds);
    } catch (e) {
      console.error('[gena] renew failed:', e.message, ', re-subscribing...');
      bump('renew_failures');
      note('renew failed (' + e.message + '), re-subscribing', 'warn');
      try {
        var sub = await subscribe(device, callbackUrl, EVENT_PATH, REQUESTED_TIMEOUT);
        state.sid = sub.sid; state.seqExpected = 0;
        console.log('[gena] re-subscribed SID:', sub.sid);
        bump('resubscribes');
        if (state.stats) state.stats.setSession({ sid: sub.sid }, true);
        scheduleRenew(device, callbackUrl, sub.negotiatedSeconds);
      } catch (e2) {
        console.error('[gena] re-subscribe failed:', e2.message);
        if (state.diag) {
          state.diag.change('resub-fail', e2.message,
            'GENA re-subscribe failed: ' + e2.message);
        }
      }
    }
  }, delayMs);
}

// ---------------------------------------------------------------------------
// Periodic sync: silently re-polls Sonos every 60s so lastVol stays accurate
// after screensaver/sleep without triggering the OSD
// ---------------------------------------------------------------------------
function startPeriodicSync() {
  setInterval(function() {
    if (!state.device || !state.correlator || !state.sid) return;
    refreshTransportState();
    // Never correct while a keypress burst is still settling.
    if (!settled()) return;
    reconcileTvAndSonos();
  }, SYNC_INTERVAL_MS);
}

// ---------------------------------------------------------------------------
// Shutdown
// ---------------------------------------------------------------------------
var shuttingDown = false;
async function shutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log('\n[main] shutting down...');
  detail('shutdown requested (' + (signal || 'unknown') + ')');
  if (state.renewTimer)  clearTimeout(state.renewTimer);
  if (state.retryTimer)  clearTimeout(state.retryTimer);
  if (state.settleTimer) clearTimeout(state.settleTimer);
  if (state.inputHandle) state.inputHandle.close();

  // The subscription is the one thing that outlives this process. If we are
  // killed before this lands, the player keeps posting events to a TV that is
  // no longer there, so record how it went.
  if (state.sid) {
    try {
      var res = await genaLib.unsubscribeStatus(state.device, state.sid, EVENT_PATH);
      detail('unsubscribed on shutdown (HTTP ' + res.statusCode + ')');
      bump('clean_unsubscribes');
    } catch (e) {
      note('could not release the subscription on shutdown: ' + e.message, 'error');
    }
  }

  if (state.stats) {
    state.stats.setSession({
      cleanExit:  true,
      exitAt:     new Date().toISOString(),
      lastSeenAt: new Date().toISOString()
    }, true);
  }

  if (state.wss)       state.wss.close();
  if (state.server)    state.server.close();
  if (state.apiServer) state.apiServer.close();
  process.exit(0);
}

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------
function parseArgs() {
  var argv = process.argv.slice(2), out = {};
  for (var i = 0; i < argv.length; i++) {
    switch (argv[i]) {
      case '--callback-ip':   out.callbackIp = argv[++i]; break;
      case '--input-device':  out.inputDevice = argv[++i]; break;
      default: break;
    }
  }
  return out;
}

function detectLanIp() {
  var ifaces = os.networkInterfaces(), names = Object.keys(ifaces);
  for (var i = 0; i < names.length; i++) {
    var list = ifaces[names[i]];
    for (var j = 0; j < list.length; j++) {
      var iface = list[j];
      if (iface.family === 'IPv4' && !iface.internal) return iface.address;
    }
  }
  throw new Error('Cannot detect LAN IP');
}

function delay(ms) { return new Promise(function(r) { setTimeout(r, ms); }); }
function pad6(n)   { var s = String(n); while (s.length < 6) s = '0' + s; return s; }

// ---------------------------------------------------------------------------
// Entry point
// Reports what the platform supports. Advisory only, nothing here stops the
// service, because a partial failure (say, no overlay) still leaves volume sync
// working, and the user is better served by a running service and a clear log.
function runProbes(cb) {
  var extra = [];

  // Devices are opened speculatively, only one carries the remote's volume
  // keys, and which /dev/input/eventN that is shifts between boots. So "open"
  // is the health signal; "live" only becomes true after a real keypress.
  var ih   = state.inputHandle;
  var open = (ih && ih.devices) ? ih.devices.length : 0;
  var live = (ih && ih.live) ? ih.live().length : 0;
  extra.push({
    name:   'input',
    ok:     open > 0,
    detail: open
      ? open + ' device(s) open, ' + live + ' producing events' +
        (live ? '' : ' (normal until a volume key is pressed)')
      : 'no input devices opened. Remote volume keys will not be seen'
  });

  extra.push({
    name:   'sonos',
    ok:     !!state.device,
    detail: state.device
      ? 'connected to ' + ((state.config && state.config.sonosModel) || 'player')
      : 'no player connected'
  });

  compatLib.runProbes(extra, function (results) {
    state.probes = results;
    if (cb) { cb(results); return; }
    var failed = results.filter(function (r) { return !r.ok; });
    results.forEach(function (r) {
      state.diag.write(r.ok ? 'info' : 'warn',
        'probe ' + r.name + ': ' + (r.ok ? 'ok' : 'FAILED') + ', ' + r.detail);
    });
    if (failed.length) {
      console.warn('[compat] ' + failed.length + ' dependency check(s) failed: ' +
        failed.map(function (r) { return r.name; }).join(', ') +
        ', see the Diagnostics screen in the Setup app.');
    } else {
      console.log('[compat] all dependency checks passed.');
    }
  });
}

// ---------------------------------------------------------------------------
async function main() {
  state.diag    = new diagLib.DiagLog(DIAG_FILE,    { maxBytes: 128 * 1024 });
  state.notable = new diagLib.DiagLog(NOTABLE_FILE, { maxBytes: 32 * 1024 });
  state.stats   = new statsLib.Stats(STATS_FILE);

  // Whatever the last run left behind, before this one overwrites it.
  var prev = state.stats.rotateSession();
  state.stats.bump('sessions');

  state.platform = compatLib.readPlatform();
  state.compat   = compatLib.checkPlatform(state.platform);

  state.diag.info('--- service start, v' + pkg.version + ' ---');
  reportPreviousSession(prev);
  state.diag.info('platform: webOS ' + (state.platform.release || 'unknown') +
    ' on ' + (state.platform.model || 'unknown model') +
    ', node ' + state.platform.node);
  state.diag.write(state.compat.status === 'tested' ? 'info' : 'warn',
    'compatibility: ' + state.compat.status + ', ' + state.compat.message);

  console.log('[compat] ' + state.compat.message);
  if (state.compat.status !== 'tested') {
    console.warn('[compat] This build is not blocked from running. If something ' +
      'misbehaves, open the Setup app and save the diagnostics report.');
  }

  openPorts();

  // Start infrastructure servers first
  state.server = startListener(DEFAULT_LISTEN_PORT, onGenaNotify);
  state.wss    = new WebSocketServer({ port: DEFAULT_WS_PORT });
  state.wss.on('listening', function() { console.log('[ws]   overlay server on :' + DEFAULT_WS_PORT); });
  state.wss.on('error', function(e)   { console.error('[ws]   server error:', e.message); });
  startApiServer();

  await delay(150);

  var config = readConfig();
  if (config) {
    state.config = config;
    console.log('[main] config found, connecting to Sonos at', config.sonosIp);
    await connectToSonos(config);
  } else {
    console.log('[main] no config, waiting for setup via the Setup app.');
  }

  startPeriodicSync();
  startHeartbeat();
  runProbes(null);

  process.on('SIGINT',  function () { shutdown('SIGINT'); });
  process.on('SIGTERM', function () { shutdown('SIGTERM'); });
}

main().catch(function(err) {
  console.error('[fatal]', err.message);
  if (state.diag) state.diag.error('fatal: ' + err.message);
  process.exit(1);
});
