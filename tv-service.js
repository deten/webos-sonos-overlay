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
var getSonosMute          = soapLib.getMute;
var getTransportState     = soapLib.getTransportState;
var getSonosSource        = soapLib.getSource;
var parseLastChange       = parseLib.parseLastChange;
var openInputDevicesMulti = inputLib.openInputDevicesMulti;
var detectInputDevice     = inputLib.detectInputDevice;
var Correlator            = corrLib.Correlator;

var EVENT_PATH        = '/MediaRenderer/RenderingControl/Event';

// What the subscription is asked to last, and how often it is renewed.
//
// Measured on an Arc Ultra: a subscription to a host that has gone away
// expires exactly on its timeout, so a shorter one is the window during which
// the player keeps posting to a TV that has been switched off. A tester's Beam
// answered as if it still held subscriptions 43 hours old, so on that firmware
// the timeout may not be honoured at all, and the renewal interval doubles as
// the check that ours is still alive: a player that restarts drops every
// subscription, and we find out at the next renewal rather than 24 minutes on.
var REQUESTED_TIMEOUT = 600;
var RENEW_MAX_MS      = 300000;

var DEFAULT_INPUT_DEV   = '/dev/input/event1';
var DEFAULT_LISTEN_PORT = 7474;
var DEFAULT_WS_PORT     = 7475;
var API_PORT            = 7476;

var APP_ID      = 'com.brineandbuild.sonosoverlay';
var CONFIG_DIR  = '/var/lib/' + APP_ID;
var CONFIG_FILE = CONFIG_DIR + '/config.json';

// Putting the number back on screen means patching the compositor's volume
// QML. The file lives on a read-only compressed filesystem, so a patched copy
// is bind mounted over it, and the compositor only reads it when it starts.
var QML_PATH    = '/usr/lib/qml/WebOSCompositor/views/volume/StarfishVolume.qml';
var PATCHED_QML = CONFIG_DIR + '/StarfishVolume.qml';

// Where the boot hook lives. It starts the service and nothing else; the QML
// work used to happen here, which restarted the compositor at a moment nobody
// chose, roughly 40s into every boot, closing whatever had been opened.
var BOOT_HOOK_DIR  = '/var/lib/webosbrew/init.d';
var BOOT_HOOK_PATH = BOOT_HOOK_DIR + '/sonos-overlay';

// How often to re-check whether the screen is free enough to restart on.
var RESTART_POLL_MS = 20000;
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

// Events arriving for a subscription this run did not create are left over
// from an earlier run the TV powered off under. They are cancelled, but only
// after this long, because the first event of our own new subscription can
// arrive before the reply that tells us its ID.
var FOREIGN_SID_GRACE_MS = 5000;

// How long after a mute press to check whether it reached the soundbar.
var MUTE_CHECK_MS = 1500;

// How often the player's source is re-read even when nothing else changed.
var SOURCE_CHECK_MS = 60000;

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

// How long an instruction stays outstanding before we stop trying. Normally it
// is cleared within a few seconds by the check that follows every write, so
// this only matters for a player that will not move at all. It has to be
// longer than the sync interval, or a retry could never happen before the
// instruction expired.
var PENDING_MAX_MS = 25000;

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

  overlayMounted:     false,
  overlayRestartTimer: null,
  overlayDeferredFor:  null,  // the app we are waiting to get out from under

  // The value the TV asked the player to reach, and when it was asked. While
  // it is outstanding we are mid-move, so the player's own reports are
  // catch-up rather than someone else's change.
  pendingTarget: null,
  pendingSince:  0,

  // Each side as of the last reconciliation, so the next one can tell which
  // of them moved rather than assuming the player should always be twice the
  // TV. Without this, anything set from the Sonos app is undone on the next
  // tick, and the player can never rest on an odd number.
  lastKnownTv:    null,
  lastKnownSonos: null,

  // What the TV and the player are doing beyond the volume number. These are
  // the facts a report most often turns on: sound leaving the soundbar for the
  // TV's own speakers, or the soundbar deciding it is playing music.
  tvOutput:      null,   // external_arc, tv_speaker, ...
  tvMuted:       null,
  sonosSource:   null,   // tv | music | none
  sourceCheckedAt: 0,
  lanIp:         null,   // the address the player is posting events to
  foreignSids:   {},     // sid -> true once a cancel has been scheduled
  startedAt:     mono(),
  connectedAt:   0,
  lastRssMb:     0,
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

// Milliseconds on a clock that only moves forward. The TV's wall clock is
// wrong at power on and steps forward, sometimes by hours, once it syncs, which
// made durations in the log nonsense ("last event 21219s ago" on a TV that had
// been up for two) and could stall the settle timer outright if it ever
// stepped back. Wall time is kept only for comparisons across reboots.
function mono() {
  var t = process.hrtime();
  return t[0] * 1000 + Math.floor(t[1] / 1e6);
}

function agoMs(t) { return t ? (mono() - t) : null; }
function agoStr(t) {
  var ms = agoMs(t);
  return ms === null ? 'never' : (ms < 10000 ? ms + 'ms' : Math.round(ms / 1000) + 's');
}

// What the last run left behind. A process killed by a power cut cannot
// record its own ending, so the heartbeat it wrote while alive is the closest
// we get to knowing when the TV went off and whether it shut down properly.
function reportPreviousSession(prev) {
  state.prevSession = prev && prev.sid ? prev : null;
  // A run that never reached the player has no startedAt, only heartbeats,
  // and still needs its ending recorded.
  if (!prev || !(prev.startedAt || prev.lastSeenAt)) return;

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
// Also the once-a-minute check on two slow failures nothing else would notice.
function startHeartbeat() {
  setInterval(function () {
    if (state.stats) {
      state.stats.setSession({ lastSeenAt: new Date().toISOString() }, true);
    }

    // Memory. The service runs for hours at a time on a TV with little to
    // spare, and a leak ends with the system killing it, which leaves no trace
    // of its own. Recorded when it grows by a meaningful step.
    var rssMb = Math.round(process.memoryUsage().rss / (1024 * 1024));
    peak('max_rss_mb', rssMb);
    if (rssMb >= state.lastRssMb + 10) {
      if (state.lastRssMb) {
        detail('memory: ' + state.lastRssMb + 'MB -> ' + rssMb + 'MB', 'info');
      }
      state.lastRssMb = rssMb;
    }

    // The TV's own address. The player posts events to the address we gave
    // it when subscribing, so a DHCP change leaves it posting into the void
    // while renewals keep succeeding and nothing looks wrong.
    if (state.lanIp) {
      var now = null;
      try { now = detectLanIp(); } catch (e) { now = null; }
      if (now && now !== state.lanIp) {
        bump('lan_ip_changes');
        note('the TV\'s network address changed, so the player is sending ' +
             'events to the old one. Resubscribing at the new address.', 'warn');
        state.lanIp = now;
        if (state.config && !state.connecting) connectToSonos(state.config);
      }
    }
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
  L.push('TV node:       ' + (p.node || 'unknown') + ' ' + process.arch);
  var up = tvUptimeSeconds();
  L.push('TV uptime:     ' + (up === null ? 'unknown' : fmtDuration(up)) +
         ', service ' + fmtDuration(Math.round((mono() - state.startedAt) / 1000)));
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
  L.push('last known:     TV ' + state.lastKnownTv + ', Sonos ' + state.lastKnownSonos +
         (state.pendingTarget === null ? ''
           : '   (moving the Sonos to ' + state.pendingTarget + ', ' +
             agoStr(state.pendingSince) + ')'));
  L.push('ceiling:        ' + state.maxVolume + ' Sonos / ' + maxTvVol() + ' TV');
  L.push('TV output:      ' + (state.tvOutput || 'unknown') +
         (state.tvOutput && state.tvOutput !== 'external_arc'
           ? '   (sound is NOT going to the soundbar)' : ''));
  L.push('TV muted:       ' + state.tvMuted);
  L.push('Sonos source:   ' + (state.sonosSource || 'unknown') +
         (state.sonosSource === 'music' ? '   (not on the TV input)' : ''));
  L.push('transport:      ' + state.transportState);
  L.push('last key:       ' + (state.lastKeyAt
    ? Math.round((mono() - state.lastKeyAt) / 1000) + 's ago' : 'none'));
  L.push('last event:     ' + (state.lastGenaAt
    ? Math.round((mono() - state.lastGenaAt) / 1000) + 's ago' : 'none this session'));
  L.push('on-screen num:  ' +
    ((state.config && state.config.showOnScreenVolume === false)
      ? 'turned off in settings'
      : (isQmlMounted() ? 'patch mounted' : 'patch not mounted') +
        (state.overlayDeferredFor
          ? ', compositor restart held while ' + state.overlayDeferredFor +
            ' is in front'
          : '')));
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
  row('crashes',         n('crashes'),      'the service died on an error');
  row('unhandled errors', n('unhandled_rejections'));
  row('connect failures', n('connect_failures'), 'attempts to reach the player');
  L.push('');
  row('stale subs found', n('stale_subs_alive'),
      'subscriptions the player still held at the next start');
  row('leftover subs',   n('foreign_subs'),
      'events arriving for a subscription this run did not create');
  row('  cancelled',     n('foreign_subs_cancelled'));
  row('sub replaced',    n('sub_replaced'),
      'another subscriber took this TV\'s address');
  row('renew failures',  n('renew_failures'),
      'the player had dropped our subscription');
  row('resub failures',  n('resub_failures'), 'event feed lost and rebuilt');
  L.push('');
  row('output changes',  n('output_changes'),
      'TV sound moved on or off the soundbar');
  row('source changes',  n('source_changes'),
      'soundbar switched between TV and music');
  row('mute mismatches', n('mute_mismatch'),
      'mute on the TV did not reach the soundbar');
  L.push('');
  row('key bursts',      n('bursts'));
  row('keys pressed',    n('keys'));
  row('corrections',     n('corrections'),  'volume writes we sent the player');
  row('correction miss', n('correction_missed'),
      'writes where the player did not end up where asked');
  row('adoptions',       n('adoptions'),    'external changes shown on the TV');
  row('  while pending', n('adoptions_while_pending'),
      'adopted while our own correction was still settling');
  row('catch-up ignored', n('catchup_ignored'),
      'player reports treated as lag, not as someone else');
  row('gave up',         n('pending_abandoned'),
      'instructions the player never reached');
  row('TV led',          n('tv_led'),       'TV changed without a remote press');
  row('player led',      n('player_led'),   'player changed, found by polling');
  row('write failures',  n('write_failures'));
  row('read failures',   n('read_failures'));
  L.push('');
  row('screen restarts', n('compositor_restarts'));
  row('  deferred',      n('restarts_deferred'),
      'held back because an app was in front');
  L.push('');
  row('events received', n('events'));
  row('event gaps',      n('event_gaps'),
      'player volume moved with no event sent');
  row('  healed',        n('gap_resubscribes'),
      'fresh subscriptions taken because of one');
  row('sequence gaps',   n('seq_gaps'),     'events the player sent that never arrived');
  row('longest quiet',   n('max_gena_gap_s') + 's',
      'between events; long is normal when nothing changes');
  row('resubscribes',    n('resubscribes'));
  row('slow responses',  n('slow_player'),  'over ' + SLOW_PLAYER_MS + 'ms');
  row('slowest response', n('max_player_ms') + 'ms');
  L.push('');
  row('input restarts',  n('input_restarts'), 'remote reader died and came back');
  row('address changes', n('lan_ip_changes'), 'the TV moved on the network');
  row('peak memory',     n('max_rss_mb') + 'MB');

  var prev = state.stats.prevSession();
  if (prev && prev.lastSeenAt) {
    L.push('');
    L.push('previous session: last heartbeat ' + prev.lastSeenAt +
           ', exit ' + (prev.cleanExit ? 'clean' : 'not recorded'));
  }
  return L;
}

// ---------------------------------------------------------------------------
// The on-screen number: patch, mount, and choosing when to restart.
// ---------------------------------------------------------------------------
function isQmlMounted() {
  try {
    return fs.readFileSync('/proc/mounts', 'utf8').indexOf('StarfishVolume.qml') !== -1;
  } catch (e) { return false; }
}

// The patch simply removes the guard that hides LG's own volume display when
// sound is leaving over ARC. Kept once built; the source file never changes
// unless the TV takes a firmware update.
function ensurePatchedCopy() {
  try {
    if (fs.existsSync(PATCHED_QML)) return true;
    var src = fs.readFileSync(QML_PATH, 'utf8');
    if (src.indexOf('external_arc') === -1) return false;
    var out = src.split('\n').filter(function (line) {
      return line.indexOf('external_arc') === -1;
    }).join('\n');
    if (!fs.existsSync(CONFIG_DIR)) fs.mkdirSync(CONFIG_DIR, { recursive: true });
    fs.writeFileSync(PATCHED_QML, out, 'utf8');
    detail('built the volume patch for this firmware');
    return true;
  } catch (e) {
    note('could not prepare the volume patch: ' + e.message, 'error');
    return false;
  }
}

// Apps a compositor restart would destroy. Home screens and inputs lose
// nothing, so those are fair game; a streaming app someone is watching is not.
function isRestartSafe(appId) {
  if (!appId) return true;                                    // nothing in front
  if (appId === 'com.webos.app.home') return true;
  if (appId === 'com.webos.app.livetv') return true;
  if (appId.indexOf('com.webos.app.hdmi') === 0) return true;
  if (appId.indexOf('com.webos.app.externalinput') === 0) return true;
  if (appId.indexOf('com.webos.app.inputcommon') === 0) return true;
  if (appId.indexOf('com.webos.app.factorywin') === 0) return true;
  return false;
}

// Fails toward "safe" on purpose. If we cannot tell what is in front, behave
// the way earlier versions always did and restart, rather than risk never
// applying the patch and silently losing the on-screen number.
function getForegroundApp(cb) {
  cp.exec('/usr/bin/luna-send -n 1 -f ' +
          'luna://com.webos.applicationManager/getForegroundAppInfo \'{}\' 2>/dev/null',
    { timeout: 4000 }, function (err, stdout) {
      if (err || !stdout) { cb(null); return; }
      try {
        var j  = JSON.parse(stdout);
        var fg = j.foregroundAppInfo;
        var id = j.appId ||
                 (Array.isArray(fg) && fg.length && fg[0].appId) ||
                 null;
        cb(id || null);
      } catch (e) { cb(null); }
    });
}

function restartCompositor(appId, reason) {
  state.overlayRestartTimer = null;
  state.overlayDeferredFor  = null;
  detail('restarting the compositor to apply the volume patch (' + reason +
         ', in front: ' + (appId || 'nothing') + ')');
  bump('compositor_restarts');
  cp.exec('systemctl restart surface-manager-daemon.service 2>/dev/null',
    function (err) {
      if (err) note('compositor restart failed: ' + err.message, 'error');
    });
}

// The mount does nothing until the compositor restarts, and the restart tears
// down whatever is on screen. At boot that is usually the home screen or an
// input and costs nothing, but on a TV that resumes an app at power on it
// closes what the viewer was watching.
//
// Waiting is off by default, because it is a real trade: the screen survives,
// but the on-screen number does not start working until the viewer next
// passes through the home screen. TVs that boot straight to an input, where
// the restart costs nothing, are better off restarting immediately.
function scheduleCompositorRestart(reason, force) {
  if (state.overlayRestartTimer) {
    clearTimeout(state.overlayRestartTimer);
    state.overlayRestartTimer = null;
  }
  if (force) { restartCompositor(null, reason); return; }

  if (!(state.config && state.config.deferScreenRestart === true)) {
    restartCompositor(null, reason);
    return;
  }

  getForegroundApp(function (appId) {
    if (isRestartSafe(appId)) { restartCompositor(appId, reason); return; }

    if (state.overlayDeferredFor !== appId) {
      state.overlayDeferredFor = appId;
      detail('holding the compositor restart while ' + appId +
             ' is in front; the on-screen number starts working once the ' +
             'screen is free');
      bump('restarts_deferred');
    }
    state.overlayRestartTimer = setTimeout(function () {
      scheduleCompositorRestart(reason, false);
    }, RESTART_POLL_MS);
  });
}

// cb(applied) where applied is true only if the patch is already live.
function applyOverlay(opts, cb) {
  opts = opts || {};
  cb   = cb || function () {};

  if (!opts.force && state.config && state.config.showOnScreenVolume === false) {
    detail('on-screen volume number is turned off in settings, ' +
           'leaving the compositor alone');
    cb(false);
    return;
  }

  if (isQmlMounted()) {
    state.overlayMounted = true;
    cb(true);
    return;
  }

  if (!ensurePatchedCopy()) {
    note('this firmware has no volume guard to patch, so the on-screen ' +
         'number cannot be restored here', 'warn');
    cb(false);
    return;
  }

  cp.exec('mount --bind "' + PATCHED_QML + '" "' + QML_PATH + '" 2>/dev/null',
    function (err) {
      if (err) {
        note('mounting the volume patch failed: ' + err.message, 'error');
        cb(false);
        return;
      }
      state.overlayMounted = true;
      detail('volume patch mounted');
      scheduleCompositorRestart(opts.reason || 'startup', !!opts.force);
      cb(false);
    });
}

// Asking the page to close itself with PalmSystem.hide() or window.close()
// does nothing on some builds, which left the Finish button stuck on
// "Finishing..." forever. The app manager will do it properly. Method names
// differ between webOS versions, so try both and ignore failures.
// luna-send exits 0 even when the call it made failed, so the reply has to be
// read rather than the exit code. On webOS 6 closeByAppId is the method that
// works; plain close answers "no app matched by pid".
function lunaCall(uri, payload, cb) {
  cp.exec('/usr/bin/luna-send -n 1 -f ' + uri + ' \'' + payload + '\' 2>/dev/null',
    { timeout: 5000 }, function (err, stdout) {
      if (err || !stdout) { cb(false); return; }
      try { cb(JSON.parse(stdout).returnValue === true); }
      catch (e) { cb(false); }
    });
}

function closeSetupApp() {
  var payload = '{"id":"' + APP_ID + '"}';
  lunaCall('luna://com.webos.applicationManager/closeByAppId', payload,
    function (ok) {
      if (ok) { detail('setup app closed'); return; }
      lunaCall('luna://com.webos.applicationManager/close', payload,
        function (ok2) {
          detail(ok2 ? 'setup app closed'
                     : 'could not close the setup app from the service',
                 ok2 ? 'info' : 'warn');
        });
    });
}

// ---------------------------------------------------------------------------
// Boot hook
// ---------------------------------------------------------------------------
// It starts the service and nothing else. Everything about the screen is now
// decided by the service, which can see what is in front of it.
function bootHookScript() {
  function open(p) {
    var rule = 'INPUT -p tcp --dport ' + p + ' -j ACCEPT';
    return 'iptables -C ' + rule + ' 2>/dev/null || iptables -I ' + rule + ' 2>/dev/null\n';
  }
  return '#!/bin/sh\n' +
    open(DEFAULT_LISTEN_PORT) + open(DEFAULT_WS_PORT) + open(API_PORT) +
    // Only wait if there was something to kill; at boot there never is, and
    // the wait was costing a second of every power-on.
    'if pkill -f \'tv-service.bundle.js\' 2>/dev/null; then sleep 1; fi\n' +
    'APP=' + APP_DIR + '\n' +
    'nohup /usr/bin/node "$APP/tv-service.bundle.js" >> /var/log/sonos-overlay.log 2>&1 &\n';
}

function writeBootHook() {
  if (!fs.existsSync(BOOT_HOOK_DIR)) fs.mkdirSync(BOOT_HOOK_DIR, { recursive: true });
  fs.writeFileSync(BOOT_HOOK_PATH, bootHookScript(), 'utf8');
  fs.chmodSync(BOOT_HOOK_PATH, 0o755);
}

// An update rewrites its own hook. Without this, someone upgrading would keep
// the old hook forever, and it is the old hook that restarts the compositor
// from the boot path.
function healBootHook() {
  try {
    if (!fs.existsSync(BOOT_HOOK_PATH)) return;   // setup owns first install
    if (fs.readFileSync(BOOT_HOOK_PATH, 'utf8') === bootHookScript()) return;
    writeBootHook();
    detail('boot hook replaced with the current version');
  } catch (e) {
    detail('could not update the boot hook: ' + e.message, 'warn');
  }
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
// Checked before inserting. Inserting unconditionally added three more
// identical rules on every start; one TV had seventeen by the evening.
function openPorts() {
  [DEFAULT_LISTEN_PORT, DEFAULT_WS_PORT, API_PORT].forEach(function(p) {
    var rule = 'INPUT -p tcp --dport ' + p + ' -j ACCEPT';
    cp.exec('iptables -C ' + rule + ' 2>/dev/null || iptables -I ' + rule + ' 2>/dev/null');
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
        // Never let a missing network take the whole service down from here.
        tvIp:         (function () {
                        try { return detectLanIp(); } catch (e) { return null; }
                      })(),
        showOnScreenVolume: !(state.config &&
                              state.config.showOnScreenVolume === false),
        deferScreenRestart: !!(state.config &&
                               state.config.deferScreenRestart === true),
        overlayApplied:     isQmlMounted(),
        overlayWaitingFor:  state.overlayDeferredFor,
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

    // Merged, not replaced, so a partial update can change one setting and
    // reconfiguring the player does not throw away the learned UUID or the
    // volume ceiling.
    if (url === '/api/config' && req.method === 'POST') {
      readBody(req, function(body) {
        var merged = {};
        var prev   = state.config || {};
        Object.keys(prev).forEach(function (k) { merged[k] = prev[k]; });
        Object.keys(body).forEach(function (k) { merged[k] = body[k]; });

        if (!merged.sonosIp) {
          res.end(JSON.stringify({ ok: false, error: 'Missing sonosIp' }));
          return;
        }

        var playerChanged = merged.sonosIp !== prev.sonosIp ||
                            merged.sonosPort !== prev.sonosPort;
        var overlayChanged = ('showOnScreenVolume' in body) &&
                             body.showOnScreenVolume !== prev.showOnScreenVolume;

        // A different speaker must not inherit the old one's identity. The
        // UUID is what rediscovery matches on after the network moves, so a
        // stale one would quietly reconnect to the speaker being replaced.
        if (playerChanged && prev.sonosIp && !('sonosUuid' in body)) {
          delete merged.sonosUuid;
          delete merged.sonosModel;
          detail('player changed, forgetting the previous player\'s identity');
        }

        saveConfig(merged);
        state.config = merged;

        if (overlayChanged) {
          if (merged.showOnScreenVolume === false) {
            // A restart still being held for a free screen would otherwise
            // fire later and blank the screen for a number that is now off.
            if (state.overlayRestartTimer) {
              clearTimeout(state.overlayRestartTimer);
              state.overlayRestartTimer = null;
            }
            state.overlayDeferredFor = null;
            // Unmounting now leaves the next compositor start clean. The
            // number keeps showing until then, which is harmless.
            cp.exec('umount "' + QML_PATH + '" 2>/dev/null', function () {});
            detail('on-screen number turned off; it stops appearing after the ' +
                   'next restart');
          } else {
            applyOverlay({ reason: 'turned on in settings' }, function () {});
          }
        }

        if (playerChanged || !state.device) {
          console.log('[api] config saved, connecting to Sonos at', merged.sonosIp);
          connectToSonos(merged).catch(function(e) {
            console.error('[api] connect error after config save:', e.message);
          });
        } else {
          console.log('[api] config saved, player unchanged');
        }
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
    // The last step of setup. Pressing Finish is an explicit instruction, so
    // this is the one path that restarts the compositor without waiting for a
    // convenient moment: the user is looking at the setup app and expects it
    // to close.
    if (url === '/api/apply-overlay' && req.method === 'POST') {
      if (isQmlMounted()) {
        state.overlayMounted = true;
        res.end(JSON.stringify({ ok: true, alreadyApplied: true }));
        closeSetupApp();
        return;
      }

      if (state.config && state.config.showOnScreenVolume === false) {
        res.end(JSON.stringify({ ok: true, alreadyApplied: true, disabled: true }));
        closeSetupApp();
        return;
      }

      // Answer before restarting; the restart kills the webview waiting on us.
      res.end(JSON.stringify({ ok: true, restarting: true }));
      setTimeout(function() {
        applyOverlay({ force: true, reason: 'setup finished' }, function() {});
      }, 600);
      return;
    }

    // Closing the app from the page does not work on every webOS build, so the
    // service does it through the app manager instead.
    if (url === '/api/close-app' && req.method === 'POST') {
      res.end(JSON.stringify({ ok: true }));
      closeSetupApp();
      return;
    }

    if (url === '/api/startup-hook' && req.method === 'POST') {
      try {
        writeBootHook();
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
    state.lanIp = callbackIp;

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
    // Both sides start known and agreed, so the first sync tick sees no
    // movement and leaves them alone.
    state.lastKnownTv     = seed;
    state.lastKnownSonos  = initVol;
    console.log('[main] seeding TV to', seed, 'from Arc', initVol, '(2:1)');
    detail('boot seed: Arc ' + initVol + ', TV set to ' + seed);
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
      detail('input: listening on ' + devPaths.length + ' device(s), ' +
             detected.candidates.filter(function (c) { return c.hasVolKeys; }).length +
             ' declaring volume keys');
      state.inputHandle = openInputDevicesMulti(devPaths, onInputEvent, function(err) {
        console.error('[input] fatal:', err.message);
        if (state.diag) state.diag.error('input reader fatal: ' + err.message);
      }, function (level, msg) {
        bump('input_restarts');
        detail(msg, level);
      });
    }

    // How long it took to become useful after power on, and how many tries.
    // A report of "I have to wait before it works" is this number.
    state.connectedAt = mono();
    var up = tvUptimeSeconds();
    (attempt > 0 ? note : detail)(
      'ready: connected to ' + (config.sonosModel || 'the player') +
      (up === null ? '' : ' ' + up + 's after power on') +
      ', ' + Math.round((mono() - state.startedAt) / 1000) +
      's after the service started' +
      (attempt > 0 ? ', after ' + (attempt + 1) + ' attempts' : ''), 'info');

    console.log('\n[main] ready, press Vol+/Vol−/Mute on the remote.\n');
  } catch (e) {
    console.error('[main] connect failed (attempt ' + (attempt + 1) + '):', e.message);
    bump('connect_failures');
    // The first failure of a streak is worth keeping; the rest are counted.
    if (attempt === 0) {
      note('could not reach the player (' + e.message + '), retrying' +
           (tvUptimeSeconds() === null ? ''
                                       : ', ' + tvUptimeSeconds() + 's after power on'),
           'warn');
    } else if (state.diag) {
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
  state.lastKeyAt = mono();
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
      startedAt: mono(),
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
  b.lastAt = mono();
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
    checkMuteReached();
    return;
  }
  scheduleSettle();
}

// Nothing here forwards mute to the soundbar; that is left to CEC. On a TV
// where CEC does not carry volume to the soundbar, it probably does not carry
// mute either, and the TV would go silent on screen while the soundbar keeps
// playing. This records whether the two agree shortly after each press.
function checkMuteReached() {
  if (state.muteCheckTimer) clearTimeout(state.muteCheckTimer);
  state.muteCheckTimer = setTimeout(function () {
    state.muteCheckTimer = null;
    if (!state.device) return;
    readTvVolume(function () {
      getSonosMute(state.device).then(function (arcMuted) {
        var tvMuted = state.tvMuted;
        if (tvMuted === null) {
          detail('mute pressed: Arc muted=' + arcMuted + ', TV mute state unknown');
          return;
        }
        if (tvMuted === arcMuted) {
          detail('mute pressed: TV and Arc agree, muted=' + arcMuted);
          return;
        }
        bump('mute_mismatch');
        note('MUTE: pressed mute, TV muted=' + tvMuted + ' but Arc muted=' +
             arcMuted + ' ' + Math.round(MUTE_CHECK_MS / 1000 * 10) / 10 +
             's later. Mute is not reaching the soundbar over HDMI.');
      }).catch(function (e) {
        detail('mute pressed, could not read the Arc mute state: ' + e.message, 'warn');
      });
    });
  }, MUTE_CHECK_MS);
}

function settled() {
  return !state.lastKeyAt || (mono() - state.lastKeyAt) > SETTLE_MS;
}

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
    var changed = ts && ts !== state.transportState;
    if (changed) {
      console.log('[arc] transport state:', state.transportState, '->', ts);
      // Worth recording: a report of the app showing the wrong thing is much
      // easier to read against what the player was actually saying at the time.
      detail('transport: ' + state.transportState + ' -> ' + ts);
    }
    if (ts) state.transportState = ts;

    // The source is re-read when playback changes, and otherwise once a minute.
    if (changed || agoMs(state.sourceCheckedAt) > SOURCE_CHECK_MS) {
      refreshSource();
    }
  }).catch(function() {});
}

// Whether the soundbar thinks it is carrying TV audio or playing music. When
// the app shows music while the TV is playing, this is what the player itself
// was saying at the time, which separates a Sonos display quirk from the
// soundbar genuinely having switched away from the TV.
function refreshSource() {
  if (!state.device) return;
  state.sourceCheckedAt = mono();
  getSonosSource(state.device).then(function (src) {
    if (!src || src === state.sonosSource) return;
    var prev = state.sonosSource;
    state.sonosSource = src;
    if (prev === null) { detail('Arc source: ' + src); return; }
    bump('source_changes');
    (src === 'tv' || prev === 'tv' ? note : detail)(
      'Arc source changed: ' + prev + ' -> ' + src +
      ' | TV output ' + (state.tvOutput || 'unknown') +
      ', transport ' + (state.transportState || 'unknown'), 'info');
  }).catch(function () {});
}

// After the remote goes quiet, take the Sonos at its word. This is the only
// path that is guaranteed to run; GENA may have delivered its last event
// mid-burst, and during silent playback it does not emit at all.
function scheduleSettle() {
  if (state.settleTimer) clearTimeout(state.settleTimer);
  state.settleTimer = setTimeout(function() {
    state.settleTimer = null;
    if (!settled()) { scheduleSettle(); return; }
    reconcileTvAndSonos('settle');
  }, SETTLE_POLL_MS);
}

// The TV leads when the remote is used, and only then.
//
// reason 'settle' means a keypress just finished, which is an instruction: the
// player is driven to twice the TV's value. reason 'sync' is the periodic tick,
// which reads both sides for the record and finishes an instruction that has
// not landed yet, but never invents one of its own. That distinction is the
// whole point: the old version reconciled unconditionally every 10 seconds, so
// any volume set from the Sonos app was undone within seconds, and because the
// player was forced onto twice the TV's value it could never rest on an odd
// number at all.
function reconcileTvAndSonos(reason) {
  reason = reason || 'settle';
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

    var askedAt = mono();
    getVolume(state.device).then(function(sonosVol) {
      recordPlayerLatency(mono() - askedAt);
      checkForMissedEvent(sonosVol);

      // Which side moved since we last looked. This is the whole arbitration:
      // the old code compared the player against twice the TV's value, so it
      // re-asserted that relationship forever and undid anything done from the
      // Sonos app within ten seconds. Comparing against what each side was
      // last seen at instead means a change is followed once, by whichever
      // side made it, and then left alone.
      var tvMoved    = state.lastKnownTv    !== null && tvVol    !== state.lastKnownTv;
      var sonosMoved = state.lastKnownSonos !== null && sonosVol !== state.lastKnownSonos;
      state.sonosVol = sonosVol;

      if (reason === 'settle') {
        // The remote was used. That is an instruction, and the TV leads.
        state.pendingTarget = clampVol(tvToSonos(state.tvVol));
        state.pendingSince  = mono();
      } else if (state.pendingTarget !== null &&
                 state.pendingTarget === sonosVol) {
        // Reached. Checked before expiry, because the sync interval is longer
        // than the expiry window, so an instruction that succeeded would
        // otherwise be reported as having been given up on.
        state.pendingTarget  = null;
        state.lastKnownTv    = state.tvVol;
        state.lastKnownSonos = sonosVol;
        closeBurst(null);
        return;
      } else if (state.pendingTarget !== null &&
                 agoMs(state.pendingSince) > PENDING_MAX_MS) {
        note('gave up trying to move the player to ' + state.pendingTarget +
             ', it is on ' + sonosVol + ' after ' +
             Math.round(agoMs(state.pendingSince) / 1000) + 's', 'warn');
        bump('pending_abandoned');
        state.pendingTarget = null;
        state.lastKnownTv    = state.tvVol;
        state.lastKnownSonos = sonosVol;
        return;
      } else if (state.pendingTarget === null && tvMoved) {
        // The TV moved without a keypress we saw: voice control, the LG app,
        // or a CEC command from something else. The TV still leads, so the
        // player follows, exactly as it did before this arbitration existed.
        detail('TV led: ' + state.lastKnownTv + ' -> ' + tvVol +
               ' with no keypress, moving the player to match');
        bump('tv_led');
        state.pendingTarget = clampVol(tvToSonos(state.tvVol));
        state.pendingSince  = mono();
      } else if (state.pendingTarget === null && sonosMoved) {
        // The player moved and the TV did not: someone used the Sonos app, or
        // another device in the household. Show it on the TV and leave the
        // player where it was put, odd number and all.
        var shown = Math.min(sonosToTv(sonosVol), maxTvVol());
        detail('player led: Arc ' + state.lastKnownSonos + ' -> ' + sonosVol +
               ', showing ' + shown + ' on the TV');
        bump('player_led');
        if (shown !== tvVol) { state.tvVol = shown; pushTvVolume(shown); }
        state.lastKnownTv    = state.tvVol;
        state.lastKnownSonos = sonosVol;
        closeBurst(null);
        return;
      } else if (state.pendingTarget === null) {
        // Nothing moved on either side. Nothing to do, which is the case the
        // old version got wrong by writing anyway.
        closeBurst(null);
        return;
      }

      var target = state.pendingTarget;
      if (target === sonosVol) {
        // Either CEC already carried it there or our write landed.
        state.pendingTarget  = null;
        state.lastKnownTv    = state.tvVol;
        state.lastKnownSonos = sonosVol;
        closeBurst(null);
        return;
      }

      // Never jump the Arc up by a lot in one correction. Going down is always
      // safe so it is unrestricted. The rest of the gap is closed by a
      // follow-up rather than by the next sync tick, which could otherwise
      // arrive after the instruction has already expired.
      var limited = false;
      if (target > sonosVol + MAX_RAISE_PER_CORRECTION) {
        target  = sonosVol + MAX_RAISE_PER_CORRECTION;
        limited = true;
        console.warn('[cap] limiting raise to', target, '- TV asked for', state.tvVol);
        note('raise limited to ' + target + ' on the way to ' +
             state.pendingTarget, 'warn');
      }
      if (limited) {
        setTimeout(function () { reconcileTvAndSonos('sync'); }, 1500);
      }

      console.log('[settle] TV', state.tvVol, '(wants Arc ' + tvToSonos(state.tvVol) + ')',
        '| Arc', sonosVol, '-> setting Arc to', target);
      state.pendingSonosWrite = target;
      state.lastWriteAt  = mono();
      state.lastWriteVal = target;
      // Recorded as intended, so the echo does not read as the player moving
      // by itself on the next tick. A failed write leaves the instruction
      // outstanding, and the pending branch keeps retrying it.
      state.lastKnownTv    = state.tvVol;
      state.lastKnownSonos = target;
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
      recordPlayerLatency(mono() - askedAt);
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

  // A gap is proof the subscription is not delivering. On the development TV
  // this happened 31 times in a week of ordinary use, with the feed dead for
  // hours at a time and nobody noticing, because polling kept the volume
  // right. Taking a fresh subscription costs one request and replaces the
  // silent one, so do it rather than wait for the next renewal to fail.
  var heal = state.sid && state.device && state.callbackUrl &&
             (!state.lastGapHealAt || agoMs(state.lastGapHealAt) > 60000);
  note('EVENT GAP: player volume moved ' + believed + ' -> ' + polledVol +
       ' and no event was sent (last event ' + agoStr(state.lastGenaAt) +
       ', our last write ' + agoStr(state.lastWriteAt) + ')' +
       (heal ? '. Taking a fresh subscription.' : ''));
  if (heal) {
    state.lastGapHealAt = mono();
    bump('gap_resubscribes');
    resubscribeNow('events stopped arriving');
  }
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
      if (v === target) {
        // Landed. Clearing the instruction here rather than waiting for the
        // next sync tick is what keeps the window where the player's own
        // reports are treated as catch-up down to a few seconds.
        if (state.pendingTarget === target) {
          state.pendingTarget  = null;
          state.lastKnownTv    = state.tvVol;
          state.lastKnownSonos = v;
        }
        return;
      }
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
  //
  // Judged by whether the player actually moved since we last recorded it,
  // not by whether it equals twice the TV. With the player resting on an odd
  // value, the old comparison fired on every event that carried the volume,
  // re-adopting the same number and redrawing the TV's volume display each time.
  var moved = state.lastKnownSonos === null
    ? genaVol !== tvToSonos(state.tvVol)
    : genaVol !== state.lastKnownSonos;

  if (state.tvVol !== null && moved) {
    // An instruction of ours is still outstanding, so this is the player
    // catching up, not a person. Adopting here is what dragged the TV down to
    // half a lagging player's value part way through a held button.
    if (state.pendingTarget !== null) {
      bump('catchup_ignored');
      detail('catch-up: Arc ' + prev + ' -> ' + genaVol +
             ' while on the way to ' + state.pendingTarget + ', not adopting');
      return;
    }

    var adopted = Math.min(sonosToTv(genaVol), maxTvVol());
    console.log('[extern] Arc moved', prev, '->', genaVol,
      'with no keypress, showing on the TV as', adopted);

    var sinceWrite    = agoMs(state.lastWriteAt);
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

    // The TV number becomes a display of what the player is doing. It is
    // deliberately not followed by writing twice this value back to the
    // player: that is what turned a nudge to 9 into 10, and undid a nudge
    // down to 9 completely. The player keeps what it was given, odd or not.
    // Skipped when the TV already shows it, since every write draws the OSD.
    if (adopted !== state.tvVol) pushTvVolume(adopted);
    state.tvVol = adopted;

    // Both sides recorded as intended, so the sync tick that follows does not
    // see our own display update as the TV moving and push the player back.
    state.lastKnownTv    = adopted;
    state.lastKnownSonos = genaVol;
  }
}

// ---------------------------------------------------------------------------
// GENA NOTIFY handler
// ---------------------------------------------------------------------------
function onGenaNotify(headers, rawBody, recvAt, fromIp) {
  var sid = headers['sid'] || '';
  var seq = parseInt(headers['seq'] || '0', 10);

  // A subscription this run did not create. Every earlier run that the TV
  // powered off under left one of these on the player, pointing at this same
  // address, and the player keeps posting to all of them. Their data is not
  // ours to act on (it may even be a different speaker, from before a
  // reconfigure), so it is recorded and cancelled rather than processed.
  if (sid && sid !== state.sid) {
    handleForeignSid(sid, seq, fromIp);
    return;
  }

  if (sid === state.sid) {
    if (seq !== state.seqExpected && !(seq === 0 && state.seqExpected > 0)) {
      console.warn('[gena] SEQ gap: expected', state.seqExpected, 'got', seq);
      // A gap means the player sent events we never received. Rare on a LAN,
      // so worth knowing about when it happens.
      bump('seq_gaps');
      detail('event sequence gap: expected ' + state.seqExpected + ', got ' +
             seq + ', so ' + Math.max(0, seq - state.seqExpected) +
             ' event(s) from the player never arrived', 'warn');
    }
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
    state.lastGenaAt = mono();
    state.genaCount++;
    bump('events');
    if (state.correlator) state.correlator.recordGena(masterVol, muted, recvAt);
    broadcastVolume(masterVol, muted);
  }
}

// Schedules the cancel of a subscription we did not create, once per SID.
//
// The wait matters: the first event of a subscription we have just taken can
// arrive before the reply carrying its ID has been processed, and cancelling
// that would silently cut our own event feed. So the decision is re-checked
// after a grace period, by which time our own ID is always known.
//
// What it means depends on the player, and the two cases need opposite
// responses:
//
//   - Our own subscription is still alive. Then this one is a leftover the
//     player is keeping alongside it, which is what a player that never
//     expires old subscriptions would do. Cancel it.
//   - Ours is gone. An Arc Ultra keeps one subscription per callback address,
//     so anything subscribing with our address silently replaces ours, and
//     this is the replacement. Cancelling it would leave us with nothing, so
//     take the address back instead.
//
// Renewing ours answers which case it is, and costs one request.
function handleForeignSid(sid, seq, fromIp) {
  if (state.foreignSids[sid]) return;
  state.foreignSids[sid] = true;

  setTimeout(function () {
    if (sid === state.sid) {
      // It was ours after all, just faster than the subscribe reply.
      delete state.foreignSids[sid];
      return;
    }
    bump('foreign_subs');

    var otherPlayer = fromIp && state.device && fromIp !== state.device.ip;
    var host = fromIp ? { ip: fromIp, port: 1400 } : state.device;
    if (!host) return;

    // From a player we are no longer configured for, typically left over from
    // before the speaker was changed in setup. Nothing of ours is on that
    // player, so it can simply be cancelled.
    if (otherPlayer || !state.sid) {
      cancelForeign(host, sid, seq,
        otherPlayer ? 'from a different player than the configured one'
                    : 'while this run held no subscription');
      return;
    }

    renew(state.device, state.sid, EVENT_PATH, REQUESTED_TIMEOUT).then(function () {
      cancelForeign(host, sid, seq,
        'alongside our own, which is still alive, so the player is keeping ' +
        'old subscriptions rather than replacing them');
    }).catch(function (e) {
      bump('sub_replaced');
      note('SUB REPLACED: our subscription is gone (' + e.message + ') and ' +
           'another one is using this TV\'s address (event #' + seq + '). ' +
           'Something else subscribed with our address. Taking it back.', 'warn');
      resubscribeNow('our subscription was replaced');
    });
  }, FOREIGN_SID_GRACE_MS);
}

function cancelForeign(host, sid, seq, why) {
  genaLib.unsubscribeStatus(host, sid, EVENT_PATH).then(function (res) {
    if (res.statusCode === 200) bump('foreign_subs_cancelled');
    note('LEFTOVER SUB: events arriving for a subscription this run did not ' +
         'create, ' + why + ' (event #' + seq + '). Cancelled it (HTTP ' +
         res.statusCode + ').');
  }).catch(function (e) {
    note('LEFTOVER SUB: found one but could not cancel it: ' + e.message, 'warn');
    delete state.foreignSids[sid];   // try again if it keeps arriving
  });
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
// The same reply carries the sound output and mute state, which are recorded
// on the way past because they cost nothing extra to read.
function readTvVolume(cb) {
  cp.exec(
    '/usr/bin/luna-send -n 1 -f luna://com.webos.service.audio/master/getVolume \'{}\' 2>/dev/null',
    { timeout: 5000 },
    function(err, stdout) {
      if (err) { cb(null); return; }
      try {
        var parsed = JSON.parse(stdout);
        var vs = (parsed && parsed.volumeStatus) || {};
        noteTvOutput(vs.soundOutput);
        if (typeof vs.muteStatus === 'boolean') state.tvMuted = vs.muteStatus;
        cb(typeof vs.volume === 'number' ? vs.volume : null);
      } catch (e) { cb(null); }
    }
  );
}

// The TV quietly moving its sound off the soundbar is the most likely single
// explanation for "the Sonos stopped responding", and nothing else records it.
function noteTvOutput(out) {
  if (!out || out === state.tvOutput) return;
  var prev = state.tvOutput;
  state.tvOutput = out;
  if (prev === null) {
    detail('TV sound output: ' + out +
           (out === 'external_arc' ? '' : ', not the soundbar'));
    if (out !== 'external_arc') bump('output_not_arc_at_start');
    return;
  }
  bump('output_changes');
  note('TV sound output changed: ' + prev + ' -> ' + out +
       (out === 'external_arc'
         ? ', back on the soundbar'
         : ', sound is no longer going to the soundbar') +
       ' | Arc source ' + (state.sonosSource || 'unknown') +
       ', transport ' + (state.transportState || 'unknown'));
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
  state.callbackUrl = callbackUrl;
  var delayMs = Math.min(Math.floor(negotiatedSeconds * 1000 / 2), RENEW_MAX_MS);
  console.log('[gena] renewal in', Math.round(delayMs / 1000) + 's');

  state.renewTimer = setTimeout(async function() {
    try {
      var result = await renew(device, state.sid, EVENT_PATH, REQUESTED_TIMEOUT);
      console.log('[gena] renewed | negotiated:', result.negotiatedSeconds + 's');
      scheduleRenew(device, callbackUrl, result.negotiatedSeconds);
    } catch (e) {
      console.error('[gena] renew failed:', e.message, ', re-subscribing...');
      bump('renew_failures');
      // A 412 here means the player no longer knows our subscription at all.
      // Usually the player restarted, which drops every subscription it held.
      note('the player no longer had our subscription (' + e.message + '). ' +
           'It probably restarted. Taking a new one.', 'warn');
      resubscribeNow('renewal failed');
    }
  }, delayMs);
}

// Take a fresh subscription on the configured player, replacing ours.
// Because the player keeps only one subscription per callback address, this
// also displaces anything else that had taken our address.
async function resubscribeNow(reason) {
  if (!state.device || !state.callbackUrl) return;
  try {
    var sub = await subscribe(state.device, state.callbackUrl, EVENT_PATH, REQUESTED_TIMEOUT);
    state.sid = sub.sid; state.seqExpected = 0;
    console.log('[gena] re-subscribed SID:', sub.sid);
    bump('resubscribes');
    detail('re-subscribed (' + reason + '), timeout ' + sub.negotiatedSeconds + 's');
    if (state.stats) state.stats.setSession({ sid: sub.sid }, true);
    scheduleRenew(state.device, state.callbackUrl, sub.negotiatedSeconds);
  } catch (e2) {
    console.error('[gena] re-subscribe failed:', e2.message);
    // This used to be the end of the road: no renewal was scheduled, so
    // events stopped for the rest of the session while the report still said
    // subscribed. Most often it means the player moved address, so start
    // over from discovery rather than retrying a dead one.
    bump('resub_failures');
    note('lost the event subscription and could not take a new one (' +
         e2.message + '). Reconnecting from scratch in 30s.', 'error');
    state.sid = null;
    if (state.retryTimer) clearTimeout(state.retryTimer);
    state.retryTimer = setTimeout(function () {
      if (state.config) connectToSonos(state.config);
    }, 30000);
  }
}

// ---------------------------------------------------------------------------
// Periodic sync: silently re-polls Sonos every 60s so lastVol stays accurate
// after screensaver/sleep without triggering the OSD
// ---------------------------------------------------------------------------
function startPeriodicSync() {
  setInterval(function() {
    // Deliberately not gated on the event subscription. Volume sync is plain
    // request and reply and works without one; tying it to the subscription
    // meant losing events silently took volume sync down with it.
    if (!state.device) return;
    refreshTransportState();
    // Never correct while a keypress burst is still settling.
    if (!settled()) return;
    reconcileTvAndSonos('sync');
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

function fmtDuration(s) {
  if (s === null || s === undefined) return 'unknown';
  if (s < 120) return s + 's';
  if (s < 7200) return Math.round(s / 60) + 'm';
  return Math.floor(s / 3600) + 'h ' + Math.round((s % 3600) / 60) + 'm';
}

function tvUptimeSeconds() {
  try {
    return Math.round(parseFloat(fs.readFileSync('/proc/uptime', 'utf8').split(' ')[0]));
  } catch (e) { return null; }
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
// Crashes. The boot hook starts the service once per power on, so a crash
// means no volume sync until the next one, and without this nothing about it
// would survive: the runtime log is on a ramfs and the process is gone.
function installCrashHandlers() {
  process.on('uncaughtException', function (err) {
    bump('crashes');
    note('CRASH: ' + ((err && err.stack) || String(err)).split('\n').slice(0, 6).join(' | '),
         'error');
    if (state.stats) state.stats.flush(true);
    process.exit(1);
  });
  process.on('unhandledRejection', function (reason) {
    // Not fatal, but each one is a code path that failed without handling it.
    bump('unhandled_rejections');
    if (state.diag) {
      state.diag.change('rejection', String(reason && reason.message),
        'unhandled rejection: ' +
        ((reason && reason.stack) || String(reason)).split('\n').slice(0, 4).join(' | '));
    }
  });
}

// A second copy started while one is already running cannot bind the ports.
// It used to crash on that; now it says so and steps aside cleanly.
function exitIfPortTaken(server, name) {
  server.on('error', function (e) {
    if (e && e.code === 'EADDRINUSE') {
      note('another copy of the service already holds the ' + name +
           ' port, so this one is exiting and leaving it running', 'info');
      process.exit(0);
    }
    note(name + ' server error: ' + (e && e.message), 'error');
  });
}

async function main() {
  state.diag    = new diagLib.DiagLog(DIAG_FILE,    { maxBytes: 128 * 1024 });
  state.notable = new diagLib.DiagLog(NOTABLE_FILE, { maxBytes: 32 * 1024 });
  state.stats   = new statsLib.Stats(STATS_FILE);
  installCrashHandlers();

  // Registered first, not last. They used to be installed only after the
  // connect finished, which can take a minute of retries, and a stop signal
  // arriving before then skipped the unsubscribe and left one more stale
  // subscription on the player.
  process.on('SIGINT',  function () { shutdown('SIGINT'); });
  process.on('SIGTERM', function () { shutdown('SIGTERM'); });

  state.platform = compatLib.readPlatform();
  state.compat   = compatLib.checkPlatform(state.platform);

  state.diag.info('--- service start, v' + pkg.version + ' ---');
  state.diag.info('platform: webOS ' + (state.platform.release || 'unknown') +
    ' on ' + (state.platform.model || 'unknown model') +
    ', node ' + state.platform.node + ' ' + process.arch +
    ', started ' + (tvUptimeSeconds() === null ? '?' : tvUptimeSeconds()) +
    's after power on');
  // The keypress reader assumes a 16 byte input event, which is only true of
  // 32-bit userspace. A 64-bit TV would read garbage and see no keys at all.
  if (process.arch === 'arm64' || process.arch === 'x64') {
    note('this TV runs a 64-bit node (' + process.arch + '). The remote reader ' +
         'expects 32-bit input events and will probably see no key presses.', 'warn');
  }
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
  exitIfPortTaken(state.server, 'event');
  state.wss    = new WebSocketServer({ port: DEFAULT_WS_PORT });
  state.wss.on('listening', function() { console.log('[ws]   overlay server on :' + DEFAULT_WS_PORT); });
  state.wss.on('error', function(e)   {
    console.error('[ws]   server error:', e.message);
    detail('overlay socket server error: ' + e.message, 'warn');
  });
  startApiServer();
  exitIfPortTaken(state.apiServer, 'setup');

  // Only once the ports are ours. A duplicate copy exits above, and must do
  // so without touching the session record: rotating it would throw away the
  // running copy's subscription ID, which the next boot needs to cancel it.
  await new Promise(function (resolve) {
    if (state.apiServer.listening) { resolve(); return; }
    state.apiServer.once('listening', resolve);
    setTimeout(resolve, 3000);
  });

  // Whatever the last run left behind, before this one overwrites it.
  var prev = state.stats.rotateSession();
  state.stats.bump('sessions');
  reportPreviousSession(prev);

  await delay(150);

  var config = readConfig();
  if (config) state.config = config;

  // Bring an older install's boot hook up to date, then take care of the
  // screen ourselves rather than leaving it to the hook.
  healBootHook();
  applyOverlay({ reason: 'startup' }, function (already) {
    detail(already ? 'on-screen number was already active at startup'
                   : 'on-screen number is being set up');
  });

  if (config) {
    console.log('[main] config found, connecting to Sonos at', config.sonosIp);
    await connectToSonos(config);
  } else {
    console.log('[main] no config, waiting for setup via the Setup app.');
  }

  startPeriodicSync();
  startHeartbeat();
  runProbes(null);
}

main().catch(function(err) {
  console.error('[fatal]', err.message);
  bump('crashes');
  note('fatal during startup: ' +
       ((err && err.stack) || String(err)).split('\n').slice(0, 6).join(' | '), 'error');
  if (state.stats) state.stats.flush(true);
  process.exit(1);
});
