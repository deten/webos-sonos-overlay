'use strict';

var os = require('os');

var ssdpLib  = require('./lib/ssdp');
var genaLib  = require('./lib/gena');
var soapLib  = require('./lib/soap');
var parseLib = require('./lib/parse');

var discoverSonos  = ssdpLib.discoverSonos;
var startListener  = genaLib.startListener;
var subscribe      = genaLib.subscribe;
var renew          = genaLib.renew;
var unsubscribe    = genaLib.unsubscribe;
var setVolume      = soapLib.setVolume;
var getVolume      = soapLib.getVolume;
var parseLastChange = parseLib.parseLastChange;

var EVENT_PATH        = '/MediaRenderer/RenderingControl/Event';
var REQUESTED_TIMEOUT = 1800;   // seconds we ask for; Arc may negotiate shorter
var RENEW_FACTOR      = 0.80;   // renew at 80% of the negotiated timeout

// ---------------------------------------------------------------------------
// Runtime state
// ---------------------------------------------------------------------------
var state = {
  device:      null,
  sid:         null,
  seqExpected: 0,
  renewTimer:  null,
  // Set when we fire setVolume; cleared when the confirming NOTIFY arrives.
  pendingSet:  null   // { volume: n, sentAt: hrtime }
};

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------
async function main() {
  var args = parseArgs();

  var device;
  if (args.ip) {
    device = { ip: args.ip, port: args.sonosPort || 1400 };
    console.log('[main] manual IP:', device.ip + ':' + device.port);
  } else {
    console.log('[main] discovering Sonos via SSDP...');
    device = await discoverSonos({ timeoutMs: 8000 });
    console.log('[main] found device at', device.ip + ':' + device.port, '—', device.location);
  }
  state.device = device;

  var listenerPort = args.listenerPort || 7474;
  var callbackIp   = args.callbackIp  || detectLanIp();
  var callbackUrl  = 'http://' + callbackIp + ':' + listenerPort + '/notify';

  console.log('[main] callback URL:', callbackUrl);
  console.log('[main] Windows Firewall must allow inbound TCP on port', listenerPort);

  var server = startListener(listenerPort, onNotify);

  // Small gap so the listener socket is bound before we SUBSCRIBE.
  await delay(150);

  var sub = await subscribe(device, callbackUrl, EVENT_PATH, REQUESTED_TIMEOUT);
  state.sid = sub.sid;
  console.log('[gena] subscribed SID:', sub.sid,
              '| requested:', REQUESTED_TIMEOUT + 's',
              '| negotiated:', sub.negotiatedSeconds + 's');

  scheduleRenew(device, callbackUrl, sub.negotiatedSeconds);

  var vol = await getVolume(device);
  console.log('[main] current volume:', vol);
  console.log('[main] ready — change volume via Sonos app or remote to see events.');
  console.log('[main] uncomment testSetGet() in main() for a round-trip test.');
  console.log('[main] Ctrl-C to quit.\n');

  // Uncomment to run the automatic set/get round-trip test on startup:
  // await testSetGet(device);

  process.on('SIGINT', async function() {
    console.log('\n[main] shutting down...');
    if (state.renewTimer) clearTimeout(state.renewTimer);
    if (state.sid) await unsubscribe(device, state.sid, EVENT_PATH);
    server.close();
    process.exit(0);
  });
}

// ---------------------------------------------------------------------------
// GENA event handler
// ---------------------------------------------------------------------------
function onNotify(headers, rawBody, recvAt) {
  var sid = headers['sid'] || '';
  var seq = parseInt(headers['seq'] || '0', 10);
  var nts = headers['nts'] || '';

  // Only process upnp:propchange events.
  if (nts && nts !== 'upnp:propchange') return;

  // Sequence gap detection.
  // Note: Sonos may reset SEQ to 0 after re-subscribe — treat that as a reset,
  // not a gap, to avoid false-positive warnings.
  if (sid === state.sid) {
    if (seq !== state.seqExpected && !(seq === 0 && state.seqExpected > 0)) {
      console.warn('[gena] SEQ gap: expected', state.seqExpected, 'got', seq,
                   '(dropped', seq - state.seqExpected, 'events?)');
    }
    state.seqExpected = seq + 1;
  }

  var entries;
  try {
    entries = parseLastChange(rawBody);
  } catch(e) {
    console.error('[parse] error:', e.message);
    console.error('[parse] raw body:', rawBody.slice(0, 600));
    return;
  }

  if (entries.length === 0) {
    console.log('[notify] SEQ=' + seq + ' | no parseable state variables (raw body logged below)');
    console.log('[notify] raw:', rawBody.slice(0, 400));
    return;
  }

  // Measure receive → parsed latency.
  var elapsed    = process.hrtime(recvAt);
  var dispatchUs = Math.round((elapsed[0] * 1e9 + elapsed[1]) / 1000);

  // Identify the master volume entry (Sonos uses channel "Master").
  var masterVol = null;
  entries.forEach(function(e) {
    if (e.name === 'Volume' && e.channel === 'Master') masterVol = e.val;
    // Log if a VolumeMaster state variable appears — the prompt wants to know.
    if (e.name === 'VolumeMaster') {
      console.log('[notify] VolumeMaster state variable found — val:', e.val);
    }
  });

  // Round-trip measurement: did this NOTIFY confirm a setVolume we sent?
  var roundTripMs = null;
  if (state.pendingSet !== null && masterVol !== null) {
    if (masterVol === state.pendingSet.volume) {
      var rt     = process.hrtime(state.pendingSet.sentAt);
      roundTripMs = (rt[0] * 1e9 + rt[1]) / 1e6;
      state.pendingSet = null;
    }
  }

  var chanSummary = entries.map(function(e) {
    return e.name + (e.channel ? '[' + e.channel + ']' : '') + '=' + e.val;
  }).join(' ');

  var parts = [
    'SID=…' + sid.slice(-8),
    'SEQ=' + seq,
    chanSummary,
    'recv→parsed=' + dispatchUs + 'µs'
  ];
  if (roundTripMs !== null) {
    parts.push('set→confirm=' + roundTripMs.toFixed(1) + 'ms');
  }

  console.log('[notify]', parts.join(' | '));
}

// ---------------------------------------------------------------------------
// GENA renewal
// ---------------------------------------------------------------------------
function scheduleRenew(device, callbackUrl, negotiatedSeconds) {
  if (state.renewTimer) clearTimeout(state.renewTimer);

  var delayMs = Math.floor(negotiatedSeconds * RENEW_FACTOR * 1000);
  console.log('[gena] renewal in', (delayMs / 1000).toFixed(0) + 's',
              '(80% of ' + negotiatedSeconds + 's)');

  state.renewTimer = setTimeout(async function() {
    try {
      var result = await renew(device, state.sid, EVENT_PATH, REQUESTED_TIMEOUT);
      console.log('[gena] renewed SID:', state.sid,
                  '| negotiated:', result.negotiatedSeconds + 's');
      scheduleRenew(device, callbackUrl, result.negotiatedSeconds);
    } catch(e) {
      console.error('[gena] renew failed:', e.message, '— attempting re-subscribe...');
      try {
        var sub = await subscribe(device, callbackUrl, EVENT_PATH, REQUESTED_TIMEOUT);
        state.sid         = sub.sid;
        state.seqExpected = 0;
        console.log('[gena] re-subscribed, new SID:', sub.sid);
        scheduleRenew(device, callbackUrl, sub.negotiatedSeconds);
      } catch(e2) {
        console.error('[gena] re-subscribe failed:', e2.message,
                      '— events will stop. Restart the script.');
      }
    }
  }, delayMs);
}

// ---------------------------------------------------------------------------
// Test helper — call manually or uncomment in main()
// ---------------------------------------------------------------------------
async function testSetGet(device) {
  var before = await getVolume(device);
  // Step ±2 so the change is audible but not jarring.
  var target = before < 50 ? before + 2 : before - 2;

  console.log('\n[test] getVolume before:', before, '→ setting to:', target);
  state.pendingSet = { volume: target, sentAt: process.hrtime() };
  await setVolume(device, target);
  console.log('[test] setVolume sent — waiting for GENA confirmation...');

  await delay(5000);

  var after = await getVolume(device);
  console.log('[test] getVolume after:', after);

  // Restore original volume.
  await setVolume(device, before);
  console.log('[test] restored to:', before, '\n');
}

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------
function parseArgs() {
  var argv = process.argv.slice(2);
  var out  = {};

  for (var i = 0; i < argv.length; i++) {
    switch (argv[i]) {
      case '--ip':            out.ip           = argv[++i]; break;
      case '--sonos-port':    out.sonosPort    = parseInt(argv[++i], 10); break;
      case '--listener-port': out.listenerPort = parseInt(argv[++i], 10); break;
      case '--callback-ip':   out.callbackIp   = argv[++i]; break;
      default:
        console.warn('[args] unknown arg:', argv[i]);
    }
  }

  if (process.env.SONOS_IP && !out.ip) out.ip = process.env.SONOS_IP;

  return out;
}

// Pick the first non-loopback IPv4 address. On a multi-homed machine you may
// need --callback-ip to select the interface that shares a subnet with the Sonos.
function detectLanIp() {
  var ifaces = os.networkInterfaces();
  var names  = Object.keys(ifaces);

  for (var i = 0; i < names.length; i++) {
    var list = ifaces[names[i]];
    for (var j = 0; j < list.length; j++) {
      if (list[j].family === 'IPv4' && !list[j].internal) return list[j].address;
    }
  }

  throw new Error(
    'Cannot auto-detect LAN IP. Run with --callback-ip <your-lan-ip>.'
  );
}

function delay(ms) {
  return new Promise(function(resolve) { setTimeout(resolve, ms); });
}

// ---------------------------------------------------------------------------
main().catch(function(err) {
  console.error('[fatal]', err.message);
  process.exit(1);
});
