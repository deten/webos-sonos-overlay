#!/usr/bin/env node
'use strict';
var fs   = require('fs');
var path = require('path');
var zlib = require('zlib');
var cp   = require('child_process');

var ROOT     = path.join(__dirname, '..');
var DIST_DIR = path.join(ROOT, 'dist');
var PKG      = JSON.parse(fs.readFileSync(path.join(ROOT, 'package.json'), 'utf8'));

if (!fs.existsSync(DIST_DIR)) fs.mkdirSync(DIST_DIR);

// ── Ensure tv-service bundle is built ────────────────────────────────────
var BUNDLE = path.join(DIST_DIR, 'tv-service.bundle.js');
if (!fs.existsSync(BUNDLE) ||
    fs.statSync(path.join(ROOT, 'tv-service.js')).mtimeMs >
    fs.statSync(BUNDLE).mtimeMs) {
  console.log('Building tv-service bundle...');
  cp.execSync('npm run build:tv', { cwd: ROOT, stdio: 'inherit' });
}

// ── Minimal tar builder ───────────────────────────────────────────────────
function tarEntry(name, content, mode) {
  var isDir = !content;
  var size  = isDir ? 0 : content.length;
  var buf   = Buffer.alloc(512, 0);
  buf.write(name, 0, Math.min(name.length, 99), 'ascii');
  buf.write(padOct(mode || (isDir ? 0o755 : 0o644), 7), 100, 7, 'ascii');
  buf.write('0000000', 108, 7, 'ascii');
  buf.write('0000000', 116, 7, 'ascii');
  buf.write(padOct(size, 11), 124, 11, 'ascii');
  buf.write('00000000000', 136, 11, 'ascii');
  buf.write(isDir ? '5' : '0', 156, 1, 'ascii');
  buf.write('ustar  ', 257, 7, 'ascii');
  buf.fill(0x20, 148, 156);
  var sum = 0;
  for (var i = 0; i < 512; i++) sum += buf[i];
  buf.write(padOct(sum, 6) + '\0 ', 148, 8, 'ascii');
  var parts = [buf];
  if (!isDir && size > 0) {
    parts.push(content);
    var pad = (512 - (size % 512)) % 512;
    if (pad) parts.push(Buffer.alloc(pad, 0));
  }
  return Buffer.concat(parts);
}

function padOct(n, len) { return n.toString(8).padStart(len, '0'); }

function tarGz(entries) {
  var chunks = entries.concat([Buffer.alloc(1024, 0)]);
  return zlib.gzipSync(Buffer.concat(chunks));
}

// ── ar writer ────────────────────────────────────────────────────────────
function arHeader(name, size) {
  var h = Buffer.alloc(60, 0x20);
  h.write(name, 0, name.length, 'ascii');
  h.write('0', 16, 1, 'ascii');
  h.write('0', 28, 1, 'ascii');
  h.write('0', 34, 1, 'ascii');
  h.write('100644', 40, 6, 'ascii');
  var sz = size.toString();
  h.write(sz, 48, sz.length, 'ascii');
  h.write('\x60\x0a', 58, 2, 'binary');
  return h;
}

function buildIpk(outPath, packageId, description, dataEntries) {
  var control = [
    'Package: ' + packageId,
    'Version: ' + PKG.version,
    'Section: misc',
    'Priority: optional',
    'Architecture: all',
    'Maintainer: sjbayer',
    'Description: ' + description,
    ''
  ].join('\n');

  var controlTar = tarGz([
    tarEntry('./', null),
    tarEntry('./control', Buffer.from(control, 'utf8'))
  ]);

  var dataTar  = tarGz(dataEntries);
  var debian   = Buffer.from('2.0\n', 'ascii');
  var ipkParts = [
    Buffer.from('!<arch>\n', 'ascii'),
    arHeader('debian-binary   ', debian.length), debian,
    arHeader('control.tar.gz  ', controlTar.length), controlTar,
    (controlTar.length % 2 ? Buffer.from('\n') : Buffer.alloc(0)),
    arHeader('data.tar.gz     ', dataTar.length), dataTar,
    (dataTar.length % 2 ? Buffer.from('\n') : Buffer.alloc(0))
  ];

  fs.writeFileSync(outPath, Buffer.concat(ipkParts));
  console.log('ipk built:', outPath, '(' + Math.round(fs.statSync(outPath).size / 1024) + ' KB)');
}

// ── Single merged IPK ─────────────────────────────────────────────────────
var APP_ID     = 'com.brineandbuild.sonosoverlay';
var APP_DIR    = './usr/palm/applications/' + APP_ID + '/';
var appEntries = [tarEntry('./', null)];
['./usr/', './usr/palm/', './usr/palm/applications/'].forEach(function(d) {
  appEntries.push(tarEntry(d, null));
});
appEntries.push(tarEntry(APP_DIR, null));
['appinfo.json', 'index.html', 'icon.png'].forEach(function(f) {
  var full = path.join(ROOT, 'setup', f);
  if (!fs.existsSync(full)) return;
  var body = fs.readFileSync(full);
  if (f === 'appinfo.json') {
    var info = JSON.parse(body.toString('utf8'));
    info.version = PKG.version;
    body = Buffer.from(JSON.stringify(info, null, 2) + String.fromCharCode(10), 'utf8');
  }
  appEntries.push(tarEntry(APP_DIR + f, body));
});
appEntries.push(tarEntry(APP_DIR + 'start-service.sh', fs.readFileSync(path.join(ROOT, 'setup', 'start-service.sh')), 0o755));
appEntries.push(tarEntry(APP_DIR + 'tv-service.bundle.js', fs.readFileSync(BUNDLE)));

buildIpk(
  path.join(DIST_DIR, APP_ID + '.ipk'),
  APP_ID,
  'Sonos volume overlay for webOS LG TV',
  appEntries
);
