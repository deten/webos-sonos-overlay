#!/usr/bin/env node
'use strict';
// Generates a Homebrew Channel repository under repo/.
//
// Homebrew Channel lets a user add a custom repository URL in its settings. It
// fetches <base>apps.json, and each package there carries a manifest naming the
// ipk to download and its sha256. Publishing that lets someone install this
// project entirely from the TV, with no ssh and no PC.
//
// Everything here is derived: the owner and repository come from the git remote,
// the version from package.json, and the hash and size from the built ipk. Run
// it after `npm run release`.

var fs     = require('fs');
var path   = require('path');
var cp     = require('child_process');
var crypto = require('crypto');

var ROOT     = path.join(__dirname, '..');
var REPO_DIR = path.join(ROOT, 'repo');
var pkg      = JSON.parse(fs.readFileSync(path.join(ROOT, 'package.json'), 'utf8'));

var APP_ID  = 'com.brineandbuild.sonosoverlay';
var VERSION = pkg.version;

// ── where the repository will be served from ──────────────────────────────
// Overridable so the repository can be hosted somewhere other than GitHub.
function gitSlug() {
  try {
    var url = cp.execSync('git remote get-url origin', { cwd: ROOT }).toString().trim();
    var m = url.match(/github\.com[:/]([^/]+)\/([^/.]+)/);
    if (m) return { owner: m[1], repo: m[2] };
  } catch (e) { /* fall through */ }
  return null;
}

var slug = gitSlug();
if (!slug && !process.env.REPO_BASE) {
  console.error('Could not read a GitHub remote, and REPO_BASE is not set.');
  console.error('Set REPO_BASE and IPK_BASE explicitly, or add a GitHub origin.');
  process.exit(1);
}

// Where apps.json and the icon live. raw.githubusercontent serves these fine and
// needs no GitHub Pages setup. Requires the repository to be public.
var REPO_BASE = process.env.REPO_BASE ||
  'https://raw.githubusercontent.com/' + slug.owner + '/' + slug.repo + '/main/repo/';

// Where the ipk lives. Release assets are the convention, and they keep large
// binaries out of the git history.
var IPK_BASE = process.env.IPK_BASE ||
  'https://github.com/' + slug.owner + '/' + slug.repo + '/releases/download/v' + VERSION + '/';

var SOURCE_URL = process.env.SOURCE_URL ||
  (slug ? 'https://github.com/' + slug.owner + '/' + slug.repo : '');

// ── the built package ─────────────────────────────────────────────────────
var localIpk = path.join(ROOT, 'releases', 'v' + VERSION, APP_ID + '.ipk');
if (!fs.existsSync(localIpk)) {
  localIpk = path.join(ROOT, 'dist', APP_ID + '.ipk');
}
if (!fs.existsSync(localIpk)) {
  console.error('No ipk found for v' + VERSION + '. Run `npm run release` first.');
  process.exit(1);
}

var ipkBuf  = fs.readFileSync(localIpk);
var ipkName = APP_ID + '_' + VERSION + '_all.ipk';
var sha256  = crypto.createHash('sha256').update(ipkBuf).digest('hex');

// ── manifest ──────────────────────────────────────────────────────────────
// rootRequired is true: the service is started through the Homebrew Channel
// exec endpoint and installs a hook under /var/lib/webosbrew.
//
// No "requirements" floor is declared on purpose. The service reports whether a
// release is tested and keeps running either way, so blocking the install would
// contradict that. Add one here if an untested release turns out to be harmful.
var manifest = {
  id:             APP_ID,
  version:        VERSION,
  type:           'web',
  title:          'Sonos Volume Overlay',
  appDescription: pkg.description,
  iconUri:        REPO_BASE + 'icon.png',
  sourceUrl:      SOURCE_URL,
  rootRequired:   true,
  ipkUrl:         IPK_BASE + ipkName,
  ipkHash:        { sha256: sha256 },
  ipkSize:        ipkBuf.length
};

var appsJson = {
  paging:   { page: 1, count: 1, maxPage: 1, itemsTotal: 1 },
  packages: [
    {
      id:               APP_ID,
      title:            manifest.title,
      iconUri:          manifest.iconUri,
      manifestUrl:      REPO_BASE + 'webosbrew.manifest.json',
      manifest:         manifest,
      pool:             'main',
      shortDescription: 'Keeps a Sonos soundbar in step with the TV volume and ' +
                        'shows an on-screen indicator.',
      fullDescriptionUrl: 'apps/' + APP_ID + '/full_description.html'
    }
  ]
};

// ── write ─────────────────────────────────────────────────────────────────
function mkdirp(p) { if (!fs.existsSync(p)) fs.mkdirSync(p, { recursive: true }); }

mkdirp(REPO_DIR);
mkdirp(path.join(REPO_DIR, 'apps', APP_ID));

fs.writeFileSync(path.join(REPO_DIR, 'apps.json'),
  JSON.stringify(appsJson, null, 2) + '\n', 'utf8');
fs.writeFileSync(path.join(REPO_DIR, 'webosbrew.manifest.json'),
  JSON.stringify(manifest, null, 2) + '\n', 'utf8');

var icon = path.join(ROOT, 'setup', 'icon.png');
if (fs.existsSync(icon)) {
  fs.writeFileSync(path.join(REPO_DIR, 'icon.png'), fs.readFileSync(icon));
}

fs.writeFileSync(path.join(REPO_DIR, 'apps', APP_ID, 'full_description.html'),
  '<h3>Sonos Volume Overlay</h3>\n' +
  '<p><b>This is in testing. Use at your own risk.</b> It requires a rooted TV ' +
  'and patches a system file belonging to the compositor.</p>\n' +
  '<p>When a TV is set to external ARC sound output, webOS hides its own volume ' +
  'display and hands control to HDMI-CEC. The remote still changes the volume, ' +
  'but no number appears on screen and the TV and the soundbar drift apart.</p>\n' +
  '<p>This restores the on-screen indicator and keeps the soundbar locked to the ' +
  'TV volume. After installing, open the app once to pick your Sonos player.</p>\n' +
  '<p>Tested on webOS 6.4.0. It runs on any release and tells you whether yours ' +
  'has been tested.</p>\n', 'utf8');

// A copy of the ipk under the release asset name, ready to upload.
fs.writeFileSync(path.join(ROOT, 'dist', ipkName), ipkBuf);

console.log('repo/ written for v' + VERSION);
console.log('  apps.json served from : ' + REPO_BASE + 'apps.json');
console.log('  ipk expected at       : ' + manifest.ipkUrl);
console.log('  sha256                : ' + sha256);
console.log('  upload asset          : dist/' + ipkName);
console.log('');
console.log('Add this URL in Homebrew Channel > Settings > Add repository:');
console.log('  ' + REPO_BASE);
