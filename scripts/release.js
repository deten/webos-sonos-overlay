#!/usr/bin/env node
'use strict';
// Snapshots the current build into releases/v<version>/ so past deployments stay
// browsable as plain folders. Version comes from package.json. Old release
// folders are safe to delete at any time; nothing depends on them.

var fs     = require('fs');
var path   = require('path');
var cp     = require('child_process');
var crypto = require('crypto');

var ROOT     = path.join(__dirname, '..');
var DIST_DIR = path.join(ROOT, 'dist');
var REL_ROOT = path.join(ROOT, 'releases');

var pkg     = JSON.parse(fs.readFileSync(path.join(ROOT, 'package.json'), 'utf8'));
var version = pkg.version;
var outDir  = path.join(REL_ROOT, 'v' + version);
var force   = process.argv.indexOf('--force') !== -1;

if (fs.existsSync(outDir) && !force) {
  console.error('releases/v' + version + ' already exists.');
  console.error('Bump "version" in package.json, or re-run with --force to overwrite.');
  process.exit(1);
}

// Clear stale artifacts so the snapshot is exactly what this build produced.
if (fs.existsSync(DIST_DIR)) {
  fs.readdirSync(DIST_DIR).forEach(function (f) {
    if (/\.(ipk|js)$/.test(f)) fs.unlinkSync(path.join(DIST_DIR, f));
  });
}

cp.execSync('npm run build-ipk', { cwd: ROOT, stdio: 'inherit' });

if (!fs.existsSync(REL_ROOT)) fs.mkdirSync(REL_ROOT);
if (!fs.existsSync(outDir))   fs.mkdirSync(outDir);

function gitInfo() {
  try {
    return {
      commit: cp.execSync('git rev-parse --short HEAD', { cwd: ROOT }).toString().trim(),
      dirty:  cp.execSync('git status --porcelain', { cwd: ROOT }).toString().trim() !== '',
    };
  } catch (e) { return null; }
}

var artifacts = fs.readdirSync(DIST_DIR).filter(function (f) {
  return /\.(ipk|js)$/.test(f);
});

var files = artifacts.map(function (name) {
  var buf = fs.readFileSync(path.join(DIST_DIR, name));
  fs.writeFileSync(path.join(outDir, name), buf);
  return {
    name:   name,
    bytes:  buf.length,
    sha256: crypto.createHash('sha256').update(buf).digest('hex'),
  };
});

var manifest = {
  version: version,
  built:   new Date().toISOString(),
  git:     gitInfo(),
  files:   files,
};
fs.writeFileSync(path.join(outDir, 'manifest.json'),
  JSON.stringify(manifest, null, 2) + '\n', 'utf8');

var notes = path.join(outDir, 'NOTES.md');
if (!fs.existsSync(notes)) {
  fs.writeFileSync(notes, '# v' + version + '\n\n_What changed in this build._\n', 'utf8');
}

console.log('\nreleases/v' + version + ':');
files.forEach(function (f) {
  console.log('  ' + f.name + '  ' + Math.round(f.bytes / 1024) + ' KB');
});
if (manifest.git && manifest.git.dirty) {
  console.log('\nNote: working tree was dirty at build time.');
}
