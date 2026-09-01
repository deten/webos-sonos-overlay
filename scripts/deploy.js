#!/usr/bin/env node
'use strict';
// Deployment driver. All host-specific values (TV address, SSH key path) live in
// deploy.config.json, which is git-ignored. Copy deploy.config.example.json to
// deploy.config.json and fill it in, or set TV_IP / TV_SSH_KEY in the environment.

var fs   = require('fs');
var path = require('path');
var cp   = require('child_process');

var ROOT = path.join(__dirname, '..');

function loadConfig() {
  var file = path.join(ROOT, 'deploy.config.json');
  var cfg  = {};
  if (fs.existsSync(file)) {
    try {
      cfg = JSON.parse(fs.readFileSync(file, 'utf8'));
    } catch (e) {
      fail('deploy.config.json is not valid JSON: ' + e.message);
    }
  }
  if (process.env.TV_IP)      cfg.tvIp   = process.env.TV_IP;
  if (process.env.TV_USER)    cfg.tvUser = process.env.TV_USER;
  if (process.env.TV_SSH_KEY) cfg.sshKey = process.env.TV_SSH_KEY;

  cfg.tvUser = cfg.tvUser || 'root';
  cfg.appId  = cfg.appId  || 'com.brineandbuild.sonosoverlay';

  if (!cfg.tvIp) {
    fail('No TV address configured.\n' +
         'Copy deploy.config.example.json to deploy.config.json and set "tvIp",\n' +
         'or run with TV_IP=<address>.');
  }
  return cfg;
}

function fail(msg) {
  console.error(msg);
  process.exit(1);
}

function sshArgs(cfg) {
  var args = [];
  if (cfg.sshKey) args.push('-i', expandHome(cfg.sshKey));
  return args;
}

function expandHome(p) {
  if (p.charAt(0) !== '~') return p;
  var home = process.env.HOME || process.env.USERPROFILE || '';
  return path.join(home, p.slice(1));
}

function run(cmd, args) {
  var r = cp.spawnSync(cmd, args, { stdio: 'inherit' });
  if (r.error) fail(cmd + ': ' + r.error.message);
  if (r.status !== 0) fail(cmd + ' exited with code ' + r.status);
}

function ssh(cfg, remoteCmd) {
  run('ssh', sshArgs(cfg).concat([cfg.tvUser + '@' + cfg.tvIp, remoteCmd]));
}

function scp(cfg, local, remote) {
  run('scp', sshArgs(cfg).concat([local, cfg.tvUser + '@' + cfg.tvIp + ':' + remote]));
}

function npmRun(script) {
  run(process.platform === 'win32' ? 'npm.cmd' : 'npm', ['run', script]);
}

// ── commands ──────────────────────────────────────────────────────────────
var commands = {
  'open-ports': function (cfg) {
    var rule = function (p) {
      return 'iptables -I INPUT -p tcp --dport ' + p + ' -j ACCEPT 2>/dev/null';
    };
    ssh(cfg, [rule(7474), rule(7475), rule(7476), 'true'].join('; '));
    console.log('Ports 7474-7476 opened.');
  },

  'service': function (cfg) {
    npmRun('build:tv');
    scp(cfg, path.join(ROOT, 'dist', 'tv-service.bundle.js'), '/home/root/tv-service.js');
    console.log('Service bundle copied to /home/root/tv-service.js');
  },

  'ipk': function (cfg) {
    npmRun('build-ipk');
    var ipk = cfg.appId + '.ipk';
    scp(cfg, path.join(ROOT, 'dist', ipk), '/tmp/' + ipk);
    var payload = JSON.stringify({ id: cfg.appId, ipkUrl: '/tmp/' + ipk, subscribe: true });
    ssh(cfg,
      "luna-send -n 50 luna://com.webos.appInstallService/dev/install '" + payload +
      "' > /tmp/ipk-install.txt 2>&1 & sleep 30; " +
      "grep -o '\"state\":\"[^\"]*\"' /tmp/ipk-install.txt | tail -1");
  },

  'run': function (cfg) {
    ssh(cfg, 'fuser -k 7474/tcp 2>/dev/null; sleep 1; node /home/root/tv-service.js');
  },

  'launch': function (cfg) {
    var payload = JSON.stringify({ id: cfg.appId });
    ssh(cfg,
      "luna-send -n 1 luna://com.webos.applicationManager/launch '" + payload +
      "' > /tmp/launch.txt 2>&1 & sleep 2; cat /tmp/launch.txt");
  },
};

var cmd = process.argv[2];
if (!cmd || !commands[cmd]) {
  console.error('Usage: node scripts/deploy.js <' + Object.keys(commands).join('|') + '>');
  process.exit(1);
}
commands[cmd](loadConfig());
