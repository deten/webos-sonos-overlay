#!/bin/sh
APP=/media/developer/apps/usr/palm/applications/com.brineandbuild.sonosoverlay
iptables -I INPUT -p tcp --dport 7474 -j ACCEPT 2>/dev/null
iptables -I INPUT -p tcp --dport 7475 -j ACCEPT 2>/dev/null
iptables -I INPUT -p tcp --dport 7476 -j ACCEPT 2>/dev/null
pkill -f "tv-service.bundle.js" 2>/dev/null
pkill -f "node /home/root/tv-service" 2>/dev/null
sleep 1
nohup /usr/bin/node "$APP/tv-service.bundle.js" >> /var/log/sonos-overlay.log 2>&1 &
echo "STARTED:$!"
