#!/bin/sh
APP=/media/developer/apps/usr/palm/applications/com.brineandbuild.sonosoverlay
# Checked before inserting, so repeated starts do not pile up duplicate rules.
for P in 7474 7475 7476; do
  iptables -C INPUT -p tcp --dport $P -j ACCEPT 2>/dev/null ||
    iptables -I INPUT -p tcp --dport $P -j ACCEPT 2>/dev/null
done
pkill -f "tv-service.bundle.js" 2>/dev/null
pkill -f "node /home/root/tv-service" 2>/dev/null
sleep 1
nohup /usr/bin/node "$APP/tv-service.bundle.js" >> /var/log/sonos-overlay.log 2>&1 &
echo "STARTED:$!"
