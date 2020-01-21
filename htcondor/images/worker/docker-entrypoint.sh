#!/bin/sh

echo "CONDOR_HOST = $CONDOR_HOST" > /etc/condor/config.d/docker
echo "COLLECTOR_HOST = $CONDOR_HOST:9618" >> /etc/condor/config.d/docker
echo "CCB_ADDRESS = $CONDOR_HOST:9618" >> /etc/condor/config.d/docker
echo "START = ProminenceWantCluster =?= \"$CONDOR_CLUSTER\" && PROMINENCE_WORKER_HEALTHY =?= True" >> /etc/condor/config.d/docker
echo "PROM_CLOUD = \"$CONDOR_CLOUD\"" >> /etc/condor/config.d/docker
echo "PROM_NODES = $CONDOR_NODES" >> /etc/condor/config.d/docker
echo "PROM_CORES_TOTAL = $CONDOR_CORES" >> /etc/condor/config.d/docker
/usr/local/bin/get-location >> /etc/condor/config.d/docker

# Complete setup of CA
certhash=`openssl x509 -hash -noout -in /etc/condor/ca/root-ca.crt`
ln -s /etc/condor/ca/root-ca.crt /etc/condor/ca/${certhash}.0
ln -s /etc/condor/ca/root-ca.signing_policy /etc/condor/ca/${certhash}.signing_policy

# HTCondor execute directory
mkdir -p /home/prominence
chmod a+xrw /home/prominence
mkdir -p /home/prominence/condor
chown condor:condor /home/prominence/condor
mkdir -p /home/user
chown user /home/user

if [ -d "$PROMINENCE_MOUNTPOINT" ]; then
    chown user $PROMINENCE_MOUNTPOINT
fi

if [ -d "/home/user/$PROMINENCE_MOUNTPOINT" ]; then
    chown user /home/user/$PROMINENCE_MOUNTPOINT
    chmod a+xrw /home/user/$PROMINENCE_MOUNTPOINT
fi

# Logs
mkdir -p /var/log/condor
chown condor:condor /var/log/condor

# Write resources file
/usr/local/bin/write-resources.py

# Run HTCondor
/usr/sbin/condor_master -f
