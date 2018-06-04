#!/bin/bash

mkdir -p /var/zookeeper

cp zoo.cfg /opt/zookeeper/conf/.
cp zookeeper.service /etc/systemd/system/.

systemctl daemon-reload

systemctl enable zookeeper

systemctl start zookeeper
