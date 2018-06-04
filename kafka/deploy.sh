#!/bin/bash

mkdir -p /var/kafka

cp server.properties /opt/kafka/config/.
cp kafka.service /etc/systemd/system/.

systemctl daemon-reload

systemctl enable kafka

systemctl start kafka                                                                                   
