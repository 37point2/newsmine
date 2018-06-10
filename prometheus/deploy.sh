#!/bin/bash

cp prometheus.yml /opt/prometheus/.
cp prometheus.service /etc/systemd/system/.

systemctl daemon-reload

systemctl enable prometheus

systemctl start prometheus                                                                             
