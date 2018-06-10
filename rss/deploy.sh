#!/bin/bash

set -e
set -x

systemctl stop newsmine_rss

mkdir -p /opt/newsmine_rss

cp rss.py /opt/newsmine_rss/.
cp rss_feeds.py /opt/newsmine_rss/.
cp ../rss.txt /opt/newsmine_rss/.
cp ../feed_parser.py /opt/newsmine_rss/.
cp ../file_utils.py /opt/newsmine_rss/.
cp ../prometheus_helpers.py /opt/newsmine_rss/.
cp ../time_utils.py /opt/newsmine_rss/.

python3.5 -m virtualenv /opt/newsmine_rss/venv
source /opt/newsmine_rss/venv/bin/activate
pip3.5 install -r requirements.txt

cp newsmine_rss.service /etc/systemd/system/.
systemctl daemon-reload
systemctl enable newsmine_rss
systemctl start newsmine_rss
systemctl status newsmine_rss
