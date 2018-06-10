#!/bin/bash
set -e
set -x

systemctl stop newsmine_articles@{1..4}.service

mkdir -p /opt/newsmine_articles

cp articles.py /opt/newsmine_articles/.
cp article_parser.py /opt/newsmine_articles/.
cp ../feed_parser.py /opt/newsmine_articles/.
cp ../file_utils.py /opt/newsmine_articles/.
cp ../prometheus_helpers.py /opt/newsmine_articles/.
cp ../time_utils.py /opt/newsmine_articles/.

python3.5 -m virtualenv /opt/newsmine_articles/venv
source /opt/newsmine_articles/venv/bin/activate
pip3.5 install -r requirements.txt

cp newsmine_articles@.service /etc/systemd/system/.
systemctl daemon-reload
systemctl enable newsmine_articles@.service
systemctl start newsmine_articles@{1..4}.service
systemctl status newsmine_articles@{1..4}.service