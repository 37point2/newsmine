#!/bin/bash

run_tests=0

while getopts ":t" opt; do
	case "${opt}" in
		t)  
			run_tests=1
			;;
	esac
done

# Need AIRFLOW_HOME env var
if [ -z "$AIRFLOW_HOME" ]; then
	echo "MUST set AIRFLOW_HOME env var"
	exit 1
fi

./db_setup.py

dags_dir=$AIRFLOW_HOME/dags

if [ ! -d $dags_dir ]; then
	echo "Making $AIRFLOW_HOME/dags"
	mkdir $dags_dir
fi

echo "Copying files"
cp import_rss_dag.py $dags_dir/.
cp get_feeds_dag.py $dags_dir/.
cp rss.txt $AIRFLOW_HOME/.
cp rss.py $dags_dir/.
cp file_utils.py $dags_dir/.
cp feed_parser.py $dags_dir/.

echo "Setting up dags"
cd $dags_dir
./import_rss_dag.py
./get_feeds_dag.py

if [ $run_tests == 1 ]; then
	echo "Testing dags"
	ds=$(date +%Y-%m-%dT%H:%M:%S)
	# airflow test import_rss_dag load_data $ds
	# airflow test get_feeds_dag fetch_feed_data $ds
	airflow test get_feeds_dag parse_feed_data $ds
fi