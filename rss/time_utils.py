#!/usr/bin/env python3.5

from datetime import datetime, timedelta

def ts_to_hour(ts):
    if '.' in ts:
        #
        # Hack off the milliseconds
        #
        ts = ts.split('.')[0]
    datestamp = datetime.strptime(ts, '%Y-%m-%dT%H:%M:%S')
    return datetime.strftime(datestamp, '%Y-%m-%dT%H')

def ts_to_month(ts):
    if '.' in ts:
        #
        # Hack off the milliseconds
        #
        ts = ts.split('.')[0]
    datestamp = datetime.strptime(ts, '%Y-%m-%dT%H:%M:%S')
    return datetime.strftime(datestamp, '%Y-%m')
