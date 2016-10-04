#!/usr/bin/python

import logging

import gtfs_realtime_pb2
from google.protobuf import text_format
from flask import Flask
from flask import request
from urllib2 import urlopen
import os
import threading
from StringIO import StringIO
import gzip

import foli
import hsl
import joli
import gtfs


logging.basicConfig(level=os.environ.get('LOGLEVEL', 'INFO'))

# Tampere realtime siri feed
JOLI_URL = os.environ.get('JOLI_URL', "http://data.itsfactory.fi/journeys/api/1/vehicle-activity")
# Turku realtime siri feed
FOLI_URL = os.environ.get('FOLI_URL', "http://data.foli.fi/siri/vm")
# HSL realtime siri feed
HSL_URL = os.environ.get('HSL_URL', "http://api.digitransit.fi/realtime/navigator-server/v1/siriaccess/vm/json?operatorRef=HSL")
# HSL area train GTFS-RT feed to merge the HSL data into
TRAIN_URL = os.environ.get('TRAIN_URL', "http://api.digitransit.fi/realtime/raildigitraffic2gtfsrt/v1/hsl")
# HSL area service-alerts GTFS-RT feed
TRIP_UPDATE_URL = os.environ.get('TRIP_UPDATE_URL', "http://api.digitransit.fi/realtime/service-alerts/v1/")


# global data
hsl_msg = gtfs_realtime_pb2.FeedMessage()


class Poll(object):
    def __init__(self, url, interval, fn, preprocess=False, gzipped=False):
        self.url = url
        self.interval = interval
        self.stopped = threading.Event()
        self.result = None
        self.fn = fn
        self.preprocess = preprocess
        self.gzipped = gzipped
        thread = threading.Thread(target=self.run)
        thread.daemon = True
        thread.start()

    def run(self):
        while True:
            try:
                logging.debug("fetching data from %s", self.url)

                result = urlopen(self.url, timeout=60).read()
                if self.gzipped:
                    result = gzip.GzipFile(fileobj=StringIO(result)).read()
                if self.fn is not None:
                    logging.debug("processing url %s", self.url)
                    self.result = self.fn(result)
                else:
                    logging.debug("storing content %s", self.url)
                    self.result = result
                if self.preprocess:
                    logging.debug("calling preprocess %s", self.url)
                    process_hsl_data()
            except:
                logging.exception("fetching %s failed", self.url)

            self.stopped.wait(self.interval)


def handle_trip_update(orig_msg, alerts):
    if alerts is None:
        return

    for entity in alerts.entity:
        if entity.HasField('trip_update'):
            new_entity = orig_msg.entity.add()
            new_entity.CopyFrom(entity)


# data is processed asynchronously as new data come in
def process_hsl_data():
    nmsg = gtfs_realtime_pb2.FeedMessage()
    try:
        # WARNING Race condition:
        #         the result might be changed between the if and the next line
        if TRAIN_poll.result is not None:
            nmsg.MergeFrom(TRAIN_poll.result)
    except:
        logging.exception("processing hsl train data failed")

    try:
        # WARNING Race condition:
        #         the result might be changed between the if and the next line
        if HSL_poll.result is not None:
            nmsg.MergeFrom(HSL_poll.result)
    except:
        logging.exception("processing hsl data failed")

    try:
        # WARNING Race condition:
        #         the result might be changed between the if and the next line
        if TRIP_UPDATE_poll.result is not None:
            handle_trip_update(nmsg, TRIP_UPDATE_poll.result)
    except:
        logging.exception("processing hsl trip updates failed")

    # WARNING Race condition: The GIL in CPython guarantees this works,
    #                         but other implementations might need atomic locks
    global hsl_msg
    hsl_msg = nmsg

JOLI_poll = Poll(JOLI_URL, 60, joli.handle_journeys)
FOLI_poll = Poll(FOLI_URL, 60, foli.handle_journeys, gzipped=True)


# TODO Does every thread need to preprocess, or would one be good enough?
#      Would lessen the workload to 1/3, but increase update latency for other two
HSL_poll = Poll(HSL_URL, 60, hsl.handle_siri, preprocess=True)
TRAIN_poll = Poll(TRAIN_URL, 60, gtfs.parse_gtfsrt, preprocess=True)
TRIP_UPDATE_poll = Poll(TRIP_UPDATE_URL, 60, gtfs.parse_gtfsrt, preprocess=True)

app = Flask(__name__)


@app.route('/JOLI')
def tampere_data():
    return toString(JOLI_poll.result)


@app.route('/FOLI')
def turku_data():
    return toString(FOLI_poll.result)


@app.route('/HSL')
def hsl_data():
    return toString(hsl_msg)


def toString(msg):
    if 'debug' in request.args:
        return text_format.MessageToString(msg)
    else:
        return msg.SerializeToString()


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
#    app.debug = True
    app.run(host='0.0.0.0', port=port, threaded=True)
