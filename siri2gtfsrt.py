#!/usr/bin/python

import gtfs_realtime_pb2
from google.protobuf import text_format
from flask import Flask
from flask import request
from urllib2 import urlopen
import os
import threading
from traceback import print_exc

import hsl
import foli
import gtfs

# Tampere realtinme siri feed
JOLI_URL = os.environ.get('JOLI_URL', "http://data.itsfactory.fi/journeys/api/1/vehicle-activity")
# HSL realtime siri feed
HSL_URL = os.environ.get('HSL_URL', "http://api.digitransit.fi/realtime/navigator-server/v1/siriaccess/vm/json?operatorRef=HSL")
# HSL area train GTFS-RT feed to merge the HSL data into
TRAIN_URL = os.environ.get('TRAIN_URL', "http://api.digitransit.fi/realtime/raildigitraffic2gtfsrt/v1/hsl")
# HSL area service-alerts GTFS-RT feed
TRIP_UPDATE_URL = os.environ.get('TRIP_UPDATE_URL', "http://api.digitransit.fi/realtime/service-alerts/v1/")


#global data
ctx = {"msg" : gtfs_realtime_pb2.FeedMessage()}

class Poll(object):
    def __init__(self, url, interval, fn, preprocess = False):
        self.url = url
        self.interval = interval
        self.stopped = threading.Event()
        self.result = None
        self.fn = fn
        self.preprocess = preprocess
        thread = threading.Thread(target=self.run)
        thread.daemon = True
        thread.start()

    def run(self):
        while True:
            try:
                print "fetching data from ", self.url

                result = urlopen(self.url, timeout = 60).read()
                if self.fn is not None:
                    print "processing url", self.url
                    self.result = self.fn(result)
                else:
                    print "storing content", self.url
                    self.result = result
                if self.preprocess:
                    print "calling preprocess", self.url
                    process_hsl_data()
            except:
                print_exc()

            self.stopped.wait(self.interval)

def handle_trip_update(orig_msg, alerts):
    if alerts == None:
        return

    for entity in alerts.entity:
        if entity.HasField('trip_update'):
            new_entity = orig_msg.entity.add()
            new_entity.CopyFrom(entity)

# data is processed asynchronously as new data come in
def process_hsl_data():
    try:
        if TRAIN_poll.result is not None:
            ctx["nmsg"] = TRAIN_poll.result
        else:
           ctx["nmsg"] = gtfs_realtime_pb2.FeedMessage()
    except:
        print_exc()

    try:
        if HSL_poll.result is not None:
            ctx["nmsg"].MergeFrom(HSL_poll.result)
    except:
        print_exc()

    try:
        if TRIP_UPDATE_poll.result is not None:
            handle_trip_update(ctx["nmsg"], TRIP_UPDATE_poll.result)
    except:
        print_exc()

    ctx["msg"] = ctx["nmsg"];

HSL_poll = Poll(HSL_URL, 60, hsl.handle_siri, True)
JOLI_poll = Poll(JOLI_URL, 60, foli.handle_journeys, False)
TRAIN_poll = Poll(TRAIN_URL, 60, gtfs.parse_gtfsrt, True)
TRIP_UPDATE_poll = Poll(TRIP_UPDATE_URL, 60, gtfs.parse_gtfsrt, True)

app = Flask(__name__)

@app.route('/JOLI')
def jore_data():
    return toString(JOLI_poll.result)

@app.route('/HSL')
def hsl_data():
    return toString(ctx["msg"]);

def toString(msg):
    if 'debug' in request.args:
        return text_format.MessageToString(msg)
    else:
        return msg.SerializeToString()


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
#    app.debug = True
    app.run(host='0.0.0.0', port=port, threaded=True)
