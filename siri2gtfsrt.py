#!/usr/bin/python

import json
import gtfs_realtime_pb2
from google.protobuf import text_format
from flask import Flask
from flask import request, abort
from urllib2 import urlopen
import time
import datetime
import dateutil.parser
import pytz
import os
import threading
from traceback import print_exc

#Tampere realtinme siri feed
JOLI_URL = os.environ.get('JOLI_URL', "http://data.itsfactory.fi/journeys/api/1/vehicle-activity")
# HSL realtime siri feed
HSL_URL = os.environ.get('HSL_URL', "http://api.digitransit.fi/realtime/navigator-server/v1/siriaccess/vm/json?operatorRef=HSL")
# HSL area train GTFS-RT feed to merge the HSL data into
TRAIN_URL = os.environ.get('TRAIN_URL', "http://api.digitransit.fi/realtime/raildigitraffic2gtfsrt/v1/hsl")
# HSL area service-alerts GTFS-RT feed
TRIP_UPDATE_URL = os.environ.get('TRIP_UPDATE_URL', "http://api.digitransit.fi/realtime/service-alerts/v1/")

EPOCH = datetime.datetime(1970, 1, 1, tzinfo=pytz.utc)

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

def handle_siri(raw):
    siri_data = json.loads(raw.decode('utf-8'))['Siri']
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.header.gtfs_realtime_version = "1.0"
    msg.header.incrementality = msg.header.FULL_DATASET
    msg.header.timestamp = int(siri_data['ServiceDelivery']['ResponseTimestamp']) / 1000

    for i, vehicle in enumerate(siri_data['ServiceDelivery']['VehicleMonitoringDelivery'][0]['VehicleActivity']):
        route_id = vehicle['MonitoredVehicleJourney']['LineRef']['value'][:5].strip()

        if route_id in ('1300', '1300V', '1300M' ):
            continue # No other information than location for metros

        if route_id[0:4] in ('3001', '3002'):
            continue # Train data is better at rata.digitraffic.fi

        if route_id[0] in ('k', 'K'):
            continue # Kutsuplus

        if 'Delay' not in vehicle['MonitoredVehicleJourney']:
            continue


        ent = msg.entity.add()
        ent.id = str(i)
        ent.trip_update.timestamp = vehicle['RecordedAtTime']/1000
        ent.trip_update.trip.route_id = route_id

        try:
            int(vehicle['MonitoredVehicleJourney']['Delay'])
        except:
            print_exc()
            print vehicle, vehicle['MonitoredVehicleJourney']['Delay']

            continue

        ent.trip_update.trip.start_date = vehicle['MonitoredVehicleJourney']['FramedVehicleJourneyRef']['DataFrameRef']['value'].replace("-","")
        if 'DatedVehicleJourneyRef' in vehicle['MonitoredVehicleJourney']['FramedVehicleJourneyRef']:
            start_time = vehicle['MonitoredVehicleJourney']['FramedVehicleJourneyRef']['DatedVehicleJourneyRef']
            ent.trip_update.trip.start_time = start_time[:2]+":"+start_time[2:]+":00"

        if 'DirectionRef' in vehicle['MonitoredVehicleJourney'] and 'value' in vehicle['MonitoredVehicleJourney']['DirectionRef']:
            ent.trip_update.trip.direction_id = int(vehicle['MonitoredVehicleJourney']['DirectionRef']['value'])-1

        if 'VehicleRef' in vehicle['MonitoredVehicleJourney']:
            ent.trip_update.vehicle.label = vehicle['MonitoredVehicleJourney']['VehicleRef']['value']

        stoptime = ent.trip_update.stop_time_update.add()

        if 'MonitoredCall' in vehicle['MonitoredVehicleJourney']:
            if 'StopPointRef' in vehicle['MonitoredVehicleJourney']['MonitoredCall']:
                stoptime.stop_id = vehicle['MonitoredVehicleJourney']['MonitoredCall']['StopPointRef']
            elif 'Order' in vehicle['MonitoredVehicleJourney']['MonitoredCall']:
                stoptime.stop_sequence = vehicle['MonitoredVehicleJourney']['MonitoredCall']['Order']
            stoptime.arrival.delay = int(vehicle['MonitoredVehicleJourney']['Delay'])
        else:
            ent.trip_update.delay = int(vehicle['MonitoredVehicleJourney']['Delay'])

    return msg

def handle_trip_update(orig_msg, alerts):
    if alerts == None:
        return

    for entity in alerts.entity:
        if entity.HasField('trip_update'):
            new_entity = orig_msg.entity.add()
            new_entity.CopyFrom(entity)

def parse_gtfsrt(raw):
    if raw == None:
        return None
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.ParseFromString(raw)

    return msg

def handle_journeys(raw):
    journeys_data = json.loads(raw.decode('utf-8'))
    if journeys_data['status'] != "success":
        abort(500)
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.header.gtfs_realtime_version = "1.0"
    msg.header.incrementality = msg.header.FULL_DATASET
    msg.header.timestamp = int(time.time())

    for i, vehicle in enumerate(journeys_data['body']):
        ent = msg.entity.add()
        ent.id = str(i)

        route_id = vehicle['monitoredVehicleJourney']['lineRef']
        ent.trip_update.trip.route_id = route_id

        date = vehicle['monitoredVehicleJourney']['framedVehicleJourneyRef']['dateFrameRef'].replace("-","")
        ent.trip_update.trip.start_date = date

        start_time = vehicle['monitoredVehicleJourney']['originAimedDepartureTime']
        ent.trip_update.trip.start_time = start_time[:2]+":"+start_time[2:]+":00"

        direction = vehicle['monitoredVehicleJourney']['directionRef']
        ent.trip_update.trip.direction_id = int(direction)-1


        if 'onwardCalls' not in vehicle['monitoredVehicleJourney']:
            continue

        for call in vehicle['monitoredVehicleJourney']['onwardCalls']:
            stoptime = ent.trip_update.stop_time_update.add()
            stoptime.stop_sequence = int(call['order'])
            arrival_time = (dateutil.parser.parse(call['expectedArrivalTime']) - EPOCH).total_seconds()
            stoptime.arrival.time = int(arrival_time)
            departure_time = (dateutil.parser.parse(call['expectedDepartureTime']) - EPOCH).total_seconds()
            stoptime.departure.time = int(departure_time)
    return msg

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

HSL_poll = Poll(HSL_URL, 60, handle_siri, True)
JOLI_poll = Poll(JOLI_URL, 60, handle_journeys, False)
TRAIN_poll = Poll(TRAIN_URL, 60, parse_gtfsrt, True)
TRIP_UPDATE_poll = Poll(TRIP_UPDATE_URL, 60, parse_gtfsrt, True)

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
