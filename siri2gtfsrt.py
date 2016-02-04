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

HSL_URL = os.environ.get('HSL_URL', "http://dev.hsl.fi/siriaccess/vm/json?operatorRef=HSL")

JOLI_URL = os.environ.get('JOLI_URL', "http://data.itsfactory.fi/journeys/api/1/vehicle-activity")

# Another GTFS-RT feed to merge the new HSL data into
CHAIN_URL = os.environ.get('CHAIN_URL', "http://digitransit.fi/raildigitraffic2gtfsrt/hsl")
TRIP_UPDATE_URL = os.environ.get('TRIP_UPDATE_URL', "http://digitransit.fi/hsl-alert")

EPOCH = datetime.datetime(1970, 1, 1, tzinfo=pytz.utc)

class Poll(object):
    def __init__(self, url, interval):
        self.url = url
        self.interval = interval
        self.stopped = threading.Event()
        self.result = None
        thread = threading.Thread(target=self.run)
        thread.daemon = True
        thread.start()

    def run(self):
        while not self.stopped.wait(self.interval):
            try:
                self.result = urlopen(self.url).read()
            except:
                print_exc()

HSL_poll = Poll(HSL_URL, 1)
JOLI_poll = Poll(JOLI_URL, 10)
CHAIN_poll = Poll(CHAIN_URL, 60)
TRIP_UPDATE_poll = Poll(TRIP_UPDATE_URL, 60)

app = Flask(__name__)

@app.route('/HSL')
def hsl_data():
    msg = gtfs_realtime_pb2.FeedMessage()

    try:
        data1 = handle_chain(CHAIN_poll)
        msg.MergeFrom(data1)
    except:
        if CHAIN_poll.result is not None:
            print_exc()

    try:
        data2 = handle_siri(HSL_poll)
        msg.MergeFrom(data2)
    except:
        if HSL_poll.result is not None:
            print_exc()

    try:
        handle_trip_update(msg, TRIP_UPDATE_poll)
    except:
        print_exc()

    if 'debug' in request.args:
        return text_format.MessageToString(msg)
    else:
        return msg.SerializeToString()

def handle_trip_update(orig_msg, poll):
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.ParseFromString(poll.result)
    for entity in msg.entity:
        if entity.HasField('trip_update'):
            new_entity = orig_msg.entity.add()
            new_entity.CopyFrom(entity)


def handle_chain(poll):
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.ParseFromString(poll.result)
    return msg

def handle_siri(poll):
    siri_data = json.loads(poll.result.decode('utf-8'))['Siri']
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
            stoptime.arrival.delay = vehicle['MonitoredVehicleJourney']['Delay']
        else:
            ent.trip_update.delay = vehicle['MonitoredVehicleJourney']['Delay']

    return msg

@app.route('/JOLI')
def jore_data():
    return handle_journeys(JOLI_poll)

def handle_journeys(poll):
    journeys_data = json.loads(poll.result.decode('utf-8'))
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

    if 'debug' in request.args:
        return text_format.MessageToString(msg)
    else:
        return msg.SerializeToString()


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
#    app.debug = True
    app.run(host='0.0.0.0', port=port, threaded=True)
