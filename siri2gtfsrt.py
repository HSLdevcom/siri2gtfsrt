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

HSL_URL = "http://dev.hsl.fi/siriaccess/vm/json?operatorRef=HSL"

JOLI_URL = "http://data.itsfactory.fi/journeys/api/1/vehicle-activity"

EPOCH = datetime.datetime(1970, 1, 1, tzinfo=pytz.utc)


app = Flask(__name__)

@app.route('/HSL')
def hsl_data():
    return handle_siri(HSL_URL)

def handle_siri(url):
    siri_data = json.loads(urlopen(url).read().decode('utf-8'))['Siri']
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.header.gtfs_realtime_version = "1.0"
    msg.header.incrementality = msg.header.FULL_DATASET
    msg.header.timestamp = int(siri_data['ServiceDelivery']['ResponseTimestamp']) / 1000

    for i, vehicle in enumerate(siri_data['ServiceDelivery']['VehicleMonitoringDelivery'][0]['VehicleActivity']):
        route_id = vehicle['MonitoredVehicleJourney']['LineRef']['value'][:5].strip()

        if route_id in ('1300', '1300V', '1300M' ):
            continue # No other information than location for metros

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

    if 'debug' in request.args:
        return text_format.MessageToString(msg)
    else:
        return msg.SerializeToString()


@app.route('/JOLI')
def jore_data():
    return handle_journeys(JOLI_URL)

def handle_journeys(url):
    journeys_data = json.loads(urlopen(url).read().decode('utf-8'))
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
    app.debug = True
    app.run(host='0.0.0.0', port=port)
