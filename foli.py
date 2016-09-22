import datetime
import time

import dateutil.parser
import gtfs_realtime_pb2
import json
import pytz

from flask import abort

EPOCH = datetime.datetime(1970, 1, 1, tzinfo=pytz.utc)


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

        date = vehicle['monitoredVehicleJourney']['framedVehicleJourneyRef']['dateFrameRef'].replace("-", "")
        ent.trip_update.trip.start_date = date

        start_time = vehicle['monitoredVehicleJourney']['originAimedDepartureTime']
        ent.trip_update.trip.start_time = start_time[:2] + ":" + start_time[2:] + ":00"

        direction = vehicle['monitoredVehicleJourney']['directionRef']
        ent.trip_update.trip.direction_id = int(direction) - 1

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
