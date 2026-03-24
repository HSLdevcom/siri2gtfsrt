from datetime import datetime
import json
import logging
from io import BytesIO
from urllib.request import urlopen
import zipfile
import pytz
import csv
import codecs
from google.transit import gtfs_realtime_pb2

gtfs_timezone = pytz.timezone("Europe/Helsinki")


required_fields = ('monitored', 'recordedattime', 'originaimeddeparturetime', '__tripref', 'vehicleref',
                   'next_stoppointref', 'next_expectedarrivaltime', 'next_expecteddeparturetime')


def handle_journeys(raw):
    data = json.loads(raw.decode('utf-8'))
    if data['status'] != "OK":
        logging.error("Foli status was not OK")
        return None
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.header.gtfs_realtime_version = "1.0"
    msg.header.incrementality = msg.header.FULL_DATASET
    msg.header.timestamp = int(data['servertime'])

    for i, vehicle in data['result']['vehicles'].items():
        if 'monitored' not in vehicle or not vehicle['monitored']:
            continue

        fields_not_found = []
        for rf in required_fields:
            if rf not in vehicle:
                if rf == 'next_expecteddeparturetime' and 'next_aimeddeparturetime' in vehicle:
                    if 'next_expectedarrivaltime' in vehicle and vehicle['next_expectedarrivaltime'] > vehicle['next_aimeddeparturetime']:
                        vehicle['next_expecteddeparturetime'] = vehicle['next_expectedarrivaltime']
                    else:
                        vehicle['next_expecteddeparturetime'] = vehicle['next_aimeddeparturetime']
                elif rf == 'next_expectedarrivaltime' and 'next_aimedarrivaltime' in vehicle:
                    if 'next_expecteddeparturetime' in vehicle and vehicle['next_expecteddeparturetime'] < vehicle['next_aimedarrivaltime']:
                        vehicle['next_expectedarrivaltime'] = vehicle['next_expecteddeparturetime']
                    else:
                        vehicle['next_expectedarrivaltime'] = vehicle['next_aimedarrivaltime']
                else:
                    fields_not_found.append(rf)

        if len(fields_not_found) > 0:
            logging.error("Fields missing from FOLI vehicle %s (%s)" % (i, ', '.join(fields_not_found)))
            continue

        ent = msg.entity.add()
        ent.id = i

        ent.trip_update.timestamp = vehicle['recordedattime']

        start = datetime.fromtimestamp(vehicle['originaimeddeparturetime'], gtfs_timezone)
        ent.trip_update.trip.start_date = start.strftime('%Y%m%d')
        ent.trip_update.trip.trip_id = vehicle['__tripref']

        # vehicleref isn't user friendly, but the same numbers seem to exist from day to day
        ent.trip_update.vehicle.id = vehicle['vehicleref']
        # Ignore delay, as we have stoptime estimates

        # Estimates for the next stops
        stoptime = ent.trip_update.stop_time_update.add()
        stoptime.stop_id = vehicle['next_stoppointref']
        stoptime.arrival.time = vehicle['next_expectedarrivaltime']
        stoptime.departure.time = vehicle['next_expecteddeparturetime']

        # Data seems to always have next stop, sometimes more in onwardcalls
        if 'onwardcalls' in vehicle:
            for onwardcall in vehicle['onwardcalls']:
                if 'stoppointref' in onwardcall and 'expecteddeparturetime' in onwardcall and 'expectedarrivaltime' in onwardcall:
                    stoptime = ent.trip_update.stop_time_update.add()
                    stoptime.stop_id = onwardcall['stoppointref']
                    stoptime.arrival.time = onwardcall['expectedarrivaltime']
                    stoptime.departure.time = onwardcall['expecteddeparturetime']

    return msg
