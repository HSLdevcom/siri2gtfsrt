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

GTFS_URL = 'https://data.foli.fi/gtfs/gtfs.zip'
gtfs_timezone = pytz.timezone("Europe/Helsinki")

myzipfile = zipfile.ZipFile(BytesIO(urlopen(GTFS_URL).read()))

routes = {}
with myzipfile.open('routes.txt') as route_file:
    routesreader = csv.reader(codecs.iterdecode(route_file, 'utf-8'), delimiter=',', quotechar='"')
    header = None
    for parts in routesreader:
        if not header:
            header = parts
            continue

        routeinfo = dict(zip(header, parts))
        routes[routeinfo['route_short_name']] = routeinfo['route_id']


def shortname_to_routeid(shortname):
    return routes[shortname]


required_fields = ('monitored', 'recordedattime', 'originaimeddeparturetime', 'lineref', 'directionref', 'vehicleref',
                   'next_stoppointref', 'next_expectedarrivaltime', 'next_expecteddeparturetime', 'latitude',
                   'longitude', 'inpanic', 'incongestion')


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
        ent.trip_update.trip.start_time = start.strftime('%H:%M:%S')
        ent.trip_update.trip.route_id = shortname_to_routeid(vehicle['lineref'])
        # The feed has 1 and 2 as direction values.
        # FOLI uses the opposite logic from HSL or Tampere: 2 is GTFS 0 and 1 is GTFS 1
        ent.trip_update.trip.direction_id = (int(vehicle['directionref']) - 2) % 2

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
                stoptime = ent.trip_update.stop_time_update.add()
                stoptime.stop_id = onwardcall['stoppointref']
                stoptime.arrival.time = onwardcall['expectedarrivaltime']
                stoptime.departure.time = onwardcall['expecteddeparturetime']

        ent.vehicle.trip.CopyFrom(ent.trip_update.trip)
        ent.vehicle.position.latitude = vehicle['latitude']
        ent.vehicle.position.longitude = vehicle['longitude']
        # There doesn't seem to be bearing, odometer or speed information available

        # It's just a guess if these booleans actually match these levels,
        # or if they are actually ever anything else than false.
        # Could be that UNKNOWN_CONGESTION_LEVEL should be always used.
        if vehicle['inpanic']:
            ent.vehicle.congestion_level = gtfs_realtime_pb2.VehiclePosition.SEVERE_CONGESTION
        elif vehicle['incongestion']:
            ent.vehicle.congestion_level = gtfs_realtime_pb2.VehiclePosition.CONGESTION
        else:
            ent.vehicle.congestion_level = gtfs_realtime_pb2.VehiclePosition.RUNNING_SMOOTHLY

        # Seems like the vehicle is never marked as being on a stop
        # (vehicleatstop is always false)

    return msg
