from datetime import datetime
import json
import logging
from StringIO import StringIO
from urllib2 import urlopen
import zipfile
import pytz

from gtfs_realtime_pb2 import FeedMessage, VehiclePosition

GTFS_URL = 'http://dev.hsl.fi/gtfs.foli/foli.zip'
gtfs_timezone = pytz.timezone("Europe/Helsinki")

zipdata = StringIO()
zipdata.write(urlopen(GTFS_URL).read())
myzipfile = zipfile.ZipFile(zipdata)

routes = {}
with myzipfile.open('routes.txt') as route_file:
    for line in route_file:
        parts = line.split(',')
        routes[parts[2]] = parts[0]


def shortname_to_routeid(shortname):
    return routes[shortname]


def handle_journeys(raw):
    data = json.loads(raw.decode('utf-8'))
    if data['status'] != "OK":
        logging.error("Foli status was not OK")
        return None
    msg = FeedMessage()
    msg.header.gtfs_realtime_version = "1.0"
    msg.header.incrementality = msg.header.FULL_DATASET
    msg.header.timestamp = int(data['servertime'])

    for i, vehicle in data['result']['vehicles'].iteritems():
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
            ent.vehicle.congestion_level = VehiclePosition.SEVERE_CONGESTION
        elif vehicle['incongestion']:
            ent.vehicle.congestion_level = VehiclePosition.CONGESTION
        else:
            ent.vehicle.congestion_level = VehiclePosition.RUNNING_SMOOTHLY

        # Seems like the vehicle is never marked as being on a stop
        # (vehicleatstop is always false)

    return msg
