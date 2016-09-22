import json
import gtfs_realtime_pb2
from traceback import print_exc


def handle_siri(raw):
    siri_data = json.loads(raw.decode('utf-8'))['Siri']
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.header.gtfs_realtime_version = "1.0"
    msg.header.incrementality = msg.header.FULL_DATASET
    msg.header.timestamp = int(siri_data['ServiceDelivery']['ResponseTimestamp']) / 1000

    for i, vehicle in enumerate(siri_data['ServiceDelivery']['VehicleMonitoringDelivery'][0]['VehicleActivity']):
        route_id = vehicle['MonitoredVehicleJourney']['LineRef']['value'][:5].strip()

        if route_id in ('1300', '1300V', '1300M'):
            continue  # No other information than location for metros

        if route_id[0:4] in ('3001', '3002'):
            continue  # Train data is better at rata.digitraffic.fi

        if route_id[0] in ('k', 'K'):
            continue  # Kutsuplus

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

        ent.trip_update.trip.start_date = vehicle['MonitoredVehicleJourney']['FramedVehicleJourneyRef']['DataFrameRef']['value'].replace("-", "")
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
