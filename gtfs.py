import gtfs_realtime_pb2


def parse_gtfsrt(raw):
    if raw is None:
        return None
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.ParseFromString(raw)

    return msg
