#!/usr/bin/python

import logging

from google.protobuf import text_format
from flask import Flask
from flask import request
from urllib.request import urlopen
import os
import threading
from io import BytesIO
import gzip

import foli


logging.basicConfig(level=os.environ.get('LOGLEVEL', 'INFO'))

# Turku realtime siri feed
FOLI_URL = os.environ.get('FOLI_URL', "http://data.foli.fi/siri/vm")


class Poll(object):
    def __init__(self, url, interval, fn, preprocess=False, gzipped=False):
        self.url = url
        self.interval = interval
        self.stopped = threading.Event()
        self.result = None
        self.fn = fn
        self.preprocess = preprocess
        self.gzipped = gzipped
        thread = threading.Thread(target=self.run)
        thread.daemon = True
        thread.start()

    def run(self):
        while True:
            try:
                logging.debug("fetching data from %s", self.url)

                result = urlopen(self.url, timeout=60).read()
                if self.gzipped:
                    result = gzip.GzipFile(fileobj=BytesIO(result)).read()
                if self.fn is not None:
                    logging.debug("processing url %s", self.url)
                    self.result = self.fn(result)
                else:
                    logging.debug("storing content %s", self.url)
                    self.result = result
            except:
                logging.exception("fetching %s failed", self.url)

            self.stopped.wait(self.interval)

FOLI_poll = Poll(FOLI_URL, 60, foli.handle_journeys, gzipped=True)

app = Flask(__name__)


@app.route('/FOLI')
def turku_data():
    return toString(FOLI_poll.result)


def toString(msg):
    if 'debug' in request.args:
        return text_format.MessageToString(msg)
    else:
        return msg.SerializeToString()


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
#    app.debug = True
    app.run(host='0.0.0.0', port=port, threaded=True)
