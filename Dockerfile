FROM python:3-alpine
MAINTAINER Reittiopas version: 0.1

RUN \
  pip install gtfs-realtime-bindings && \
  pip install protobuf && \
  pip install flask && \
  pip install python-dateutil && \
  pip install pytz

ENV DIR_PATH=/opt/siri2gtfsrt
RUN mkdir -p ${DIR_PATH}
WORKDIR ${DIR_PATH}
ADD . ${DIR_PATH}

ENV FOLI_URL=http://data.foli.fi/siri/vm

ENV PORT=8080
EXPOSE ${PORT}

CMD python siri2gtfsrt.py
