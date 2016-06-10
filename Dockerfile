FROM python:2
MAINTAINER Reittiopas version: 0.1

RUN \
  pip install transitfeed && \
  pip install protobuf && \
  pip install flask && \
  pip install python-dateutil && \
  pip install pytz

ENV DIR_PATH=/opt/siri2gtfsrt
RUN mkdir -p ${DIR_PATH}
WORKDIR ${DIR_PATH}
ADD . ${DIR_PATH}

ENV CHAIN_URL=http://api.digitransit.fi/realtime/raildigitraffic2gtfsrt/v1/hsl
ENV TRIP_UPDATE_URL=http://api.digitransit.fi/realtime/service-alerts/v1/
ENV HSL_URL=http://api.digitransit.fi/realtime/vehicle-positions/v1/siriaccess/vm/json?operatorRef=HSL
ENV PORT=8080
EXPOSE ${PORT}

CMD python siri2gtfsrt.py
