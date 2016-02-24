FROM python:2
MAINTAINER Reittiopas version: 0.1

RUN \
  apt-get update && \
  apt-get install -y python-pip && \
  pip install transitfeed && \
  pip install protobuf && \
  pip install flask && \
  pip install python-dateutil && \
  pip install pytz

ENV DIR_PATH=/opt/siri2gtfsrt
RUN mkdir -p ${DIR_PATH}
WORKDIR ${DIR_PATH}
ADD . ${DIR_PATH}

ENV CHAIN_URL=http://beta.digitransit.fi/raildigitraffic2gtfsrt/hsl
ENV TRIP_UPDATE_URL=http://beta.digitransit.fi/hslalert/
ENV HSL_URL=http://beta.digitransit.fi/navigator-server/siriaccess/vm/json?operatorRef=HSL
ENV PORT=8080
EXPOSE ${PORT}

CMD python siri2gtfsrt.py
