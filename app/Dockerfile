FROM python:3.9-slim

LABEL maintainer="Stephen Newey <github@s-n.me>"

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY octopus_to_influxdb.py ./

ENTRYPOINT [ "python", "./octopus_to_influxdb.py" ]
