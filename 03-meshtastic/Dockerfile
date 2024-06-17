FROM python:3.9-alpine

LABEL maintainer="DPreyl" \
      description="Meshtastic to MQTT Bridge"

COPY requirements.txt /
RUN pip install -r /requirements.txt

COPY . /app
WORKDIR /app

CMD ["python3", "-u", "meshtastic_bridge.py"]
