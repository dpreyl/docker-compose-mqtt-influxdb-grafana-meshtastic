#!/usr/bin/env python3

"""A MQTT to InfluxDB Bridge

This script receives MQTT data and saves those to InfluxDB.

"""

from flask import Flask, request, jsonify
import re
from typing import NamedTuple

import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient

import time
import copy

import json

from sqlalchemy import create_engine, Column, Integer, String, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# SQLAlchemy Setup
DATABASE_URI = 'sqlite:////nodedb/nodeinfoDB.db'
engine = create_engine(DATABASE_URI)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# NodeInfo Model
class NodeInfo(Base):
    __tablename__ = 'nodeinfo'
    id = Column(Integer)
    address = Column(Integer, primary_key=True, index=True)
    hardware = Column(Integer)
    longname = Column(String)
    shortname = Column(String)

# Initialize the database
Base.metadata.create_all(bind=engine)

# Initialize session
session = SessionLocal()

INFLUXDB_ADDRESS = 'influxdbZS'
INFLUXDB_USER = 'root'
INFLUXDB_PASSWORD = 'root'
INFLUXDB_DATABASE = 'iothon_db'

MQTT_ADDRESS = 'mosquitto'
MQTT_USER = 'mqttuser'
MQTT_PASSWORD = 'mqttpassword'
MQTT_TOPIC = 'msh/ZS24/2/json/#'  # [bme280|mijia]/[temperature|humidity|battery|status]
MQTT_CLIENT_ID = 'MQTTInfluxDBBridge'

MQTT_TOPIC_NOTIFICATION = 'msh/ZS24/2/json/mqtt/'

influxdb_client = InfluxDBClient(INFLUXDB_ADDRESS, 8086, INFLUXDB_USER, INFLUXDB_PASSWORD, None)

nodeinfoDB = {}

app = Flask(__name__)


def on_connect(client, userdata, flags, rc):
    """ The callback for when the client receives a CONNACK response from the server."""
    print('Connected with result code ' + str(rc))
    client.subscribe(MQTT_TOPIC)


def on_message(client, userdata, msg):
    """The callback for when a PUBLISH message is received from the server."""
    print(msg.topic + ' ' + str(msg.payload))
    try:
        sensor_data = _parse_mqtt_message(msg.topic, msg.payload.decode('utf-8'))
    except Exception as e:
        print("Error with parse message:")
        print(e)
    if not (sensor_data is None):
        try:
            _send_sensor_data_to_influxdb(sensor_data)
        except Exception as e:
            print("Error with write to DB:")
            print(e)


def _parse_mqtt_message(topic, payload):
    global nodeinfoDB
    msg = json.loads(payload)
    print("Message:")
    print(msg)
    if msg['type'] == 'nodeinfo':
        update_nodeinfoDB(msg)
    return msg


def add_optional_fields(orig, to, names):
    for name in names:
        if name in orig:
            if name == 'snr':
                if 200 > orig[name] > -200:
                    to[name] = float(orig[name])
            elif name == 'rssi':
                if 200 > orig[name] > -200:
                    to[name] = orig[name]
            else:
                to[name] = orig[name]


def _send_sensor_data_to_influxdb(sensor_data):
    json_body = []
    message_cnt = 1
    if sensor_data['type'] == 'neighborinfo':
        message_cnt = max(1, len(sensor_data['payload']['neighbors']))
    for m_ind in range(message_cnt):
        json_message = {'measurement': 'meshtastic',
                        'tags': {},
                        'fields': {}}

        add_optional_fields(sensor_data, json_message['tags'], ['type', 'channel', 'from', 'sender', 'timestamp', 'to'])
        add_optional_fields(sensor_data, json_message['fields'], ['hops_away', 'id', 'rssi', 'snr'])

        # Query the database directly for node info
        node_info_query = session.query(NodeInfo).filter_by(address=sensor_data['from']).first()
        if node_info_query:
            json_message['tags']['hardware'] = node_info_query.hardware
            json_message['tags']['longname'] = node_info_query.longname
            json_message['tags']['shortname'] = node_info_query.shortname
        else:
            print("Not in nodeinfo, adress: {}".format(sensor_data['from']))
            continue
        if 'payload' in sensor_data:
            payload = copy.deepcopy(sensor_data['payload'])
            if 'id' in payload:
                del (payload['id'])
            if sensor_data['type'] == 'neighborinfo':
                if len(payload['neighbors']) > m_ind:
                    payload['neighbor_node_id'] = payload['neighbors'][m_ind]['node_id']
                    payload['neighbor_snr'] = payload['neighbors'][m_ind]['snr']
                del (payload['neighbors'])
            json_message['fields'].update(payload)

        json_body.append(json_message)
    if len(json_body) > 0:
        try:
            influxdb_client.write_points(json_body)
        except Exception as e:
            print("Error with influxdb:")
            print(e)
    print("written points:")
    print(json_body)

def update_nodeinfoDB(message):
    nodeinfo_entry = NodeInfo(address=message['from'], **message['payload'])
    session.merge(nodeinfo_entry)
    session.commit()


def _init_influxdb_database():
    databases = influxdb_client.get_list_database()
    if len(list(filter(lambda x: x['name'] == INFLUXDB_DATABASE, databases))) == 0:
        print('Creating database ' + INFLUXDB_DATABASE)
        influxdb_client.create_database(INFLUXDB_DATABASE)
    influxdb_client.switch_database(INFLUXDB_DATABASE)


@app.route('/webhook', methods=['POST'])
def grafana_webhook():
    data = {
        'from': 203966156,
        'type': 'sendtext',
        'payload': request.json['title'] + ": " + request.json['message']
    }
    print("Received Grafana Alert:", data)
    mqtt_client.publish(MQTT_TOPIC_NOTIFICATION, json.dumps(data))
    return jsonify(success=True), 200


mqtt_client = mqtt.Client(MQTT_CLIENT_ID)


def main():
    global mqtt_client
    time.sleep(10)

    print('Connecting to the database ' + INFLUXDB_DATABASE)
    _init_influxdb_database()

    mqtt_client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    mqtt_client.connect(MQTT_ADDRESS, 1883)
    mqtt_client.loop_start()
    app.run(host='0.0.0.0', port=5000)  # Listen for Grafana webhook on port 5000


if __name__ == '__main__':
    print('MQTT to InfluxDB bridge')
    main()
