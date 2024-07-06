"""Simple program to demo how to use meshtastic library.
   To run: python examples/pub_sub_example2.py
"""

import sys
import time

from pubsub import pub

import meshtastic
import meshtastic.serial_interface
import meshtastic.mesh_pb2
import meshtastic.portnums_pb2

import paho.mqtt.client as mqtt

import json


MQTT_ADDRESS = 'mosquitto'
#MQTT_ADDRESS = 'localhost'
MQTT_USER = 'mqttuser'
MQTT_PASSWORD = 'mqttpassword'
MQTT_CLIENT_ID = 'MeshtasticMQTTBridge'

MQTT_TOPIC_NOTIFICATION = 'msh/ZS24/2/json/mqtt/'

mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, MQTT_CLIENT_ID)
mqtt_channel = 0

iface = meshtastic.serial_interface.SerialInterface(debugOut=sys.stdout)

def onMestasticReceive(proxymessage, interface):  # pylint: disable=unused-argument
    """called when a packet arrives"""
    print(f"Received: {proxymessage}")
    mqtt_client.publish(proxymessage.topic, proxymessage.text if hasattr(proxymessage, 'text') else proxymessage.data)

def onMQTTMessage(client, userdata, msg):
    print("Recived MQTT-Message: ")
    print(msg.topic + ' ' + str(msg.payload))

    msg = json.loads(msg.payload)
    iface.sendText(msg['payload'][:200], channelIndex=mqtt_channel)

    # m = meshtastic.mesh_pb2.MqttClientProxyMessage(topic=msg.topic, data=msg.payload, retained=msg.retain)
    # if msg.payload['type'] == 'sendtext':
    #p = meshtastic.mesh_pb2.MeshPacket()
    #p.channel = mqtt_channel

    #toRadio = meshtastic.mesh_pb2.ToRadio()
    #toRadio.packet.mqttClientProxyMessage.CopyFrom(p)
    #iface._sendToRadio(toRadio)
    #iface.sendData(data=p, channelIndex=mqtt_channel)



def onMQTTConnect(client, userdata, flags, reason_code, properties):
    """ The callback for when the client receives a CONNACK response from the server."""
    print('Connected with result code ' + str(reason_code))
    client.subscribe(MQTT_TOPIC_NOTIFICATION)

def getMqttChannel(iface):
    for c in iface.localNode.channels or []:
        if c.settings and c.settings.name.lower() == "mqtt":
            return c.index
    return 0

def readConfig():
    if iface.nodesByNum:
        #logging.debug(f"self.nodes:{self.nodes}")
        for node in iface.nodesByNum.values():
            objDesc = node.localConfig.DESCRIPTOR
            config_type = objDesc.fields_by_name.get('lora')
            node.get(config_type)

def onMestasticDisconnect(interface):
    print("Reconnect to serial device")
    #interface.close()
    time.sleep(0.1)
    #interface.connect()

    interface.waitForConfig()
    print("Reconnected")


def main():
    global mqtt_client, mqtt_channel, iface
    #time.sleep(10)

    mqtt_client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
    mqtt_client.on_connect = onMQTTConnect
    mqtt_client.on_message = onMQTTMessage

    mqtt_client.connect(MQTT_ADDRESS, 1883)
    mqtt_client.loop_start()

    pub.subscribe(onMestasticReceive, "meshtastic.mqttclientproxymessage")
    pub.subscribe(onMestasticDisconnect, "meshtastic.connection.lost")


    try:
        mqtt_channel = iface.localNode.getChannelByName('mqtt').index
        #readConfig()
        while True:
            time.sleep(1000)
        iface.close()
    except Exception as ex:
        print(f"Error: Could not connect to {sys.argv[1]} {ex}")
        sys.exit(1)


if __name__ == '__main__':
    print('Meshtastic to MQTT bridge')
    main()
