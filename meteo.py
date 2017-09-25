#! /usr/bin/env python3

import paho.mqtt.client as mqtt
import requests
import json
import datetime
import sys


if len(sys.argv) < 2:
    print("usage: meteo.py <firebase_url>")
    exit(1)

values = {}
info = {}
firebase_url = sys.argv[1]


def on_connect(client, userdata, session, rc):
    client.subscribe("device/+/+")


def on_message(client, userdata, msg):
    value = msg.payload.decode("utf-8")
    topicItems = msg.topic.split('/')
    device = topicItems[1]
    topic = topicItems[2]

    if device not in values:
        values[device] = {}

    if topic == "info":
        info[device] = value.split(':')
        url = firebase_url + "/devices/" + device + ".json"
        data = dict([(i, True) for i in info[device]])
        requests.put(url, data=json.dumps(data))
    else:
        print(topic, value)
        values[device][topic] = float(value)

        if (device in info and
            len(info[device]) > 0 and
            topic == info[device][0] and
            len(info[device]) == len(values[device])):

            key = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
            url = firebase_url + "/values/" + device + '/' + key + ".json"
            requests.put(url, data=json.dumps(values[device]))


client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.connect("localhost", 1883, 60)
client.loop_forever()
