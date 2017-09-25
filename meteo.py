#! /usr/bin/env python3

import paho.mqtt.client as mqtt
import requests
import json
import datetime
import sys


if len(sys.argv) < 2:
    print("usage: meteo.py <firebase_url>")
    exit(1)

values = {"temperature": None, "humidity": None}
firebase_url = sys.argv[1]


def on_connect(client, userdata, session, rc):
    client.subscribe("temperature")
    client.subscribe("humidity")


def on_message(client, userdata, msg):
    value = float(msg.payload.decode("utf-8"))
    values[msg.topic] = value
    if (msg.topic == "humidity" and
        values["temperature"] is not None and
        values["humidity"] is not None):

        key = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        url = firebase_url + "/meteo/" + key + ".json"
        response = requests.put(url, data=json.dumps(values))
        print(response)


client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.connect("localhost", 1883, 60)
client.loop_forever()
