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

lastMinute = datetime.datetime(1900, 1, 1)
last10minutes = datetime.datetime(1900, 1, 1)
last30minutes = datetime.datetime(1900, 1, 1)
lastHour = datetime.datetime(1900, 1, 1)


def on_connect(client, userdata, session, rc):
    client.subscribe("device/+/+")


def httpPut(url, data):
    try:
        requests.put(url, data=json.dumps(data))
    except Exception as e:
        print(e)
        pass


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
        data["deviceName"] = device
        httpPut(url, data)
    else:
        values[device][topic] = float(value)

        if (device in info and
            len(info[device]) > 0 and
            topic == info[device][0] and
            len(info[device]) == len(values[device])):

            now = datetime.datetime.utcnow()
            key = now.strftime("%Y-%m-%dT%H:%M:%SZ")

            url = firebase_url + "/measures/" + device + '/5seconds/' + key + ".json"
            httpPut(url, values[device])

            global lastMinute
            if (now - lastMinute).total_seconds() >= 60:
                lastMinute = now
                url = firebase_url + "/measures/" + device + '/minute/' + key + ".json"
                httpPut(url, values[device])

            global last10minutes
            if (now - last10minutes).total_seconds() >= 600:
                last10minutes = now
                url = firebase_url + "/measures/" + device + '/10minutes/' + key + ".json"
                httpPut(url, values[device])

            global last30minutes
            if (now - last30minutes).total_seconds() >= 1800:
                last30minutes = now
                url = firebase_url + "/measures/" + device + '/30minutes/' + key + ".json"
                httpPut(url, values[device])

            global lastHour
            if (now - lastHour).total_seconds() >= 3600:
                lastHour = now
                url = firebase_url + "/measures/" + device + '/hour/' + key + ".json"
                httpPut(url, values[device])


client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.connect("localhost", 1883, 60)
client.loop_forever()
