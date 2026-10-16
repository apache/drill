#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Generates decoders/mqtt/mqtt.pcapng. The control packets are built by hand; if scapy's MQTT contrib
layer is installed the CONNECT is parsed back as a cross-check. Sessions, by client port: 52001 a CONNECT
with user name, password and a will, then SUBSCRIBE and PUBLISH, with a CONNACK accepting; 52002 HTTP on
port 1883 (not MQTT); 52003 a CONNECT whose payload is cut short (decode_error)."""
import os
import struct
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from tcp_sessions import capture, session  # noqa: E402

C, S = True, False


def mstr(s):
    b = s.encode()
    return struct.pack('>H', len(b)) + b


def rem_len(value):
    out = b''
    while True:
        byte = value % 128
        value //= 128
        if value > 0:
            byte |= 0x80
        out += bytes([byte])
        if value == 0:
            return out


def packet(ptype, body):
    return bytes([ptype << 4]) + rem_len(len(body)) + body


def connect(level, flags, client_id, will_topic, user, password):
    body = mstr('MQTT') + bytes([level, flags]) + struct.pack('>H', 60)
    body += mstr(client_id)
    if will_topic is not None:
        body += mstr(will_topic) + mstr('offline')
    if user is not None:
        body += mstr(user)
    if password is not None:
        body += mstr(password)
    return packet(1, body)


def connack(code):
    return packet(2, bytes([0, code]))


def subscribe(packet_id, topic):
    return packet(8, struct.pack('>H', packet_id) + mstr(topic) + bytes([0]))


def publish(topic, payload):
    return packet(3, mstr(topic) + payload.encode())


def cross_check(raw):
    try:
        from scapy.contrib.mqtt import MQTT
    except ImportError:
        return
    parsed = MQTT(raw)
    assert parsed.payload.clientId == b'sensor-1', parsed.payload.clientId
    assert parsed.payload.username == b'mqttuser', parsed.payload.username
    assert parsed.payload.willtopic == b'device/status', parsed.payload.willtopic


def main():
    conn = connect(4, 0xC4, 'sensor-1', 'device/status', 'mqttuser', 'mqttpass')
    cross_check(conn)
    full = session(52001, 1883, [
        (C, conn),
        (S, connack(0)),
        (C, subscribe(1, 'home/#')),
        (C, publish('home/temp', '21')),
    ])
    http = session(52002, 1883, [(C, b'GET / HTTP/1.0\r\n\r\n'), (S, b'HTTP/1.0 200 OK\r\n\r\n')])
    # A well-formed CONNECT fixed header and MQTT name, but the client-identifier length overruns the packet
    body = mstr('MQTT') + bytes([4, 0xC0]) + struct.pack('>H', 60) + struct.pack('>H', 50) + b'abc'
    truncated = session(52003, 1883, [(C, packet(1, body))])
    capture(os.path.join('mqtt', 'mqtt.pcapng'), [full, http, truncated])


if __name__ == '__main__':
    main()
