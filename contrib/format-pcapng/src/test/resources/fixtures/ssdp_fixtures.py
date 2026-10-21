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
"""Generates decoders/ssdp/ssdp.pcapng. Run from any directory:
   python3 contrib/format-pcapng/src/test/resources/fixtures/ssdp_fixtures.py"""
import os
import struct
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pcap_fixtures import shb, idb, epb, ipv4, udp, tcp, write  # noqa: E402

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'decoders', 'ssdp')

MESSAGES = [
    (40000, 1900, b'M-SEARCH * HTTP/1.1\r\nHOST: 239.255.255.250:1900\r\nMAN: "ssdp:discover"\r\nMX: 2\r\n'
                  b'ST: ssdp:all\r\nUSER-AGENT: Linux/5 UPnP/1.1 test/1.0\r\n\r\n'),
    (1900, 40000, b'HTTP/1.1 200 OK\r\nCACHE-CONTROL: max-age=100\r\nEXT:\r\n'
                  b'LOCATION: http://192.168.1.10:49152/desc.xml\r\nSERVER: Linux/3.14 UPnP/1.0 IpBridge/1.26.0\r\n'
                  b'ST: upnp:rootdevice\r\nUSN: uuid:2f402f80-da50-11e1-9b23-001788255acc::upnp:rootdevice\r\n\r\n'),
    (1900, 1900, b'NOTIFY * HTTP/1.1\r\nHOST: 239.255.255.250:1900\r\nNT: upnp:rootdevice\r\nNTS: ssdp:byebye\r\n'
                 b'USN: uuid:2f402f80-da50-11e1-9b23-001788255acc::upnp:rootdevice\r\n\r\n'),
    # Not SSDP on port 1900
    (40000, 1900, b'GET / HTTP/1.1\r\nHost: example.com\r\n\r\n'),
    # Malformed header line
    (40000, 1900, b'NOTIFY * HTTP/1.1\r\nNT: upnp:rootdevice\r\nnot a header\r\n\r\n'),
]


def main():
    os.makedirs(OUT, exist_ok=True)
    f = shb() + idb(101)
    for sport, dport, data in MESSAGES:
        f += epb(0, ipv4(17, udp(sport, dport, data), '192.168.1.20', '239.255.255.250'))
    write(os.path.join(OUT, 'ssdp.pcapng'), f)


if __name__ == '__main__':
    main()
