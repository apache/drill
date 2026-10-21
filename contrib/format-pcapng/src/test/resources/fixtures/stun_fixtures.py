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
"""Generates decoders/stun/stun.pcapng. Run from any directory:
   python3 contrib/format-pcapng/src/test/resources/fixtures/stun_fixtures.py
If scapy is installed, the messages are cross-checked with scapy's STUN layer."""
import os
import struct
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from pcap_fixtures import shb, idb, epb, ipv4, udp, write  # noqa: E402

OUT = os.path.join(HERE, '..', 'decoders', 'stun')
COOKIE = 0x2112A442
TXID = bytes.fromhex('b7e7a701bc34d686fa87dfae')

# RFC 5769 section 2.2: sample IPv4 response, XOR-MAPPED-ADDRESS 192.0.2.1:32853, SOFTWARE "test vector"
RFC5769_RESPONSE = bytes.fromhex(
    '0101003c2112a442b7e7a701bc34d686fa87dfae'
    '8022000b7465737420766563746f7220'
    '002000080001a147e112a643'
    '000800142b91f599fd9e90c38c7489f92af9ba53f06be7d7'
    '80280004c07d4c96')


def attr(t, value):
    return struct.pack('>HH', t, len(value)) + value + b'\0' * (-len(value) % 4)


def message(t, txid, *attributes):
    body = b''.join(attributes)
    return struct.pack('>HHI', t, len(body), COOKIE) + txid + body


REQUEST = message(0x0001, bytes(range(1, 13)), attr(0x0006, b'evtj:h6vY'), attr(0x8022, b'client 1.0'),
                  attr(0x0024, struct.pack('>I', 0x6e0001ff)))
ERROR = message(0x0113, bytes(range(13, 25)), attr(0x0009, b'\0\0\x04\x01Unauthorized'),
                attr(0x0014, b'example.org'), attr(0x0015, b'f//499k954d6OL34oL9FSTvy64sA'))
GOOGLE = message(0x0001, bytes(range(25, 37)))


def cross_check():
    try:
        from scapy.contrib.stun import STUN
    except ImportError:
        print('scapy not installed; skipping the cross-check')
        return
    response = STUN(RFC5769_RESPONSE)
    assert response.stun_message_type == 0x0101 and response.magic_cookie == COOKIE
    xor = [a for a in response.attributes if a.type == 0x0020][0]
    assert (xor.xip, xor.xport) == ('192.0.2.1', 32853), (xor.xip, xor.xport)
    request = STUN(REQUEST)
    assert request.attributes[0].username == b'evtj:h6vY'
    assert [a.type for a in request.attributes] == [0x0006, 0x8022, 0x0024]
    assert STUN(ERROR).length == len(ERROR) - 20
    print('scapy cross-check passed')


def main():
    cross_check()
    # An attribute whose length (40) overruns the message
    bad = bytearray(message(0x0001, bytes(12), attr(0x8022, b'abcd')))
    bad[23] = 40
    client, server = '10.0.0.1', '192.0.2.10'
    f = shb() + idb(101)
    rows = [
        (client, server, 50000, 3478, REQUEST),                       # binding request
        (server, client, 3478, 50000, RFC5769_RESPONSE),              # binding success response
        (server, client, 3478, 50000, ERROR),                         # allocate error response
        (client, server, 50000, 3478, b'\x00\x01\x00\x00' + bytes(16)),  # RFC 3489 request: no cookie
        (client, server, 50000, 3478, bytes(bad)),                    # malformed attribute
        (client, '74.125.250.129', 50001, 19302, GOOGLE),             # Google STUN port
    ]
    for src, dst, sport, dport, data in rows:
        f += epb(0, ipv4(17, udp(sport, dport, data), src, dst))
    os.makedirs(OUT, exist_ok=True)
    write(os.path.join(OUT, 'stun.pcapng'), f)


if __name__ == '__main__':
    main()
