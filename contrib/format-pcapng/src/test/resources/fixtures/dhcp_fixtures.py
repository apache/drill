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
"""Generates decoders/dhcp/dhcp.pcapng. Run from any directory:
   python3 contrib/format-pcapng/src/test/resources/fixtures/dhcp_fixtures.py"""
import os
import socket
import struct
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pcap_fixtures import shb, idb, epb, ipv4, udp, write  # noqa: E402

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'decoders', 'dhcp')
MAC = bytes.fromhex('021122334455')


def ip(a):
    return socket.inet_aton(a)


def option(code, value):
    return bytes([code, len(value)]) + value


def bootp(op, xid, yiaddr='0.0.0.0', siaddr='0.0.0.0', options=b'', cookie=True):
    header = struct.pack('>BBBBIHH4s4s4s4s', op, 1, 6, 0, xid, 0, 0x8000,
                         ip('0.0.0.0'), ip(yiaddr), ip(siaddr), ip('0.0.0.0'))
    header += MAC + bytes(10) + bytes(64) + bytes(128)
    return header + (b'\x63\x82\x53\x63' if cookie else b'') + options


def dhcp():
    # Link type 101. Rows: DISCOVER, ACK, not DHCP on port 67, plain BOOTP (no cookie), ACK with a cut option
    discover = bootp(1, 0x3903F326, options=option(53, b'\x01') + option(12, b'laptop') + option(60, b'MSFT 5.0')
                     + option(55, bytes([1, 3, 6, 15])) + b'\xff')
    ack_options = (option(53, b'\x05') + option(54, ip('192.168.1.1')) + option(51, struct.pack('>I', 86400))
                   + option(1, ip('255.255.255.0')) + option(3, ip('192.168.1.1'))
                   + option(6, ip('8.8.8.8') + ip('1.1.1.1')) + option(15, b'example.org') + b'\xff')
    ack = bootp(2, 0x3903F326, '192.168.1.100', '192.168.1.1', ack_options)
    f = shb() + idb(101)
    f += epb(0, ipv4(17, udp(68, 67, discover), '0.0.0.0', '255.255.255.255'))
    f += epb(0, ipv4(17, udp(67, 68, ack), '192.168.1.1', '192.168.1.100'))
    f += epb(0, ipv4(17, udp(68, 67, b'this is not a DHCP message'), '10.0.0.1', '10.0.0.2'))
    f += epb(0, ipv4(17, udp(68, 67, bootp(1, 1, cookie=False)), '0.0.0.0', '255.255.255.255'))
    f += epb(0, ipv4(17, udp(67, 68, ack[:-10]), '192.168.1.1', '192.168.1.100'))
    os.makedirs(OUT, exist_ok=True)
    write(os.path.join(OUT, 'dhcp.pcapng'), f)


if __name__ == '__main__':
    dhcp()
