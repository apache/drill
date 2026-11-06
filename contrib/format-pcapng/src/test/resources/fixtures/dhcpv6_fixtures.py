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
"""Generates decoders/dhcpv6/dhcpv6.pcapng. Run from any directory:
   python3 contrib/format-pcapng/src/test/resources/fixtures/dhcpv6_fixtures.py"""
import os
import socket
import struct
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pcap_fixtures import shb, idb, epb, udp, write  # noqa: E402

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'decoders', 'dhcpv6')
CLIENT_DUID = bytes.fromhex('000100012a2b2c2d021122334455')
SERVER_DUID = bytes.fromhex('00030001020000000001')


def ip6(a):
    return socket.inet_pton(socket.AF_INET6, a)


def ipv6(next_header, payload, src, dst):
    return struct.pack('>IHBB16s16s', 6 << 28, len(payload), next_header, 64, ip6(src), ip6(dst)) + payload


def option(code, value):
    return struct.pack('>HH', code, len(value)) + value


def message(msg_type, xid, *options):
    return bytes([msg_type]) + xid.to_bytes(3, 'big') + b''.join(options)


def dns_names(*names):
    out = b''
    for name in names:
        for label in name.split('.'):
            out += bytes([len(label)]) + label.encode()
        out += b'\0'
    return out


def dhcpv6():
    # Link type 101 (raw IPv6). Rows: SOLICIT, REPLY, RELAY-FORW of a SOLICIT, not DHCPv6 on 547, cut REPLY
    solicit = message(1, 0x5A1B2C, option(1, CLIENT_DUID), option(8, b'\0\0'), option(6, struct.pack('>HH', 23, 24)),
                      option(3, struct.pack('>III', 1, 0, 0)), option(39, b'\x01' + dns_names('laptop.example.org')))
    ia_addr = option(5, ip6('2001:db8::100') + struct.pack('>II', 3600, 7200))
    reply = message(7, 0x5A1B2C, option(1, CLIENT_DUID), option(2, SERVER_DUID),
                    option(3, struct.pack('>III', 1, 1800, 2880) + ia_addr),
                    option(23, ip6('2001:db8::53')), option(24, dns_names('example.org', 'corp.example')))
    relay = (bytes([12, 0]) + ip6('2001:db8:1::1') + ip6('fe80::211:22ff:fe33:4455')
             + option(18, b'eth0') + option(9, solicit))
    f = shb() + idb(101)
    f += epb(0, ipv6(17, udp(546, 547, solicit), 'fe80::211:22ff:fe33:4455', 'ff02::1:2'))
    f += epb(0, ipv6(17, udp(547, 546, reply), 'fe80::1', 'fe80::211:22ff:fe33:4455'))
    f += epb(0, ipv6(17, udp(547, 547, relay), '2001:db8:1::1', '2001:db8::1'))
    f += epb(0, ipv6(17, udp(546, 547, b'not a DHCPv6 message'), 'fe80::2', 'ff02::1:2'))
    f += epb(0, ipv6(17, udp(547, 546, reply[:-5]), 'fe80::1', 'fe80::211:22ff:fe33:4455'))
    os.makedirs(OUT, exist_ok=True)
    write(os.path.join(OUT, 'dhcpv6.pcapng'), f)


if __name__ == '__main__':
    dhcpv6()
