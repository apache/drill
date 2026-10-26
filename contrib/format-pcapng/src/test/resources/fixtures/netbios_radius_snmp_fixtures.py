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
"""Generates the NetBIOS-NS, RADIUS and SNMP decoder fixtures. Run from any directory:
   python3 contrib/format-pcapng/src/test/resources/fixtures/netbios_radius_snmp_fixtures.py"""
import os
import struct
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from pcap_fixtures import shb, idb, epb, ipv4, udp, write  # noqa: E402

DECODERS = os.path.join(HERE, '..', 'decoders')


def capture(name, packets):
    """packets: (src, sport, dst, dport, payload). Link type 101 (raw IP)."""
    f = shb() + idb(101)
    for src, sport, dst, dport, payload in packets:
        f += epb(0, ipv4(17, udp(sport, dport, payload), src, dst))
    directory = os.path.join(DECODERS, name)
    os.makedirs(directory, exist_ok=True)
    write(os.path.join(directory, name + '.pcapng'), f)


# NetBIOS Name Service

def nb_name(name, suffix):
    raw = name.encode().ljust(15, b' ') + bytes([suffix])
    return bytes([32]) + bytes(c for b in raw for c in (0x41 + (b >> 4), 0x41 + (b & 0xF))) + b'\0'


def nbns(ident, flags, questions, answers):
    body = b''.join(nb_name(n, s) + struct.pack('>HH', 0x20, 1) for n, s in questions)
    for n, s, ttl, nb_flags, address in answers:
        body += nb_name(n, s) + struct.pack('>HHIHH', 0x20, 1, ttl, 6, nb_flags) + bytes(address)
    return struct.pack('>HHHHHH', ident, flags, len(questions), len(answers), 0, 0) + body


def netbios_ns():
    # Rows: broadcast query, positive response, non-NetBIOS bytes on port 137, response cut 2 bytes short
    query = nbns(0x8001, 0x0110, [('FILESRV', 0x20)], [])
    response = nbns(0x8001, 0x8500, [], [('FILESRV', 0x20, 300000, 0x6000, [10, 0, 0, 7])])
    capture('netbios_ns', [
        ('10.0.0.5', 137, '10.0.0.255', 137, query),
        ('10.0.0.7', 137, '10.0.0.5', 137, response),
        ('10.0.0.5', 137, '10.0.0.255', 137, b'this is not a NetBIOS name service packet at all'),
        ('10.0.0.7', 137, '10.0.0.5', 137, response[:-2]),
    ])


# RADIUS

def attr(t, value):
    if isinstance(value, str):
        value = value.encode()
    return bytes([t, 2 + len(value)]) + value


def radius(code, ident, *attrs):
    body = b''.join(attrs)
    return struct.pack('>BBH', code, ident, 20 + len(body)) + bytes(range(16)) + body


def radius_fixture():
    # Rows: Access-Request, Access-Accept, Accounting-Request, non-RADIUS bytes on 1812, truncated request
    request = radius(1, 7, attr(1, 'alice'), attr(2, bytes(16)), attr(4, bytes([10, 0, 0, 2])),
                     attr(5, struct.pack('>I', 3)), attr(32, 'nas01'), attr(31, '00-11-22-33-44-55'))
    accept = radius(2, 7, attr(18, 'Welcome'))
    accounting = radius(4, 9, attr(40, struct.pack('>I', 1)), attr(44, 'S-1'), attr(8, bytes([10, 1, 2, 3])))
    capture('radius', [
        ('10.0.0.2', 40000, '10.0.0.1', 1812, request),
        ('10.0.0.1', 1812, '10.0.0.2', 40000, accept),
        ('10.0.0.2', 40001, '10.0.0.1', 1813, accounting),
        ('10.0.0.2', 40000, '10.0.0.1', 1812, b'GET / HTTP/1.1\r\nHost: example\r\n\r\n'),
        ('10.0.0.2', 40000, '10.0.0.1', 1812, request[:-4]),
    ])


# SNMP

def tlv(tag, *parts):
    content = b''.join(parts)
    if len(content) < 128:
        return bytes([tag, len(content)]) + content
    return bytes([tag, 0x82]) + struct.pack('>H', len(content)) + content


def integer(v):
    length = max(1, (v.bit_length() + 8) // 8)
    return tlv(0x02, v.to_bytes(length, 'big', signed=True))


def octets(s):
    return tlv(0x04, s.encode() if isinstance(s, str) else s)


SYS_DESCR = tlv(0x06, bytes([0x2B, 6, 1, 2, 1, 1, 1, 0]))
SYS_UPTIME = tlv(0x06, bytes([0x2B, 6, 1, 2, 1, 1, 3, 0]))


def pdu(tag, request_id, *varbinds):
    return tlv(tag, integer(request_id), integer(0), integer(0), tlv(0x30, *varbinds))


def snmp():
    # Rows: v2c get-request, get-response, v1 trap, v3 encrypted, non-SNMP bytes on 161, truncated get-request
    get = tlv(0x30, integer(1), octets('public'),
              pdu(0xA0, 1234, tlv(0x30, SYS_DESCR, tlv(0x05)), tlv(0x30, SYS_UPTIME, tlv(0x05))))
    response = tlv(0x30, integer(1), octets('public'),
                   pdu(0xA2, 1234, tlv(0x30, SYS_DESCR, octets('Linux box')),
                       tlv(0x30, SYS_UPTIME, tlv(0x43, struct.pack('>I', 123456)))))
    trap = tlv(0x30, integer(0), octets('traps'),
               tlv(0xA4, tlv(0x06, bytes([0x2B, 6, 1, 4, 1, 9])), tlv(0x40, bytes([10, 0, 0, 9])), integer(6),
                   integer(42), tlv(0x43, bytes([0x10])), tlv(0x30, tlv(0x30, SYS_DESCR, octets('x')))))
    usm = tlv(0x30, octets(bytes([0x80, 0, 0x1F, 0x88])), integer(1), integer(100), octets('carol'),
              octets(bytes(12)), octets(bytes(8)))
    v3 = tlv(0x30, integer(3), tlv(0x30, integer(77), integer(65507), octets(bytes([7])), integer(3)),
             octets(usm), octets(bytes(24)))
    capture('snmp', [
        ('10.0.0.1', 40000, '10.0.0.2', 161, get),
        ('10.0.0.2', 161, '10.0.0.1', 40000, response),
        ('10.0.0.9', 40002, '10.0.0.1', 162, trap),
        ('10.0.0.1', 40003, '10.0.0.2', 161, v3),
        ('10.0.0.1', 40000, '10.0.0.2', 161, b'GET / HTTP/1.1\r\nHost: example\r\n\r\n'),
        ('10.0.0.1', 40000, '10.0.0.2', 161, get[:-3]),
    ])


if __name__ == '__main__':
    netbios_ns()
    radius_fixture()
    snmp()
