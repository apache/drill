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
"""Generates the ICMP, ICMPv6 and ARP decoder fixtures. Run from any directory:
   python3 contrib/format-pcapng/src/test/resources/fixtures/icmp_arp_fixtures.py"""
import os
import socket
import struct
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pcap_fixtures import ETHERNET, epb, idb, ipv4, pcap_file, shb, udp, write  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
DECODERS = os.path.join(HERE, '..', 'decoders')
ETHERNET_ARP = bytes.fromhex('ffffffffffff' '020000000001' '0806')
MAC1 = bytes.fromhex('020000000001')
MAC2 = bytes.fromhex('020000000002')


def ipv6(next_header, payload, src, dst):
    return struct.pack('>IHBB16s16s', 0x60000000, len(payload), next_header, 64,
                       socket.inet_pton(socket.AF_INET6, src), socket.inet_pton(socket.AF_INET6, dst)) + payload


def icmp(icmp_type, code, rest, data=b''):
    # Checksums are not verified by the decoder, so they are left zero
    return struct.pack('>BBH', icmp_type, code, 0) + rest + data


def echo(icmp_type, ident, seq):
    return icmp(icmp_type, 0, struct.pack('>HH', ident, seq), b'ping')


def port_unreachable():
    original = ipv4(17, udp(5000, 53, b'query'), '10.0.0.1', '8.8.8.8')
    return icmp(3, 3, bytes(4), original)


def neighbor_solicitation():
    return icmp(135, 0, bytes(4), socket.inet_pton(socket.AF_INET6, 'fe80::2'))


def packet_too_big():
    original = ipv6(17, udp(5000, 443, b'data'), '2001:db8::1', '2001:db8::2')
    return icmp(2, 0, struct.pack('>I', 1280), original)


def arp(operation, sender_mac, sender_ip, target_mac, target_ip):
    return struct.pack('>HHBBH6s4s6s4s', 1, 0x0800, 6, 4, operation, sender_mac, socket.inet_aton(sender_ip),
                       target_mac, socket.inet_aton(target_ip))


def icmp_pcapng():
    # Link type 101. Rows: echo request, echo reply, port unreachable, ICMPv6 neighbor solicitation,
    # ICMPv6 packet too big, a 3-byte message (not ICMP), a truncated echo request, a UDP packet
    f = shb() + idb(101)
    f += epb(0, ipv4(1, echo(8, 0x1234, 1), '10.0.0.1', '10.0.0.2'))
    f += epb(0, ipv4(1, echo(0, 0x1234, 1), '10.0.0.2', '10.0.0.1'))
    f += epb(0, ipv4(1, port_unreachable(), '8.8.8.8', '10.0.0.1'))
    f += epb(0, ipv6(58, neighbor_solicitation(), 'fe80::1', 'ff02::1:ff00:2'))
    f += epb(0, ipv6(58, packet_too_big(), '2001:db8::fe', '2001:db8::1'))
    f += epb(0, ipv4(1, b'\x08\x00\x00', '10.0.0.1', '10.0.0.2'))
    f += epb(0, ipv4(1, echo(8, 1, 2)[:6], '10.0.0.1', '10.0.0.2'))
    f += epb(0, ipv4(17, udp(4000, 7, b'hello'), '10.0.0.1', '10.0.0.2'))
    write(os.path.join(DECODERS, 'icmp', 'icmp.pcapng'), f)


def arp_pcapng():
    # Link type 1. Rows: request, reply, gratuitous, probe, lengths that do not fit (not ARP), truncated
    bad_lengths = bytearray(arp(1, MAC1, '10.0.0.1', bytes(6), '10.0.0.2'))
    bad_lengths[4] = 16
    f = shb() + idb(1)
    for message in (arp(1, MAC1, '10.0.0.1', bytes(6), '10.0.0.2'),
                    arp(2, MAC2, '10.0.0.2', MAC1, '10.0.0.1'),
                    arp(1, MAC1, '10.0.0.1', bytes(6), '10.0.0.1'),
                    arp(1, MAC1, '0.0.0.0', bytes(6), '10.0.0.9'),
                    bytes(bad_lengths),
                    arp(1, MAC1, '10.0.0.1', bytes(6), '10.0.0.2')[:20]):
        f += epb(0, ETHERNET_ARP + message)
    write(os.path.join(DECODERS, 'arp', 'arp.pcapng'), f)


def classic_pcap():
    # The classic reader decodes Ethernet only. Rows: ARP request padded to the Ethernet minimum,
    # echo request, truncated echo request, ARP with lengths that do not fit
    bad_lengths = bytearray(arp(1, MAC1, '10.0.0.1', bytes(6), '10.0.0.2'))
    bad_lengths[5] = 40
    frames = [ETHERNET_ARP + arp(1, MAC1, '10.0.0.1', bytes(6), '10.0.0.2') + bytes(18),
              ETHERNET + ipv4(1, echo(8, 0x1234, 1), '10.0.0.1', '10.0.0.2'),
              ETHERNET + ipv4(1, echo(8, 1, 2)[:6], '10.0.0.1', '10.0.0.2'),
              ETHERNET_ARP + bytes(bad_lengths)]
    pcap_file(os.path.join(DECODERS, 'icmp', 'icmp_arp.pcap'), frames)


if __name__ == '__main__':
    for name in ('icmp', 'arp'):
        os.makedirs(os.path.join(DECODERS, name), exist_ok=True)
    icmp_pcapng()
    arp_pcapng()
    classic_pcap()
