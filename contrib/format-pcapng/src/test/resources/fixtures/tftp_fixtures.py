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
"""Generates decoders/tftp/tftp.pcapng. Run from any directory:
   python3 contrib/format-pcapng/src/test/resources/fixtures/tftp_fixtures.py"""
import os
import struct
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pcap_fixtures import shb, idb, epb, ipv4, udp, write  # noqa: E402

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'decoders', 'tftp')


def strings(*values):
    return b''.join(v.encode() + b'\0' for v in values)


def tftp():
    # Link type 101. Rows: RRQ with options, WRQ, ERROR from port 69, ACK between ephemeral ports (not
    # matched), not TFTP on port 69, RRQ whose last option value is unterminated
    rrq = struct.pack('>H', 1) + strings('boot/pxelinux.0', 'octet', 'blksize', '1428', 'tsize', '0')
    wrq = struct.pack('>H', 2) + strings('config.txt', 'NETASCII')
    error = struct.pack('>HH', 5, 1) + strings('File not found')
    ack = struct.pack('>HH', 4, 1)
    f = shb() + idb(101)
    f += epb(0, ipv4(17, udp(50000, 69, rrq), '10.0.0.1', '10.0.0.2'))
    f += epb(0, ipv4(17, udp(50001, 69, wrq), '10.0.0.1', '10.0.0.2'))
    f += epb(0, ipv4(17, udp(69, 50002, error), '10.0.0.2', '10.0.0.1'))
    f += epb(0, ipv4(17, udp(50000, 41000, ack), '10.0.0.1', '10.0.0.2'))
    f += epb(0, ipv4(17, udp(50003, 69, b'\x00\x01not tftp at all'), '10.0.0.1', '10.0.0.2'))
    f += epb(0, ipv4(17, udp(50004, 69, rrq[:-1]), '10.0.0.1', '10.0.0.2'))
    os.makedirs(OUT, exist_ok=True)
    write(os.path.join(OUT, 'tftp.pcapng'), f)


if __name__ == '__main__':
    tftp()
