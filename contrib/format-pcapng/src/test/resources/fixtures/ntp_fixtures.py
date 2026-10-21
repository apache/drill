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
"""Generates decoders/ntp/ntp.pcapng. Run from any directory:
   python3 contrib/format-pcapng/src/test/resources/fixtures/ntp_fixtures.py"""
import os
import struct
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pcap_fixtures import shb, idb, epb, ipv4, udp, tcp, write  # noqa: E402

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'decoders', 'ntp')

NTP_UNIX_OFFSET = 2208988800


def ts(unix, fraction=0x80000000):
    return struct.pack('>II', unix + NTP_UNIX_OFFSET, fraction) if unix else bytes(8)


def header(first, stratum, poll, precision, root_delay, root_disp, ref_id, ref, orig, recv, xmit):
    return (struct.pack('>BBbbII4s', first, stratum, poll, precision, root_delay, root_disp, ref_id)
            + ts(ref) + ts(orig) + ts(recv) + ts(xmit))


def main():
    os.makedirs(OUT, exist_ok=True)
    payloads = [
        # Client request: LI 0, version 4, mode 3
        (40000, 123, header(0x23, 0, 6, -20, 0, 0, bytes(4), 0, 0, 0, 1704164645)),
        # Server response: stratum 2, reference 192.168.1.1
        (123, 40000, header(0x24, 2, 6, -20, 0x0800, 0x10000, bytes([192, 168, 1, 1]),
                            1704164000, 1704164645, 1704164645, 1704164646)),
        # Not NTP (version 0) on port 123
        (40000, 123, b'GET / HTTP/1.1\r\nHost: example.com\r\nAccept: */*\r\n\r\n'),
        # ntpdc monlist request: version 2, mode 7, implementation 3, request code 42
        (40001, 123, struct.pack('>BBBBHH', 0x17, 0, 3, 42, 0, 0) + bytes(40)),
        # Control message (mode 6) whose count runs past the datagram
        (40002, 123, struct.pack('>BBHHHHH', 0x16, 2, 1, 0, 0, 0, 100)),
    ]
    f = shb() + idb(101)
    for sport, dport, data in payloads:
        f += epb(0, ipv4(17, udp(sport, dport, data), '10.0.0.1', '10.0.0.2'))
    write(os.path.join(OUT, 'ntp.pcapng'), f)


if __name__ == '__main__':
    main()
