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
"""Generates decoders/syslog/syslog.pcapng. Run from any directory:
   python3 contrib/format-pcapng/src/test/resources/fixtures/syslog_fixtures.py"""
import os
import struct
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pcap_fixtures import shb, idb, epb, ipv4, udp, tcp, write  # noqa: E402

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'decoders', 'syslog')

MESSAGES = [
    b'<165>1 2003-10-11T22:14:15.003Z mymachine.example.com evntslog - ID47 '
    b'[exampleSDID@32473 iut="3" eventSource="Application"] An application event',
    b'<38>Jan  2 03:04:05 gateway sshd[1234]: Accepted password for root from 10.0.0.9',
    # Not syslog on port 514
    b'hello world',
    # RFC 5424 header cut short
    b'<14>1 2024-01-02T03:04:05Z host',
]


def main():
    os.makedirs(OUT, exist_ok=True)
    f = shb() + idb(101)
    for data in MESSAGES:
        f += epb(0, ipv4(17, udp(40000, 514, data), '10.0.0.1', '10.0.0.2'))
    write(os.path.join(OUT, 'syslog.pcapng'), f)


if __name__ == '__main__':
    main()
