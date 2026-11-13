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
"""Generates decoders/sip/sip.pcapng. Run from any directory:
   python3 contrib/format-pcapng/src/test/resources/fixtures/sip_fixtures.py"""
import os
import struct
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pcap_fixtures import shb, idb, epb, ipv4, udp, tcp, write  # noqa: E402

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'decoders', 'sip')

INVITE = (b'INVITE sip:bob@biloxi.example.com SIP/2.0\r\n'
          b'Via: SIP/2.0/UDP pc33.atlanta.example.com;branch=z9hG4bK776asdhds\r\n'
          b'Max-Forwards: 70\r\n'
          b'To: Bob <sip:bob@biloxi.example.com>\r\n'
          b'From: Alice <sip:alice@atlanta.example.com>;tag=1928301774\r\n'
          b'Call-ID: a84b4c76e66710@pc33.atlanta.example.com\r\n'
          b'CSeq: 314159 INVITE\r\n'
          b'Contact: <sip:alice@pc33.atlanta.example.com>\r\n'
          b'Authorization: Digest username="alice", realm="atlanta.example.com", nonce="84a4cc6f", '
          b'response="7587245234b3434cc3412213e5f113a5"\r\n'
          b'Content-Type: application/sdp\r\n'
          b'Content-Length: 4\r\n'
          b'\r\n'
          b'v=0\n')
RINGING = (b'SIP/2.0 180 Ringing\r\n'
           b'v: SIP/2.0/TCP pc33.atlanta.example.com;branch=z9hG4bK776asdhds\r\n'
           b'f: Alice <sip:alice@atlanta.example.com>;tag=1928301774\r\n'
           b't: Bob <sip:bob@biloxi.example.com>;tag=a6c85cf\r\n'
           b'i: a84b4c76e66710@pc33.atlanta.example.com\r\n'
           b'CSeq: 314159 INVITE\r\n'
           b'l: 0\r\n'
           b'\r\n')


def main():
    os.makedirs(OUT, exist_ok=True)
    f = shb() + idb(101)
    f += epb(0, ipv4(17, udp(5060, 5060, INVITE), '10.0.0.1', '10.0.0.2'))
    f += epb(0, ipv4(6, tcp(5060, 40000, 1000, 0x18, RINGING), '10.0.0.2', '10.0.0.1'))
    # Not SIP on port 5060: a keep-alive
    f += epb(0, ipv4(17, udp(5060, 5060, b'\r\n\r\n'), '10.0.0.1', '10.0.0.2'))
    # Malformed header line
    f += epb(0, ipv4(17, udp(5060, 5060, b'OPTIONS sip:x@y SIP/2.0\r\nVia: SIP/2.0/UDP h\r\ngarbage\r\n\r\n'),
                     '10.0.0.1', '10.0.0.2'))
    write(os.path.join(OUT, 'sip.pcapng'), f)


if __name__ == '__main__':
    main()
