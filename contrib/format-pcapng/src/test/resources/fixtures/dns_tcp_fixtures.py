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
"""Generates decoders/dns_tcp/dns_tcp.pcapng, DNS over TCP. Needs scapy, which builds the DNS messages.
Run from any directory:
   python3 contrib/format-pcapng/src/test/resources/fixtures/dns_tcp_fixtures.py
Sessions, by client port: 40201 two pipelined queries in one connection, 40202 HTTP on port 53 (not DNS),
40203 a response cut short inside its answer (decode_error), 40204 an AXFR zone transfer of 80 records in
two response messages (answers capped at 64)."""
import os
import struct
import sys

from scapy.layers.dns import DNS, DNSQR, DNSRR

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from tcp_sessions import capture, session  # noqa: E402

C, S = True, False


def framed(message):
    data = bytes(message)
    return struct.pack('>H', len(data)) + data


def main():
    q1 = DNS(id=0x1001, rd=1, qd=DNSQR(qname='example.com', qtype='A'))
    q2 = DNS(id=0x1002, rd=1, qd=DNSQR(qname='example.org', qtype='AAAA'))
    r1 = DNS(id=0x1001, qr=1, rd=1, ra=1, qd=DNSQR(qname='example.com', qtype='A'),
             an=DNSRR(rrname='example.com', type='A', ttl=300, rdata='93.184.216.34'))
    r2 = DNS(id=0x1002, qr=1, rd=1, ra=1, qd=DNSQR(qname='example.org', qtype='AAAA'),
             an=DNSRR(rrname='example.org', type='AAAA', ttl=600, rdata='2606:2800:220:1::1'))
    pipelined = session(40201, 53, [(C, framed(q1) + framed(q2)), (S, framed(r1) + framed(r2))])

    http = session(40202, 53, [(C, b'GET / HTTP/1.0\r\n\r\n'), (S, b'HTTP/1.0 200 OK\r\n\r\n')])

    # Drop the last 2 bytes of the address but keep the length prefix consistent with what remains
    cut = bytes(r1)[:-2]
    truncated = session(40203, 53, [(C, framed(q1)), (S, struct.pack('>H', len(cut)) + cut)])

    axfr_q = DNS(id=0x2001, qd=DNSQR(qname='zone.test', qtype='AXFR'))
    responses = b''
    for part in range(2):
        records = [DNSRR(rrname='host%d.zone.test' % (part * 40 + i), type='A', ttl=3600,
                         rdata='10.1.%d.%d' % (part, i)) for i in range(40)]
        message = DNS(id=0x2001, qr=1, aa=1, qd=DNSQR(qname='zone.test', qtype='AXFR'), an=records)
        print('axfr message', part + 1, 'answers', DNS(bytes(message)).ancount)
        responses += framed(message)
    axfr = session(40204, 53, [(C, framed(axfr_q)), (S, responses)])

    # Cross-check: scapy reads back what the decoder must find
    for name, message in (('r1', r1), ('r2', r2)):
        parsed = DNS(bytes(message))
        print(name, parsed.an[0].rrname.decode(), parsed.an[0].rdata, parsed.an[0].ttl)
    print('truncated response length', len(cut), 'of', len(bytes(r1)))
    capture(os.path.join('dns_tcp', 'dns_tcp.pcapng'), [pipelined, http, truncated, axfr])


if __name__ == '__main__':
    main()
