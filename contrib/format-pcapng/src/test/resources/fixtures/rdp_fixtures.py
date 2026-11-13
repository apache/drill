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
"""Generates decoders/rdp/rdp.pcapng. The X.224 connection exchange is built by hand (scapy has no stable
RDP negotiation layer). Sessions, by client port: 51001 a CR with an mstshash cookie requesting TLS+CredSSP
and a CC selecting TLS, 51002 HTTP on port 3389 (not RDP), 51003 a CR whose RDP Negotiation Request is cut
short (decode_error), 51004 a bare CR answered by a Negotiation Failure."""
import os
import struct
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from tcp_sessions import capture, session  # noqa: E402

C, S = True, False
X224_CR, X224_CC = 0xE0, 0xD0


def tpkt(code, user_data):
    """TPKT v3 + X.224 header (LI, code, DST-REF, SRC-REF, class) around user_data."""
    total = 4 + 7 + len(user_data)
    header = struct.pack('>BBH', 0x03, 0x00, total) + bytes([6 + len(user_data), code, 0, 0, 0, 0, 0])
    return header + user_data


def neg_req(protocols):
    return struct.pack('<BBHI', 0x01, 0x00, 8, protocols)


def neg_rsp(protocol):
    return struct.pack('<BBHI', 0x02, 0x00, 8, protocol)


def neg_failure(code):
    return struct.pack('<BBHI', 0x03, 0x00, 8, code)


def cookie(user):
    return b'Cookie: mstshash=' + user.encode() + b'\r\n'


def main():
    full = session(51001, 3389, [
        (C, tpkt(X224_CR, cookie('alice') + neg_req(0x03))),   # TLS | CredSSP
        (S, tpkt(X224_CC, neg_rsp(0x01))),                     # selected TLS
    ])
    http = session(51002, 3389, [(C, b'GET / HTTP/1.0\r\n\r\n'), (S, b'HTTP/1.0 200 OK\r\n\r\n')])
    truncated = session(51003, 3389, [
        (C, tpkt(X224_CR, cookie('bob') + b'\x01\x00\x08\x00')),  # neg req header with no protocol field
    ])
    failure = session(51004, 3389, [
        (C, tpkt(X224_CR, neg_req(0x00))),                     # standard RDP security
        (S, tpkt(X224_CC, neg_failure(0x05))),                 # HYBRID_REQUIRED_BY_SERVER
    ])
    capture(os.path.join('rdp', 'rdp.pcapng'), [full, http, truncated, failure])


if __name__ == '__main__':
    main()
