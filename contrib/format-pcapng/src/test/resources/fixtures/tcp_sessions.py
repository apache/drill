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
"""Builds complete TCP sessions for the session decoder fixtures (raw IP, link type 101)."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pcap_fixtures import TS, epb, idb, ipv4, shb, tcp, write  # noqa: E402

FIN, SYN, PSH, ACK = 0x01, 0x02, 0x08, 0x10
DECODERS = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'decoders')


def session(cport, sport, exchanges, client='10.0.0.1', server='10.0.0.2', close=True):
    """Handshake, then (from_client, bytes) segments with advancing sequence numbers, then FIN from each side."""
    cseq, sseq = 1000, 5000
    c = lambda flags, data=b'': ipv4(6, tcp(cport, sport, cseq, flags, data, sseq), client, server)
    s = lambda flags, data=b'': ipv4(6, tcp(sport, cport, sseq, flags, data, cseq), server, client)
    packets = [c(SYN)]
    packets.append(ipv4(6, tcp(sport, cport, sseq, SYN | ACK, b'', cseq + 1), server, client))
    cseq += 1
    sseq += 1
    packets.append(c(ACK))
    for from_client, data in exchanges:
        if from_client:
            packets.append(c(PSH | ACK, data))
            cseq += len(data)
        else:
            packets.append(s(PSH | ACK, data))
            sseq += len(data)
    if close:
        packets.append(c(FIN | ACK))
        cseq += 1
        packets.append(s(FIN | ACK))
        sseq += 1
        packets.append(c(ACK))
    return packets


def capture(name, sessions):
    """Writes the sessions one after another, one millisecond apart, to decoders/<dir>/<name>."""
    f = shb() + idb(101)
    i = 0
    for packets in sessions:
        for p in packets:
            f += epb(0, p, ts=TS + i * 1000)
            i += 1
    path = os.path.join(DECODERS, name)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    write(path, f)
