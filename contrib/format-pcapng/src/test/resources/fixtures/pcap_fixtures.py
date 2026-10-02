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
"""Generates the synthetic PCAP and PCAP-NG test fixtures. Run from any directory:
   python3 contrib/format-pcapng/src/test/resources/fixtures/pcap_fixtures.py"""
import os
import socket
import struct

HERE = os.path.dirname(os.path.abspath(__file__))
PCAPNG = os.path.join(HERE, '..', 'pcapng')
PCAP = os.path.join(HERE, '..', 'pcap')
TS = 1704164645678000  # 2024-01-02T03:04:05.678Z in microseconds


def pad(b):
    return b + b'\0' * (-len(b) % 4)


def opt(code, val, e='<'):
    return struct.pack(e + 'HH', code, len(val)) + pad(val)


def opts(*o, e='<'):
    return b''.join(o) + (struct.pack(e + 'HH', 0, 0) if o else b'')


def block(t, body, e='<'):
    length = 12 + len(body)
    return struct.pack(e + 'II', t, length) + body + struct.pack(e + 'I', length)


def shb(e='<'):
    return block(0x0A0D0D0A, struct.pack(e + 'IHHq', 0x1A2B3C4D, 1, 0, -1), e)


def idb(link_type, *options, e='<'):
    return block(1, struct.pack(e + 'HHI', link_type, 0, 65535) + opts(*options, e=e), e)


def epb(interface, data, ts=TS, e='<'):
    return block(6, struct.pack(e + 'IIIII', interface, ts >> 32, ts & 0xffffffff, len(data), len(data)) + pad(data), e)


def ipv4(proto, payload, src, dst, ihl=5):
    header = struct.pack('>BBHHHBBH4s4s', 0x40 | ihl, 0, 20 + len(payload), 0, 0, 64, proto, 0,
                         socket.inet_aton(src), socket.inet_aton(dst))
    return header + payload


def udp(sport, dport, data):
    return struct.pack('>HHHH', sport, dport, 8 + len(data), 0) + data


def tcp(sport, dport, seq, flags, data, ack=0):
    return struct.pack('>HHIIBBHHH', sport, dport, seq, ack, 5 << 4, flags, 65535, 0, 0) + data


def write(path, data):
    with open(path, 'wb') as f:
        f.write(data)


def echo():
    # Link type 101 (raw IP). Rows: parsed, not echo, parse failure, write failure, long text warning
    f = shb() + idb(101)
    for text in (b'ECHO:hello', b'hello', b'ECHO!bad', b'ECHO:write-fail', b'ECHO:a long echo text'):
        f += epb(0, ipv4(17, udp(4000, 7, text), '10.0.0.1', '10.0.0.2'))
    write(os.path.join(PCAPNG, 'echo.pcapng'), f)


def errors():
    # Packet errors that must not stop the file; a valid packet follows each one
    good = ipv4(17, udp(4000, 7, b'ECHO:ok'), '10.0.0.1', '10.0.0.2')
    f = shb() + idb(101)
    f += idb(101, opt(9, bytes([0x7F])))                               # interface 1: unsupported if_tsresol (10^-127)
    f += epb(0, ipv4(17, udp(4000, 7, b'x'), '10.0.0.1', '10.0.0.2', ihl=3))  # malformed IPv4 header
    f += epb(0, good)
    f += epb(5, good)                                                  # undefined interface
    f += epb(1, good)                                                  # interface with bad if_tsresol
    print('errors.pcapng: bad captured length block at byte', len(f))
    bad = epb(0, good)
    bad = bad[:20] + struct.pack('<I', 9999) + bad[24:]                # captured length larger than the block
    f += bad
    f += epb(0, good)
    write(os.path.join(PCAPNG, 'errors.pcapng'), f)


def bad_block_length():
    good = ipv4(17, udp(4000, 7, b'ECHO:ok'), '10.0.0.1', '10.0.0.2')
    f = shb() + idb(101) + epb(0, good) + epb(0, good)
    print('bad_block_length.pcapng: bad block at byte', len(f))
    f += struct.pack('<II', 6, 7)                                       # block length 7: not a multiple of 4
    f += epb(0, good)
    write(os.path.join(PCAPNG, 'bad_block_length.pcapng'), f)


if __name__ == '__main__':
    echo()
    errors()
    bad_block_length()
