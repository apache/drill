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


ETHERNET = bytes.fromhex('020000000002' '020000000001' '0800')


def pcap_file(path, frames, link_type=1):
    data = struct.pack('<IHHiIII', 0xa1b2c3d4, 2, 4, 0, 0, 65535, link_type)
    for i, frame in enumerate(frames):
        data += struct.pack('<IIII', 1704164645 + i, 0, len(frame), len(frame)) + frame
    write(path, data)


def classic():
    # The classic reader decodes Ethernet only, so these frames carry an Ethernet header
    frames = [ETHERNET + ipv4(17, udp(4000, 7, t), '10.0.0.1', '10.0.0.2')
              for t in (b'ECHO:hello', b'hello', b'ECHO!bad')]
    pcap_file(os.path.join(PCAP, 'echo.pcap'), frames)
    # Second record claims 70000 bytes, more than the 65535 snapshot length
    good = ETHERNET + ipv4(17, udp(4000, 7, b'ECHO:ok'), '10.0.0.1', '10.0.0.2')
    data = struct.pack('<IHHiIII', 0xa1b2c3d4, 2, 4, 0, 0, 65535, 1)
    data += struct.pack('<IIII', 1, 0, len(good), len(good)) + good
    data += struct.pack('<IIII', 2, 0, 70000, 70000) + good
    write(os.path.join(PCAP, 'bad_record.pcap'), data)
    # A Git LFS pointer saved in place of a capture
    with open(os.path.join(PCAP, 'lfs_pointer.pcap'), 'w') as f:
        f.write('version https://git-lfs.github.com/spec/v1\n'
                'oid sha256:8d48391c3dde43c518b22c3d6ba4bbba315efac713803cc582d00e37144f577a\n'
                'size 867464224\n')

def dns_name(dotted):
    out = b''
    for label in dotted.split('.'):
        out += bytes([len(label)]) + label.encode()
    return out + b'\0'


def dns_message(ident, flags, questions, answers):
    body = b''.join(dns_name(n) + struct.pack('>HH', t, 1) for n, t in questions)
    for ttl, address in answers:
        # Name is a compression pointer to the first question name at offset 12
        body += b'\xc0\x0c' + struct.pack('>HHIH', 1, 1, ttl, len(address)) + address
    return struct.pack('>HHHHHH', ident, flags, len(questions), len(answers), 0, 0) + body


def dns():
    # Link type 101. Rows: query, response, non-DNS bytes on port 53, response cut 3 bytes short
    query = dns_message(0x1234, 0x0100, [('example.com', 1)], [])
    response = dns_message(0x1234, 0x8180, [('example.com', 1)], [(300, bytes([93, 184, 216, 34]))])
    f = shb() + idb(101)
    f += epb(0, ipv4(17, udp(5000, 53, query), '10.0.0.1', '8.8.8.8'))
    f += epb(0, ipv4(17, udp(53, 5000, response), '8.8.8.8', '10.0.0.1'))
    f += epb(0, ipv4(17, udp(5000, 53, bytes(range(1, 14))), '10.0.0.1', '8.8.8.8'))
    f += epb(0, ipv4(17, udp(53, 5000, response[:-3]), '8.8.8.8', '10.0.0.1'))
    write(os.path.join(PCAPNG, 'dns.pcapng'), f)

def oversized():
    # A block length near 2 GB must be reported, not allocated
    good = ipv4(17, udp(4000, 7, b'ECHO:ok'), '10.0.0.1', '10.0.0.2')
    f = shb() + idb(101) + epb(0, good)
    print('huge_block.pcapng: huge block at byte', len(f))
    f += struct.pack('<II', 6, 0x7FFFFFF0) + bytes(16)
    write(os.path.join(PCAPNG, 'huge_block.pcapng'), f)
    # A snapshot length near 2 GB in a capture whose packets are normal
    frame = ETHERNET + good
    data = struct.pack('<IHHiIII', 0xa1b2c3d4, 2, 4, 0, 0, 0x7FFFFFF0, 1)
    for i in range(2):
        data += struct.pack('<IIII', i, 0, len(frame), len(frame)) + frame
    write(os.path.join(PCAP, 'huge_snaplen.pcap'), data)
    # A malformed IPv4 header (IHL 3) between two good packets
    frames = [ETHERNET + ipv4(17, udp(4000, 7, b'ECHO:ok'), '10.0.0.1', '10.0.0.2', ihl=ihl) for ihl in (5, 3, 5)]
    pcap_file(os.path.join(PCAP, 'malformed.pcap'), frames)

def compressed():
    # Valid and corrupted gzip copies of real captures; mtime=0 keeps the output stable
    import gzip
    for name, folder in (('http.pcap', PCAP), ('sniff.pcapng', PCAPNG)):
        data = gzip.compress(open(os.path.join(folder, name), 'rb').read(), mtime=0)
        if name == 'http.pcap':
            write(os.path.join(folder, name + '.gz'), data)
        # Flip bytes in the middle of the compressed stream: decompression fails there
        corrupt = bytearray(data)
        middle = len(corrupt) // 2
        for i in range(middle, middle + 40):
            corrupt[i] ^= 0xFF
        stem = name.split('.')[0]
        write(os.path.join(folder, stem + '_corrupt.' + name.split('.')[1] + '.gz'), bytes(corrupt))

if __name__ == '__main__':
    echo()
    errors()
    bad_block_length()
    classic()
    dns()
    oversized()
    compressed()
