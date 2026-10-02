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
"""Generates decoders/tls/tls.pcapng. Run from any directory:
   python3 contrib/format-pcapng/src/test/resources/fixtures/tls_fixtures.py
If scapy is installed, the hellos are cross-checked with scapy's TLS layer and an independent JA3."""
import hashlib
import os
import struct
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from pcap_fixtures import shb, idb, epb, ipv4, tcp, write  # noqa: E402

OUT = os.path.join(HERE, '..', 'decoders', 'tls')
GREASE = {0x0a0a + 0x1010 * i for i in range(16)}


def u16s(*values):
    return b''.join(struct.pack('>H', v) for v in values)


def ext(t, data):
    return struct.pack('>HH', t, len(data)) + data


def sni(host):
    name = host.encode()
    return ext(0, struct.pack('>HBH', len(name) + 3, 0, len(name)) + name)


def alpn(*protocols):
    body = b''.join(bytes([len(p)]) + p.encode() for p in protocols)
    return ext(16, struct.pack('>H', len(body)) + body)


def u16_list(t, *values):
    return ext(t, struct.pack('>H', 2 * len(values)) + u16s(*values))


def handshake(t, body):
    return bytes([t]) + len(body).to_bytes(3, 'big') + body


def record(version, fragment):
    return struct.pack('>BHH', 22, version, len(fragment)) + fragment


def client_hello_body(version, session_id, ciphers, extensions):
    exts = b''.join(extensions)
    return (struct.pack('>H', version) + bytes(range(32)) + bytes([len(session_id)]) + session_id
            + struct.pack('>H', 2 * len(ciphers)) + u16s(*ciphers) + b'\x01\x00'
            + struct.pack('>H', len(exts)) + exts)


def server_hello(version, session_id, cipher, extensions):
    exts = b''.join(extensions)
    body = (struct.pack('>H', version) + bytes(range(32, 64)) + bytes([len(session_id)]) + session_id
            + struct.pack('>HB', cipher, 0) + struct.pack('>H', len(exts)) + exts)
    return record(0x0303, handshake(2, body))


CIPHERS = [0x4a4a, 0x1301, 0x1302, 0x1303, 0xc02b, 0xc02f, 0xc02c, 0xc030, 0xcca9, 0xcca8, 0xc013, 0xc014,
           0x009c, 0x009d, 0x002f, 0x0035]
CLIENT_EXTENSIONS = [ext(0x2a2a, b''), sni('www.example.com'), ext(23, b''), ext(65281, b'\x00'),
                     u16_list(10, 0x8a8a, 29, 23, 24), ext(11, b'\x01\x00'), ext(35, b''),
                     alpn('h2', 'http/1.1'), ext(5, b'\x01\x00\x00\x00\x00'),
                     u16_list(13, 0x0403, 0x0804, 0x0401, 0x0503), ext(51, u16s(4, 0x8a8a, 0) + b''),
                     ext(45, b'\x01\x01'), ext(43, b'\x06' + u16s(0xbaba, 0x0304, 0x0303)), ext(0x3a3a, b'\x00')]
SESSION_ID = bytes(range(0xe0, 0x100))
CLIENT_HELLO = record(0x0301, handshake(1, client_hello_body(0x0303, SESSION_ID, CIPHERS, CLIENT_EXTENSIONS)))
SERVER_HELLO = server_hello(0x0303, SESSION_ID, 0x1301, [ext(43, u16s(0x0304)), ext(51, u16s(29, 32) + bytes(range(64, 96)))])
EXPECTED_JA3 = ('771,4865-4866-4867-49195-49199-49196-49200-52393-52392-49171-49172-156-157-47-53,'
                '0-23-65281-10-11-35-16-5-13-51-45-43,29-23-24,0')
EXPECTED_JA3S = '771,4865,43-51'


def ja3_from_scapy(hello):
    """JA3 computed from scapy's parse, following the JA3 README: GREASE removed from ciphers, extensions and
    groups; fields joined with commas, list items with dashes."""
    from scapy.layers.tls.extensions import TLS_Ext_SupportedGroups, TLS_Ext_SupportedPointFormat
    exts = hello.ext or []
    groups, formats = [], []
    for e in exts:
        if isinstance(e, TLS_Ext_SupportedGroups):
            groups = e.groups
        if isinstance(e, TLS_Ext_SupportedPointFormat):
            formats = e.ecpl
    keep = lambda values: '-'.join(str(v) for v in values if v not in GREASE)  # noqa: E731
    return ','.join([str(hello.version), keep(hello.ciphers), keep(e.type for e in exts), keep(groups),
                     '-'.join(str(v) for v in formats)])


def ja4_from_spec(legacy_version, supported_versions, has_sni, ciphers, extensions, sig_algs, alpn):
    """JA4 (FoxIO, BSD 3-Clause) computed independently from the spec, for cross-checking the Java decoder.
    Only JA4 is implemented; the JA4S/JA4H/JA4SSH variants have an incompatible licence."""
    def h4(v):
        return '%04x' % (v & 0xFFFF)

    def sha12(s):
        return hashlib.sha256(s.encode('ascii')).hexdigest()[:12]

    ver_code = {0x0304: '13', 0x0303: '12', 0x0302: '11', 0x0301: '10', 0x0300: 's3'}
    sv = [v for v in supported_versions if v not in GREASE]
    version = max(sv) if sv else legacy_version
    alpn_chars = '00'
    if alpn and alpn[0]:
        b = alpn[0].encode()
        fc, lc = chr(b[0]), chr(b[-1])
        alpn_chars = fc + lc if fc.isalnum() and lc.isalnum() else '%x%x' % ((b[0] >> 4) & 0xF, b[-1] & 0xF)
    cc = min(99, sum(1 for c in ciphers if c not in GREASE))
    ec = min(99, sum(1 for e in extensions if e not in GREASE))
    a = '%s%s%s%02d%02d%s' % ('t', ver_code.get(version, '00'), 'd' if has_sni else 'i', cc, ec, alpn_chars)
    chex = sorted(h4(c) for c in ciphers if c not in GREASE)
    b_hash = '000000000000' if not chex else sha12(','.join(chex))
    ehex = sorted(h4(e) for e in extensions if e not in GREASE and e not in (0x0000, 0x0010))
    shex = [h4(s) for s in sig_algs if s not in GREASE]
    c_raw = ','.join(ehex) + (('_' + ','.join(shex)) if shex else '')
    c_hash = '000000000000' if not ehex else sha12(c_raw)
    return '%s_%s_%s' % (a, b_hash, c_hash)


# Extension types, signature algorithms and supported_versions of the fixture ClientHello above, in wire order.
CLIENT_EXTENSION_TYPES = [0x2a2a, 0, 23, 65281, 10, 11, 35, 16, 5, 13, 51, 45, 43, 0x3a3a]
CLIENT_SIG_ALGS = [0x0403, 0x0804, 0x0401, 0x0503]
CLIENT_SUPPORTED_VERSIONS = [0xbaba, 0x0304, 0x0303]
EXPECTED_JA4 = ja4_from_spec(0x0303, CLIENT_SUPPORTED_VERSIONS, True, CIPHERS, CLIENT_EXTENSION_TYPES,
                             CLIENT_SIG_ALGS, ['h2', 'http/1.1'])


def cross_check():
    try:
        from scapy.all import load_layer
        load_layer('tls')
        from scapy.layers.tls.record import TLS
        from scapy.layers.tls.extensions import TLS_Ext_ServerName, TLS_Ext_ALPN
        from scapy.layers.tls.handshake import TLSServerHello
    except ImportError:
        print('scapy not installed; skipping the cross-check')
        return
    client = TLS(CLIENT_HELLO).msg[0]
    assert client.msgtype == 1
    names = [e for e in client.ext if isinstance(e, TLS_Ext_ServerName)]
    assert names[0].servernames[0].servername == b'www.example.com'
    protocols = [e for e in client.ext if isinstance(e, TLS_Ext_ALPN)][0].protocols
    assert [p.protocol for p in protocols] == [b'h2', b'http/1.1']
    assert client.sid == SESSION_ID
    ja3 = ja3_from_scapy(client)
    assert ja3 == EXPECTED_JA3, ja3
    # Without a session, scapy's TLS layer does not decode a TLS 1.3 ServerHello, so parse the message alone
    server = TLSServerHello(SERVER_HELLO[5:])
    assert server.msgtype == 2 and server.cipher == 0x1301
    ja3s = ','.join([str(server.version), str(server.cipher), '-'.join(str(e.type) for e in server.ext)])
    assert ja3s == EXPECTED_JA3S, ja3s
    print('scapy cross-check passed')


def main():
    cross_check()
    print('ja3', EXPECTED_JA3, hashlib.md5(EXPECTED_JA3.encode()).hexdigest())
    print('ja3s', EXPECTED_JA3S, hashlib.md5(EXPECTED_JA3S.encode()).hexdigest())
    print('ja4', EXPECTED_JA4)
    client, server = '10.0.0.1', '93.184.216.34'
    # A hello whose lengths are complete but whose session ID length (32) overruns the hello
    bad = bytearray(client_hello_body(0x0303, b'', [0x1301], []))
    bad[34] = 32
    cut = record(0x0301, handshake(1, bytes(bad)))
    # A hello larger than one segment: the first segment ends inside the extensions
    big = record(0x0301, handshake(1, client_hello_body(
        0x0303, SESSION_ID, CIPHERS, CLIENT_EXTENSIONS[:2] + [ext(21, bytes(1400))])))
    f = shb() + idb(101)
    rows = [
        (client, server, 50000, 443, CLIENT_HELLO),                       # ClientHello
        (server, client, 443, 50000, SERVER_HELLO),                       # ServerHello
        (client, server, 50001, 443, b'GET / HTTP/1.1\r\nHost: x\r\n\r\n'),  # not TLS on 443
        (client, server, 50002, 993, cut),                                # malformed hello
        (client, server, 50003, 8443, big[:1000]),                        # hello continues later
        (client, server, 50003, 8443, big[1000:]),                        # the continuation: not a hello
    ]
    for src, dst, sport, dport, data in rows:
        f += epb(0, ipv4(6, tcp(sport, dport, 1, 0x18, data), src, dst))
    os.makedirs(OUT, exist_ok=True)
    write(os.path.join(OUT, 'tls.pcapng'), f)


if __name__ == '__main__':
    main()
