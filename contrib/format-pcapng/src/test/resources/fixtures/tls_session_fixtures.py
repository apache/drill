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
"""Generates the TLS session decoder fixtures: decoders/tls/tls_sessions.pcapng and the DER certificates
decoders/tls/leaf.der and decoders/tls/ca.der used by the unit tests.

Needs the `cryptography` package. Keys are derived from fixed seeds and the signatures used are
deterministic (RSA PKCS#1 v1.5), so the output is the same on every run. Run from any directory:
   python3 contrib/format-pcapng/src/test/resources/fixtures/tls_session_fixtures.py"""
import datetime
import ipaddress
import os
import random
import struct
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from pcap_fixtures import shb, idb, epb, ipv4, tcp, write, TS  # noqa: E402

from cryptography import x509  # noqa: E402
from cryptography.hazmat.primitives import hashes, serialization  # noqa: E402
from cryptography.hazmat.primitives.asymmetric import ec, rsa  # noqa: E402
from cryptography.x509.oid import NameOID  # noqa: E402

OUT = os.path.join(HERE, '..', 'decoders', 'tls')


# ---------------------------------------------------------------- certificates

def is_probable_prime(n, rng):
    if n < 2:
        return False
    for p in (2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37):
        if n % p == 0:
            return n == p
    d, s = n - 1, 0
    while d % 2 == 0:
        d //= 2
        s += 1
    for _ in range(40):
        x = pow(rng.randrange(2, n - 1), d, n)
        if x in (1, n - 1):
            continue
        for _ in range(s - 1):
            x = pow(x, 2, n)
            if x == n - 1:
                break
        else:
            return False
    return True


def deterministic_rsa_key(seed, bits=2048):
    rng = random.Random(seed)
    e = 65537

    def prime():
        while True:
            c = rng.getrandbits(bits // 2) | (3 << (bits // 2 - 2)) | 1
            if (c - 1) % e != 0 and is_probable_prime(c, rng):
                return c
    p, q = prime(), prime()
    n = p * q
    d = pow(e, -1, (p - 1) * (q - 1))
    numbers = rsa.RSAPrivateNumbers(p, q, d, rsa.rsa_crt_dmp1(d, p), rsa.rsa_crt_dmq1(d, q),
                                    rsa.rsa_crt_iqmp(p, q), rsa.RSAPublicNumbers(e, n))
    return numbers.private_key()


def name(cn, org):
    return x509.Name([x509.NameAttribute(NameOID.ORGANIZATION_NAME, org), x509.NameAttribute(NameOID.COMMON_NAME, cn)])


def certificates():
    ca_key = deterministic_rsa_key('drill-test-ca')
    leaf_key = ec.derive_private_key(0x5EED0F7E57C3A7, ec.SECP256R1())
    ca_name = name('Drill Test Root CA', 'Apache Drill')
    utc = datetime.timezone.utc
    ca = (x509.CertificateBuilder()
          .subject_name(ca_name).issuer_name(ca_name)
          .public_key(ca_key.public_key())
          .serial_number(0x1001)
          .not_valid_before(datetime.datetime(2024, 1, 1, tzinfo=utc))
          .not_valid_after(datetime.datetime(2034, 1, 1, tzinfo=utc))
          .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
          .sign(ca_key, hashes.SHA256()))
    leaf = (x509.CertificateBuilder()
            .subject_name(name('www.example.com', 'Example')).issuer_name(ca_name)
            .public_key(leaf_key.public_key())
            .serial_number(0x0A1B2C3D4E5F)
            .not_valid_before(datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=utc))
            .not_valid_after(datetime.datetime(2025, 1, 2, 3, 4, 5, tzinfo=utc))
            .add_extension(x509.SubjectAlternativeName([
                x509.DNSName('www.example.com'), x509.DNSName('example.com'),
                x509.IPAddress(ipaddress.ip_address('93.184.216.34'))]), critical=False)
            .sign(ca_key, hashes.SHA256()))
    return leaf.public_bytes(serialization.Encoding.DER), ca.public_bytes(serialization.Encoding.DER)


# ---------------------------------------------------------------- TLS messages

def u8(b):
    return bytes([len(b)]) + b


def u16(b):
    return struct.pack('>H', len(b)) + b


def u24(b):
    return struct.pack('>I', len(b))[1:] + b


def ext(t, data):
    return struct.pack('>H', t) + u16(data)


def sni(host):
    return ext(0, u16(b'\x00' + u16(host.encode())))


def alpn(*protocols):
    return ext(16, u16(b''.join(u8(p.encode()) for p in protocols)))


def handshake(t, body):
    return bytes([t]) + u24(body)


def record(content_type, payload, version=0x0303):
    return struct.pack('>BH', content_type, version) + u16(payload)


def client_hello(sid, suites, extensions, version=0x0303):
    body = (struct.pack('>H', version) + bytes(range(32)) + u8(sid)
            + u16(b''.join(struct.pack('>H', s) for s in suites)) + u8(b'\x00') + u16(b''.join(extensions)))
    return handshake(1, body)


def server_hello(sid, suite, extensions, version=0x0303):
    body = (struct.pack('>H', version) + bytes(range(32, 64)) + u8(sid) + struct.pack('>H', suite) + b'\x00'
            + u16(b''.join(extensions)))
    return handshake(2, body)


def certificate(*ders):
    return handshake(11, u24(b''.join(u24(d) for d in ders)))


# ---------------------------------------------------------------- TCP sessions

class Session:
    """Writes a TCP session with a handshake, data segments and a FIN from each side."""

    def __init__(self, sport, dport, client='10.0.0.1', server='10.0.0.2'):
        self.sport, self.dport, self.client_ip, self.server_ip = sport, dport, client, server
        self.cseq, self.sseq = 1000, 5000
        self.blocks = []
        self.ts = TS + sport * 1000000

    def _emit(self, from_client, flags, data=b''):
        self.ts += 1000
        if from_client:
            seg = tcp(self.sport, self.dport, self.cseq, flags, data, self.sseq)
            self.blocks.append(epb(0, ipv4(6, seg, self.client_ip, self.server_ip), ts=self.ts))
            self.cseq += len(data) + (1 if flags & 0x03 else 0)
        else:
            seg = tcp(self.dport, self.sport, self.sseq, flags, data, self.cseq)
            self.blocks.append(epb(0, ipv4(6, seg, self.server_ip, self.client_ip), ts=self.ts))
            self.sseq += len(data) + (1 if flags & 0x03 else 0)

    def open(self):
        self._emit(True, 0x02)
        self._emit(False, 0x12)
        self._emit(True, 0x10)
        return self

    def client(self, data):
        self._emit(True, 0x18, data)
        return self

    def server(self, data):
        self._emit(False, 0x18, data)
        return self

    def close(self):
        self._emit(True, 0x11)
        self._emit(False, 0x11)
        self._emit(True, 0x10)
        return b''.join(self.blocks)


def tls12_session(leaf, ca):
    hello = client_hello(b'', [0xC02F, 0xC030, 0x009C],
                         [sni('www.example.com'), alpn('h2', 'http/1.1')])
    server_sid = bytes(range(100, 132))
    shello = server_hello(server_sid, 0xC02F, [alpn('h2')])
    cert = certificate(leaf, ca)
    done = handshake(14, b'')
    # The Certificate message is fragmented across two records, and the first record is split across
    # two TCP segments
    half = len(cert) // 2
    rec1 = record(22, cert[:half])
    rec2 = record(22, cert[half:] + done)
    s = Session(50001, 443).open()
    s.client(record(22, hello, version=0x0301))
    s.server(record(22, shello) + rec1[:300])
    s.server(rec1[300:] + rec2)
    s.client(record(22, handshake(16, u8(bytes(65)))) + record(20, b'\x01') + record(22, bytes(40)))
    s.server(record(20, b'\x01') + record(22, bytes(40)))
    s.client(record(23, bytes(64)))
    s.server(record(23, bytes(128)))
    return s.close()


def tls13_session():
    sid = bytes(range(200, 232))
    hello = client_hello(sid, [0x1301, 0x1302, 0x1303, 0xC02B],
                         [sni('tls13.example.org'), alpn('h2'),
                          ext(43, u8(struct.pack('>HHH', 0x0A0A, 0x0304, 0x0303))),
                          ext(51, u16(struct.pack('>H', 29) + u16(bytes(32))))])
    shello = server_hello(sid, 0x1301, [ext(43, struct.pack('>H', 0x0304)),
                                        ext(51, struct.pack('>H', 29) + u16(bytes(32)))])
    s = Session(50002, 443).open()
    s.client(record(22, hello, version=0x0301))
    # EncryptedExtensions, Certificate, CertificateVerify and Finished travel as application data
    s.server(record(22, shello) + record(20, b'\x01') + record(23, bytes(900)))
    s.client(record(20, b'\x01') + record(23, bytes(53)))
    return s.close()


def not_tls_session():
    s = Session(50003, 443).open()
    s.client(b'GET / HTTP/1.1\r\nHost: example.com\r\n\r\n')
    s.server(b'HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n')
    return s.close()


def malformed_session():
    hello = bytearray(client_hello(b'', [0xC02F], [sni('bad.example.com')]))
    # The extensions length (the last 2-byte length before the SNI extension) claims 0x0400 bytes
    ext_len_at = len(hello) - len(sni('bad.example.com')) - 2
    hello[ext_len_at:ext_len_at + 2] = struct.pack('>H', 0x0400)
    s = Session(50004, 8443).open()
    s.client(record(22, bytes(hello)))
    return s.close()


if __name__ == '__main__':
    os.makedirs(OUT, exist_ok=True)
    leaf_der, ca_der = certificates()
    write(os.path.join(OUT, 'leaf.der'), leaf_der)
    write(os.path.join(OUT, 'ca.der'), ca_der)
    f = shb() + idb(101) + tls12_session(leaf_der, ca_der) + tls13_session() + not_tls_session() + malformed_session()
    write(os.path.join(OUT, 'tls_sessions.pcapng'), f)
