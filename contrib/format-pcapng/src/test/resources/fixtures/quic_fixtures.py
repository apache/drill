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
"""Generates decoders/quic/quic.pcapng: a real QUIC v1 Initial packet carrying a TLS ClientHello,
encrypted with the Initial keys derived from the Destination Connection ID and the published salt
(RFC 9001 section 5.2); this uses only public key material, so the ClientHello can be read by passive
inspection. Run from any directory:
   python3 contrib/format-pcapng/src/test/resources/fixtures/quic_fixtures.py
Requires the cryptography package (HKDF, AES-128-GCM, AES-128-ECB)."""
import hashlib
import os
import struct
import sys

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from pcap_fixtures import shb, idb, epb, ipv4, udp, write  # noqa: E402
from tls_fixtures import sni, alpn, u16_list, handshake, client_hello_body  # noqa: E402

OUT = os.path.join(HERE, '..', 'decoders', 'quic')
INITIAL_SALT_V1 = bytes.fromhex('38762cf7f55934b34d179ae6a4c80cadccbb7f0a')
GREASE = {0x0a0a + 0x1010 * i for i in range(16)}

def supported_versions_ext(*versions):
    body = bytes([2 * len(versions)]) + b''.join(struct.pack('>H', v) for v in versions)
    return struct.pack('>HH', 43, len(body)) + body


# The ClientHello carried in the Initial. TLS 1.3 suites, SNI, ALPN h2, supported_versions 1.3, sig algs.
CH_CIPHERS = [0x1301, 0x1302, 0x1303]
CH_EXTENSIONS = [sni('example.org'), supported_versions_ext(0x0304),
                 alpn('h2'), u16_list(13, 0x0403, 0x0804, 0x0401), u16_list(10, 29, 23)]
# Extension types, in wire order, for the JA4 cross-check: sni(0), supported_versions(43), alpn(16),
# signature_algorithms(13), supported_groups(10).
CH_EXT_TYPES = [0, 43, 16, 13, 10]
CH_SIG_ALGS = [0x0403, 0x0804, 0x0401]
CH_SUPPORTED_VERSIONS = [0x0304]
CLIENT_HELLO = handshake(1, client_hello_body(0x0303, b'', CH_CIPHERS, CH_EXTENSIONS))


def ja4_quic():
    def h4(v):
        return '%04x' % v

    def sha12(s):
        return hashlib.sha256(s.encode('ascii')).hexdigest()[:12]

    version = max(v for v in CH_SUPPORTED_VERSIONS if v not in GREASE)
    ver_code = {0x0304: '13', 0x0303: '12'}[version]
    cc = sum(1 for c in CH_CIPHERS if c not in GREASE)
    ec = sum(1 for e in CH_EXT_TYPES if e not in GREASE)
    a = 'q%sd%02d%02dh2' % (ver_code, cc, ec)
    chex = sorted(h4(c) for c in CH_CIPHERS if c not in GREASE)
    b = sha12(','.join(chex))
    ehex = sorted(h4(e) for e in CH_EXT_TYPES if e not in GREASE and e not in (0x0000, 0x0010))
    c = sha12(','.join(ehex) + '_' + ','.join(h4(s) for s in CH_SIG_ALGS))
    return '%s_%s_%s' % (a, b, c)


def varint(v):
    if v < 64:
        return bytes([v])
    if v < 16384:
        return struct.pack('>H', v | 0x4000)
    if v < 2 ** 30:
        return struct.pack('>I', v | 0x80000000)
    return struct.pack('>Q', v | 0xC000000000000000)


def hkdf_extract(salt, ikm):
    import hmac
    return hmac.new(salt, ikm, hashlib.sha256).digest()


def hkdf_expand(prk, info, length):
    import hmac
    out, t, counter = b'', b'', 1
    while len(out) < length:
        t = hmac.new(prk, t + info + bytes([counter]), hashlib.sha256).digest()
        out += t
        counter += 1
    return out[:length]


def expand_label(secret, label, length):
    full = b'tls13 ' + label
    info = struct.pack('>H', length) + bytes([len(full)]) + full + b'\x00'
    return hkdf_expand(secret, info, length)


def build_initial(dcid, scid, client_hello, pad_to=1200):
    initial_secret = hkdf_extract(INITIAL_SALT_V1, dcid)
    client_secret = expand_label(initial_secret, b'client in', 32)
    key = expand_label(client_secret, b'quic key', 16)
    iv = expand_label(client_secret, b'quic iv', 12)
    hp = expand_label(client_secret, b'quic hp', 16)

    crypto = b'\x06' + varint(0) + varint(len(client_hello)) + client_hello
    pn = 0
    pn_bytes = struct.pack('>I', pn)        # four-byte packet number
    first = 0xC0 | (len(pn_bytes) - 1)      # long header, fixed bit, Initial type, PN length - 1

    header_no_len = bytes([first]) + struct.pack('>I', 1) + bytes([len(dcid)]) + dcid \
        + bytes([len(scid)]) + scid + varint(0)   # empty token
    # Pad the plaintext so the whole datagram reaches pad_to bytes (QUIC requires a client Initial >= 1200).
    overhead = len(header_no_len) + 2 + len(pn_bytes) + 16   # +2 for the two-byte length varint, +16 GCM tag
    plaintext = crypto + b'\x00' * max(0, pad_to - overhead - len(crypto))
    length = len(pn_bytes) + len(plaintext) + 16
    header = header_no_len + varint(length) + pn_bytes

    nonce = bytes(a ^ b for a, b in zip(iv, b'\x00' * 8 + pn_bytes))
    ciphertext = AESGCM(key).encrypt(nonce, plaintext, header)
    packet = bytearray(header + ciphertext)

    pn_offset = len(header) - len(pn_bytes)
    sample = bytes(packet[pn_offset + 4:pn_offset + 20])
    enc = Cipher(algorithms.AES(hp), modes.ECB()).encryptor()
    mask = enc.update(sample) + enc.finalize()
    packet[0] ^= mask[0] & 0x0f
    for i in range(len(pn_bytes)):
        packet[pn_offset + i] ^= mask[1 + i]
    return bytes(packet)


def main():
    dcid = bytes.fromhex('8394c8f03e515708')
    scid = bytes.fromhex('c0ffee')
    initial = build_initial(dcid, scid, CLIENT_HELLO)
    print('quic initial bytes:', len(initial))
    print('dcid', dcid.hex(), 'scid', scid.hex())
    print('ja4', ja4_quic())
    client, server = '10.0.0.1', '93.184.216.34'
    rows = [
        (client, server, 50000, 443, initial),                 # valid v1 Initial with SNI
        (client, server, 50001, 443, b'not a quic packet at all'),  # non-QUIC UDP on 443 (undecoded)
        (client, server, 50002, 443, initial[:30]),            # truncated Initial (decode_error)
    ]
    f = shb() + idb(101)
    for src, dst, sport, dport, data in rows:
        f += epb(0, ipv4(17, udp(sport, dport, data), src, dst))
    os.makedirs(OUT, exist_ok=True)
    write(os.path.join(OUT, 'quic.pcapng'), f)


if __name__ == '__main__':
    main()
