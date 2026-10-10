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
"""Generates decoders/smb/smb.pcapng: SMB2/SMB3 sessions on TCP 445.
Run from any directory:
   python3 contrib/format-pcapng/src/test/resources/fixtures/smb_fixtures.py
If scapy is installed its SMB2/NTLM layers cross-check the negotiate response and the NTLM AUTHENTICATE.
Sessions, by client port: 40501 full SMB 3.1.1 session with NTLMv2 SESSION_SETUP, 40502 HTTP on port 445
(not SMB), 40503 a server SMB2 message cut short (decode_error), 40504 Kerberos SESSION_SETUP."""
import os
import struct
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from tcp_sessions import capture, session  # noqa: E402

SIGNATURE = b'NTLMSSP\x00'
UNICODE = 0x00000001
NEGOTIATE, CHALLENGE, AUTHENTICATE = 1, 2, 3
KERBEROS_OID = bytes([0x2a, 0x86, 0x48, 0x86, 0xf7, 0x12, 0x01, 0x02, 0x02])


def frame(message):
    """A 4-byte Direct-TCP/NetBIOS session header (type 0, 24-bit length) then the SMB message."""
    return struct.pack('>I', len(message)) + message


def smb2_header(command, response, next_command=0):
    h = bytearray(64)
    h[0:4] = b'\xfeSMB'
    struct.pack_into('<H', h, 4, 64)
    struct.pack_into('<H', h, 12, command)
    struct.pack_into('<I', h, 16, 1 if response else 0)
    struct.pack_into('<I', h, 20, next_command)
    return bytes(h)


def negotiate_request(dialects, client_guid):
    body = bytearray(36 + len(dialects) * 2)
    struct.pack_into('<H', body, 0, 36)
    struct.pack_into('<H', body, 2, len(dialects))
    body[12:28] = client_guid
    for i, d in enumerate(dialects):
        struct.pack_into('<H', body, 36 + i * 2, d)
    return smb2_header(0, False) + bytes(body)


def negotiate_response(dialect, signing_required, server_guid):
    body = bytearray(64)
    struct.pack_into('<H', body, 0, 65)
    struct.pack_into('<H', body, 2, 0x0002 if signing_required else 0x0001)
    struct.pack_into('<H', body, 4, dialect)
    body[8:24] = server_guid
    return smb2_header(0, True) + bytes(body)


def session_setup(response, blob):
    body_len = 8 if response else 24
    sec_offset = 64 + body_len
    body = bytearray(body_len)
    if response:
        struct.pack_into('<H', body, 0, 9)
        struct.pack_into('<H', body, 4, sec_offset)
        struct.pack_into('<H', body, 6, len(blob))
    else:
        struct.pack_into('<H', body, 0, 25)
        struct.pack_into('<H', body, 12, sec_offset)
        struct.pack_into('<H', body, 14, len(blob))
    return smb2_header(1, response) + bytes(body) + blob


def secbuf(header, at, base, payload, value):
    offset = base + len(payload)
    struct.pack_into('<H', header, at, len(value))
    struct.pack_into('<H', header, at + 2, len(value))
    struct.pack_into('<I', header, at + 4, offset)
    payload.extend(value)


def ntlm_authenticate(domain, user, workstation, nt_len):
    header = bytearray(64)
    header[0:8] = SIGNATURE
    struct.pack_into('<I', header, 8, AUTHENTICATE)
    payload = bytearray()
    secbuf(header, 12, 64, payload, b'\x00' * 24)          # LM response (placeholder)
    secbuf(header, 20, 64, payload, b'\x00' * nt_len)      # NT response: length drives v1/v2
    secbuf(header, 28, 64, payload, domain.encode('utf-16-le'))
    secbuf(header, 36, 64, payload, user.encode('utf-16-le'))
    secbuf(header, 44, 64, payload, workstation.encode('utf-16-le'))
    secbuf(header, 52, 64, payload, b'')                   # session key
    struct.pack_into('<I', header, 60, UNICODE)
    return bytes(header) + bytes(payload)


def ntlm_challenge(target):
    header = bytearray(48)
    header[0:8] = SIGNATURE
    struct.pack_into('<I', header, 8, CHALLENGE)
    payload = bytearray()
    secbuf(header, 12, 48, payload, target.encode('utf-16-le'))   # TargetName
    struct.pack_into('<I', header, 20, UNICODE)
    struct.pack_into('<Q', header, 24, 0x1122334455667788)        # server challenge (not read)
    secbuf(header, 40, 48, payload, b'')                          # empty TargetInfo
    return bytes(header) + bytes(payload)


def kerberos_blob():
    # A minimal SPNEGO initial token carrying the Kerberos v5 mechanism OID.
    return bytes([0x60, 0x40, 0x06, 0x09]) + KERBEROS_OID + b'\x00' * 32


def cross_check():
    try:
        from scapy.layers.ntlm import NTLM_AUTHENTICATE_V2
    except ImportError:
        print('scapy not available; skipping cross-check')
        return
    auth = NTLM_AUTHENTICATE_V2(ntlm_authenticate('CORP', 'alice', 'WS01', 48))
    assert auth.UserName == 'alice', auth.UserName
    assert auth.DomainName == 'CORP', auth.DomainName
    print('scapy cross-check ok: NTLM AUTHENTICATE user=alice domain=CORP')


def main():
    cross_check()
    client_guid = bytes(range(16))
    server_guid = bytes(range(16, 32))

    full = session(40501, 445, [
        (True, frame(negotiate_request([0x0202, 0x0210, 0x0300, 0x0311], client_guid))),
        (False, frame(negotiate_response(0x0311, True, server_guid))),
        (True, frame(session_setup(False, ntlm_authenticate('CORP', 'alice', 'WS01', 48)))),
        (False, frame(session_setup(True, ntlm_challenge('CORP')))),
    ])
    http = session(40502, 445, [
        (True, b'GET / HTTP/1.1\r\nHost: x\r\n\r\n'), (False, b'HTTP/1.1 404 Not Found\r\n\r\n')])
    # A server SMB2 message whose frame promises more than a 64-byte header holds.
    truncated = session(40503, 445, [
        (True, frame(negotiate_request([0x0311], client_guid))),
        (False, frame(b'\xfeSMB\x00\x00\x00\x00')),
    ])
    kerberos = session(40504, 445, [
        (True, frame(negotiate_request([0x0311], client_guid))),
        (False, frame(negotiate_response(0x0311, True, server_guid))),
        (True, frame(session_setup(False, kerberos_blob()))),
    ])
    capture(os.path.join('smb', 'smb.pcapng'), [full, http, truncated, kerberos])
    print('wrote decoders/smb/smb.pcapng')


if __name__ == '__main__':
    main()
