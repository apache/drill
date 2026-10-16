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
"""Generates decoders/ssh/ssh.pcapng and prints the HASSH values, computed independently with hashlib.
Run from any directory:
   python3 contrib/format-pcapng/src/test/resources/fixtures/ssh_fixtures.py
If scapy is installed, each KEXINIT is also parsed with scapy's SSH layer as a cross-check.
Sessions, by client port: 40101 OpenSSH client to OpenSSH server on 22, 40102 HTTP on port 22 (not SSH),
40103 client KEXINIT cut short (decode_error), 40104 dropbear server on 2222 that sends a line before its
identification."""
import hashlib
import os
import struct
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from tcp_sessions import capture, session  # noqa: E402

C, S = True, False
NAMES = ['kex_algorithms', 'server_host_key_algorithms',
         'encryption_algorithms_client_to_server', 'encryption_algorithms_server_to_client',
         'mac_algorithms_client_to_server', 'mac_algorithms_server_to_client',
         'compression_algorithms_client_to_server', 'compression_algorithms_server_to_client',
         'languages_client_to_server', 'languages_server_to_client']

CLIENT = ['curve25519-sha256,curve25519-sha256@libssh.org,ecdh-sha2-nistp256,diffie-hellman-group14-sha256,'
          'ext-info-c,kex-strict-c-v00@openssh.com',
          'ssh-ed25519-cert-v01@openssh.com,ssh-ed25519,rsa-sha2-512,rsa-sha2-256',
          'chacha20-poly1305@openssh.com,aes128-ctr,aes256-gcm@openssh.com',
          'chacha20-poly1305@openssh.com,aes128-ctr,aes256-gcm@openssh.com',
          'umac-64-etm@openssh.com,hmac-sha2-256-etm@openssh.com,hmac-sha2-256',
          'umac-64-etm@openssh.com,hmac-sha2-256-etm@openssh.com,hmac-sha2-256',
          'none,zlib@openssh.com', 'none,zlib@openssh.com', '', '']
SERVER = ['curve25519-sha256,sntrup761x25519-sha512@openssh.com,kex-strict-s-v00@openssh.com',
          'rsa-sha2-512,rsa-sha2-256,ecdsa-sha2-nistp256,ssh-ed25519',
          'aes128-ctr,aes256-ctr', 'chacha20-poly1305@openssh.com,aes256-gcm@openssh.com',
          'hmac-sha2-256', 'hmac-sha2-512-etm@openssh.com,hmac-sha2-256',
          'none', 'none,zlib@openssh.com', '', '']


def kexinit(lists):
    payload = bytes([20]) + bytes(range(16))
    for names in lists:
        payload += struct.pack('>I', len(names)) + names.encode()
    payload += b'\0' + struct.pack('>I', 0)
    padding = 8 - (5 + len(payload)) % 8 + 4
    return struct.pack('>IB', 1 + len(payload) + padding, padding) + payload + bytes(padding)


def hassh(lists, client):
    """HASSH (github.com/salesforce/hassh): kex;encryption;mac;compression, each for the sender's direction."""
    d = 0 if client else 1
    text = ';'.join([lists[0], lists[2 + d], lists[4 + d], lists[6 + d]])
    return text, hashlib.md5(text.encode()).hexdigest()


def cross_check(packet, lists):
    try:
        from scapy.layers.ssh import SSH
    except ImportError:
        return
    parsed = SSH(packet).pay
    for name, expected in zip(NAMES, lists):
        value = b','.join(getattr(parsed, name).names).decode()
        assert value == expected, (name, value, expected)


def main():
    for lists in (CLIENT, SERVER):
        cross_check(kexinit(lists), lists)
    for label, lists, client in (('hassh', CLIENT, True), ('hassh_server', SERVER, False)):
        text, digest = hassh(lists, client)
        print(label, digest, text)
    client_hello = b'SSH-2.0-OpenSSH_9.6p1 Ubuntu-3ubuntu13.5\r\n'
    full = session(40101, 22, [
        (C, client_hello), (S, b'SSH-2.0-OpenSSH_8.9p1\r\n'),
        (C, kexinit(CLIENT)), (S, kexinit(SERVER)),
        (C, bytes(range(40))),  # SSH_MSG_KEX_ECDH_INIT and onwards: not read
    ])
    http = session(40102, 22, [(C, b'GET / HTTP/1.0\r\n\r\n'), (S, b'HTTP/1.0 200 OK\r\n\r\n')])
    cut = session(40103, 22, [(C, client_hello), (S, b'SSH-2.0-OpenSSH_8.9p1\r\n'), (C, kexinit(CLIENT)[:50])])
    dropbear = session(40104, 2222, [
        (S, b'Authorized use only\r\nSSH-2.0-dropbear_2022.83\r\n'), (S, kexinit(SERVER)),
        (C, client_hello), (C, kexinit(CLIENT)),
    ])
    capture(os.path.join('ssh', 'ssh.pcapng'), [full, http, cut, dropbear])


if __name__ == '__main__':
    main()
