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
"""Generates decoders/ftp/ftp.pcapng. Run from any directory:
   python3 contrib/format-pcapng/src/test/resources/fixtures/ftp_fixtures.py
Sessions, by client port: 40001 anonymous login with passive and active transfers, 40002 HTTP on port 21
(not FTP), 40003 binary data after the greeting (decode_error), 40004 AUTH TLS (encrypted after 234)."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from tcp_sessions import capture, session  # noqa: E402

C, S = True, False


def main():
    login = session(40001, 21, [
        (S, b'220-Welcome\r\n220 FTP ready\r\n'),
        (C, b'USER anonymous\r\n'), (S, b'331 Please specify the password.\r\n'),
        (C, b'PASS guest@example.com\r\n'), (S, b'230 Login successful.\r\n'),
        (C, b'SYST\r\n'), (S, b'215 UNIX Type: L8\r\n'),
        (C, b'PWD\r\n'), (S, b'257 "/" is the current directory\r\n'),
        (C, b'CWD /pub\r\n'), (S, b'250 Directory successfully changed.\r\n'),
        (C, b'PASV\r\n'), (S, b'227 Entering Passive Mode (10,0,0,2,195,80).\r\n'),
        (C, b'RETR readme.txt\r\n'), (S, b'150 Opening BINARY mode data connection.\r\n'),
        (S, b'226 Transfer complete.\r\n'),
        (C, b'PORT 10,0,0,1,4,1\r\n'), (S, b'200 PORT command successful.\r\n'),
        (C, b'STOR upload.bin\r\n'), (S, b'553 Could not create file.\r\n'),
        (C, b'QUIT\r\n'), (S, b'221 Goodbye.\r\n'),
    ])
    http = session(40002, 21, [(C, b'GET / HTTP/1.0\r\n\r\n'), (S, b'HTTP/1.0 200 OK\r\n\r\n')])
    garbage = session(40003, 21, [(S, b'220 ready\r\n'), (C, b'USER bob\r\n'), (S, b'\x00\x01\x02\x03\r\n')])
    tls = session(40004, 21, [
        (S, b'220 ready\r\n'), (C, b'AUTH TLS\r\n'), (S, b'234 Proceed with negotiation.\r\n'),
        (C, bytes.fromhex('160301002a010000260303') + bytes(31)),
    ])
    capture(os.path.join('ftp', 'ftp.pcapng'), [login, http, garbage, tls])


if __name__ == '__main__':
    main()
