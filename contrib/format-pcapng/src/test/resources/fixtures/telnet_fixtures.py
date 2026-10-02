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
"""Generates decoders/telnet/telnet.pcapng. Telnet has no scapy dissector, so the byte streams are built
by hand. Sessions, by client port: 50001 a login with IAC negotiation and a typed username/password,
50002 HTTP on port 23 (not Telnet), 50003 an unterminated TERMINAL-TYPE subnegotiation (decode_error)."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from tcp_sessions import capture, session  # noqa: E402

C, S = True, False
IAC, SE, SB, WILL, DO = 255, 240, 250, 251, 253
ECHO, SUPPRESS_GO_AHEAD, TERMINAL_TYPE = 1, 3, 24


def seq(*values):
    return bytes(values)


def main():
    server_nego = seq(IAC, DO, TERMINAL_TYPE) + seq(IAC, WILL, ECHO) + seq(IAC, WILL, SUPPRESS_GO_AHEAD)
    client_nego = seq(IAC, WILL, TERMINAL_TYPE) + seq(IAC, SB, TERMINAL_TYPE, 0) + b'xterm' + seq(IAC, SE)
    login = session(50001, 23, [
        (S, server_nego + b'Ubuntu 22.04\r\nlogin: '),
        (C, client_nego),
        (C, b'alice\r\n'),
        (S, b'Password: '),
        (C, b'secret\r\n'),
        (S, b'Last login: today\r\n$ '),
    ])
    http = session(50002, 23, [(C, b'GET / HTTP/1.0\r\n\r\n'), (S, b'HTTP/1.0 200 OK\r\n\r\n')])
    unterminated = session(50003, 23, [
        (S, b'login: '),
        (C, seq(IAC, SB, TERMINAL_TYPE, 0) + b'xterm'),  # no IAC SE
    ])
    capture(os.path.join('telnet', 'telnet.pcapng'), [login, http, unterminated])


if __name__ == '__main__':
    main()
