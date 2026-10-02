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
"""Generates the SMTP, POP3 and IMAP session fixtures under decoders/. Run from any directory:
   python3 contrib/format-pcapng/src/test/resources/fixtures/mail_fixtures.py

Scapy has no SMTP, POP3 or IMAP dissectors, so expected values are checked by construction: every
value the tests assert appears literally below."""
import base64
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pcap_fixtures import shb, idb, epb, ipv4, tcp, write  # noqa: E402

DECODERS = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'decoders')
FIN, SYN, PSH, ACK = 0x01, 0x02, 0x08, 0x10
CLIENT, SERVER = '10.0.0.1', '10.0.0.2'
TLS = bytes.fromhex('160301002e010000') + bytes(10)  # start of a TLS record: binary after the switch


def lines(*text):
    return ''.join(t + '\r\n' for t in text).encode()


def session(cport, sport, exchange):
    """Packets of one closed TCP session; exchange is a list of ('c' | 's', bytes) in send order."""
    cseq, sseq = 1000, 5000
    packets = [ipv4(6, tcp(cport, sport, cseq, SYN, b''), CLIENT, SERVER),
               ipv4(6, tcp(sport, cport, sseq, SYN | ACK, b'', cseq + 1), SERVER, CLIENT)]
    cseq += 1
    sseq += 1
    packets.append(ipv4(6, tcp(cport, sport, cseq, ACK, b'', sseq), CLIENT, SERVER))
    for direction, data in exchange:
        if direction == 'c':
            packets.append(ipv4(6, tcp(cport, sport, cseq, PSH | ACK, data, sseq), CLIENT, SERVER))
            cseq += len(data)
        else:
            packets.append(ipv4(6, tcp(sport, cport, sseq, PSH | ACK, data, cseq), SERVER, CLIENT))
            sseq += len(data)
    packets.append(ipv4(6, tcp(cport, sport, cseq, FIN | ACK, b'', sseq), CLIENT, SERVER))
    packets.append(ipv4(6, tcp(sport, cport, sseq, FIN | ACK, b'', cseq + 1), SERVER, CLIENT))
    packets.append(ipv4(6, tcp(cport, sport, cseq + 1, ACK, b'', sseq + 1), CLIENT, SERVER))
    return packets


def capture(name, sessions):
    f = shb() + idb(101)
    for packets in sessions:
        for p in packets:
            f += epb(0, p)
    os.makedirs(os.path.join(DECODERS, name), exist_ok=True)
    write(os.path.join(DECODERS, name, name + '.pcapng'), f)


def not_mail(cport, sport):
    # Another protocol on the mail port: must stay undecoded
    return session(cport, sport, [('s', lines('SSH-2.0-OpenSSH_9.6')), ('c', lines('SSH-2.0-OpenSSH_9.6'))])


def smtp():
    plain = base64.b64encode(b'\0alice\0secret').decode()
    full = session(40001, 25, [
        ('s', lines('220 mail.example.com ESMTP Postfix')),
        ('c', lines('EHLO client.example.org')),
        ('s', lines('250-mail.example.com', '250-PIPELINING', '250-SIZE 10240000', '250 AUTH PLAIN LOGIN')),
        ('c', lines('AUTH PLAIN ' + plain)),
        ('s', lines('235 2.7.0 Authentication successful')),
        ('c', lines('MAIL FROM:<alice@example.org>', 'RCPT TO:<bob@example.com>', 'RCPT TO:<carol@example.com>',
                    'DATA')),
        ('s', lines('250 2.1.0 Ok', '250 2.1.5 Ok', '250 2.1.5 Ok', '354 End data with <CR><LF>.<CR><LF>')),
        ('c', lines('From: Alice <alice@example.org>', 'To: bob@example.com', 'Cc: carol@example.com',
                    'Subject: =?utf-8?Q?Caf=C3=A9?=', 'Date: Tue, 2 Jan 2024 03:04:05 +0000',
                    'Message-ID: <m1@example.org>', '', '..leading dot', 'body', '.')),
        ('s', lines('250 2.0.0 Ok: queued as 12345')),
        ('c', lines('QUIT')),
        ('s', lines('221 2.0.0 Bye'))])
    starttls = session(40002, 587, [
        ('s', lines('220 mail.example.com ESMTP')),
        ('c', lines('EHLO client.example.org')),
        ('s', lines('250-mail.example.com', '250 STARTTLS')),
        ('c', lines('STARTTLS')),
        ('s', lines('220 2.0.0 Ready to start TLS')),
        ('c', TLS),
        ('s', TLS)])
    # DATA that never ends: the session closes before the terminating "."
    truncated = session(40004, 25, [
        ('s', lines('220 mail.example.com ESMTP')),
        ('c', lines('HELO client.example.org')),
        ('s', lines('250 mail.example.com')),
        ('c', lines('MAIL FROM:<a@example.org>', 'RCPT TO:<b@example.com>', 'DATA')),
        ('s', lines('250 Ok', '250 Ok', '354 go ahead')),
        ('c', lines('Subject: cut off', '', 'part of the bo'))])
    capture('smtp', [full, starttls, not_mail(40003, 25), truncated])


def pop3():
    full = session(41001, 110, [
        ('s', lines('+OK POP3 server ready')),
        ('c', lines('USER bob')),
        ('s', lines('+OK')),
        ('c', lines('PASS hunter2')),
        ('s', lines('+OK logged in')),
        ('c', lines('STAT')),
        ('s', lines('+OK 2 320')),
        ('c', lines('RETR 1')),
        ('s', lines('+OK 120 octets', 'From: Alice <alice@example.org>', 'To: bob@example.com', 'Subject: Hi',
                    'Message-ID: <p1@example.org>', '', 'hello', '.')),
        ('c', lines('QUIT')),
        ('s', lines('+OK bye'))])
    stls = session(41002, 110, [
        ('s', lines('+OK POP3 server ready')),
        ('c', lines('STLS')),
        ('s', lines('+OK begin TLS')),
        ('c', TLS),
        ('s', TLS)])
    truncated = session(41004, 110, [
        ('s', lines('+OK POP3 server ready')),
        ('c', lines('RETR 1')),
        ('s', lines('+OK 50 octets', 'Subject: cut off', '', 'par'))])
    capture('pop3', [full, stls, not_mail(41003, 110), truncated])


def imap():
    header = 'From: Carol <carol@example.org>\r\nSubject: =?utf-8?B?SMOpbGxv?=\r\nMessage-ID: <i2@example.org>\r\n\r\n'
    full = session(42001, 143, [
        ('s', lines('* OK [CAPABILITY IMAP4rev1 STARTTLS AUTH=PLAIN] Dovecot ready.')),
        ('c', lines('a1 LOGIN bob "hunter 2"')),
        ('s', lines('a1 OK [CAPABILITY IMAP4rev1 IDLE SORT] Logged in')),
        ('c', lines('a2 SELECT INBOX')),
        ('s', lines('* 2 EXISTS', '* FLAGS (\\Seen \\Deleted)', 'a2 OK [READ-WRITE] Select completed.')),
        ('c', lines('a3 FETCH 1:2 (UID ENVELOPE BODY.PEEK[HEADER])')),
        ('s', lines('* 1 FETCH (UID 7 ENVELOPE ("Tue, 2 Jan 2024 03:04:05 +0000" "Hi there" '
                    '(("Alice" NIL "alice" "example.org")) NIL NIL ((NIL NIL "bob" "example.com")) NIL NIL NIL '
                    '"<i1@example.org>"))')
              + ('* 2 FETCH (UID 8 BODY[HEADER] {%d}\r\n%s)\r\n' % (len(header), header)).encode()
              + lines('a3 OK Fetch completed.')),
        ('c', lines('a4 LOGOUT')),
        ('s', lines('* BYE Logging out', 'a4 OK Logout completed.'))])
    starttls = session(42002, 143, [
        ('s', lines('* OK IMAP4rev1 ready')),
        ('c', lines('a1 STARTTLS')),
        ('s', lines('a1 OK Begin TLS negotiation now.')),
        ('c', TLS),
        ('s', TLS)])
    # A literal longer than the rest of the stream
    overflow = session(42004, 143, [
        ('s', lines('* OK IMAP4rev1 ready')),
        ('c', lines('a1 FETCH 1 BODY[HEADER]')),
        ('s', lines('* 1 FETCH (BODY[HEADER] {99999}', 'From: x'))])
    capture('imap', [full, starttls, not_mail(42003, 143), overflow])


if __name__ == '__main__':
    smtp()
    pop3()
    imap()
