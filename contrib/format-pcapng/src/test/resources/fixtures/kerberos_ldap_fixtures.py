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
"""Generates the Kerberos (UDP 88 packets) and LDAP (TCP 389 sessions) decoder fixtures. Run from any
   directory:
   python3 contrib/format-pcapng/src/test/resources/fixtures/kerberos_ldap_fixtures.py"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from pcap_fixtures import shb, idb, epb, ipv4, udp, write  # noqa: E402
from tcp_sessions import capture as session_capture, session  # noqa: E402

DECODERS = os.path.join(HERE, '..', 'decoders')


# ---- minimal BER builders (all integer values here are < 128) ----

def tlv(tag, *parts):
    content = b''.join(parts)
    if len(content) < 128:
        length = bytes([len(content)])
    else:
        length = bytes([0x82, (len(content) >> 8) & 0xFF, len(content) & 0xFF])
    return bytes([tag]) + length + content


def i(v):
    return tlv(0x02, bytes([v]))


def enu(v):
    return tlv(0x0A, bytes([v]))


def boolean(v):
    return tlv(0x01, bytes([0xFF if v else 0x00]))


def octets(b):
    return tlv(0x04, b if isinstance(b, bytes) else b.encode())


def gstring(s):
    return tlv(0x1B, s.encode())


def gtime(s):
    return tlv(0x18, s.encode())


def seq(*parts):
    return tlv(0x30, *parts)


def app(n, *parts):
    return tlv(0x60 | n, *parts)


def ctx(n, *parts):
    return tlv(0xA0 | n, *parts)


def ctx_prim(n, content):
    return tlv(0x80 | n, content)


def principal(name_type, *components):
    return seq(ctx(0, i(name_type)), ctx(1, seq(*[gstring(c) for c in components])))


def enc_data(etype):
    return seq(ctx(0, i(etype)), ctx(2, octets(bytes([1, 2, 3, 4]))))


# ---- Kerberos messages ----

def as_req(with_padata=True):
    top = [ctx(1, i(5)), ctx(2, i(10))]
    if with_padata:
        top.append(ctx(3, seq(seq(ctx(1, i(2)), ctx(2, octets(bytes([0])))))))
    body = seq(
        ctx(0, tlv(0x03, bytes(5))),              # kdc-options
        ctx(1, principal(1, 'alice')),            # cname
        ctx(2, gstring('EXAMPLE.COM')),           # realm
        ctx(3, principal(2, 'krbtgt', 'EXAMPLE.COM')),  # sname
        ctx(5, gtime('20240102030405Z')),         # till
        ctx(7, i(42)),                            # nonce
        ctx(8, seq(i(18), i(17), i(23))))         # etype
    top.append(ctx(4, body))
    return app(10, seq(*top))


def as_rep():
    ticket = ctx(5, app(1, seq(
        ctx(0, i(5)),
        ctx(1, gstring('EXAMPLE.COM')),
        ctx(2, principal(2, 'HTTP', 'web.example.com')),
        ctx(3, enc_data(23)))))              # ticket enc-part etype 23: Kerberoasting signal
    return app(11, seq(
        ctx(0, i(5)),
        ctx(1, i(11)),
        ctx(3, gstring('EXAMPLE.COM')),
        ctx(4, principal(1, 'alice')),
        ticket,
        ctx(6, enc_data(18))))


def krb_error():
    return app(30, seq(
        ctx(0, i(5)),
        ctx(1, i(30)),
        ctx(4, gtime('20240102030405Z')),
        ctx(5, i(42)),
        ctx(6, i(25)),                       # KDC_ERR_PREAUTH_REQUIRED
        ctx(9, gstring('EXAMPLE.COM')),
        ctx(10, principal(2, 'krbtgt', 'EXAMPLE.COM'))))


def kerberos():
    # Rows: AS-REQ, AS-REP, KRB-ERROR, non-Kerberos on port 88, truncated AS-REQ
    req = as_req()
    rep = as_rep()
    err = krb_error()
    f = shb() + idb(101)
    f += epb(0, ipv4(17, udp(40000, 88, req), '10.0.0.1', '10.0.0.2'))
    f += epb(0, ipv4(17, udp(88, 40000, rep), '10.0.0.2', '10.0.0.1'))
    f += epb(0, ipv4(17, udp(88, 40001, err), '10.0.0.2', '10.0.0.1'))
    f += epb(0, ipv4(17, udp(40002, 88, b'this is not a kerberos packet at all'), '10.0.0.1', '10.0.0.2'))
    f += epb(0, ipv4(17, udp(40003, 88, req[:-6]), '10.0.0.1', '10.0.0.2'))
    directory = os.path.join(DECODERS, 'kerberos')
    os.makedirs(directory, exist_ok=True)
    write(os.path.join(directory, 'kerberos.pcapng'), f)


# ---- LDAP messages ----

def ldap_message(msg_id, protocol_op):
    return seq(i(msg_id), protocol_op)


def eq(attr, value):
    return ctx(3, octets(attr), octets(value))


def simple_bind(msg_id, dn, password):
    return ldap_message(msg_id, app(0, i(3), octets(dn), ctx_prim(0, password.encode())))


def bind_response(msg_id, code):
    return ldap_message(msg_id, app(1, enu(code), octets(''), octets('')))


def search_request(msg_id, base, scope, flt):
    return ldap_message(msg_id, app(3, octets(base), enu(scope), enu(0), i(0), i(0), boolean(False), flt, seq()))


def search_done(msg_id, code):
    return ldap_message(msg_id, app(5, enu(code), octets(''), octets('')))


def search_entry(msg_id, dn):
    return ldap_message(msg_id, app(4, octets(dn), seq()))


def ldap():
    flt = ctx(0, eq('objectClass', 'user'), eq('sAMAccountName', 'admin'))
    # A valid bind + search session on 389
    client1 = [
        (True, simple_bind(1, 'cn=admin,dc=example,dc=com', 'secret')),
        (True, search_request(2, 'dc=example,dc=com', 2, flt)),
    ]
    server1 = [
        (False, bind_response(1, 0)),
        (False, search_entry(2, 'cn=alice,dc=example,dc=com')),
        (False, search_entry(2, 'cn=bob,dc=example,dc=com')),
        (False, search_done(2, 0)),
    ]
    good = session(40001, 389, interleave(client1, server1))

    # Not LDAP: plain text on port 389
    not_ldap = session(40002, 389, [(True, b'GET / HTTP/1.0\r\n\r\n'), (False, b'nope\r\n')])

    # Malformed: a valid bind then a truncated LDAPMessage header
    bad_bind = simple_bind(1, 'cn=svc,dc=example,dc=com', 'pw')
    truncated = bytes([0x30, 0x30, 0x02, 0x01, 0x05])      # claims 0x30 bytes but only 3 follow
    bad = session(40003, 389, [(True, bad_bind + truncated), (False, bind_response(1, 49))])

    session_capture('ldap/ldap.pcapng', [good, not_ldap, bad])


def interleave(client_steps, server_steps):
    """Client bind, server bind-response, client search, then server entries: realistic ordering."""
    steps = []
    steps.append(client_steps[0])
    steps.append(server_steps[0])
    steps.append(client_steps[1])
    steps.extend(server_steps[1:])
    return steps


if __name__ == '__main__':
    kerberos()
    ldap()
    print('wrote decoders/kerberos/kerberos.pcapng and decoders/ldap/ldap.pcapng')
