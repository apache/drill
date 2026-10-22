/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.pcap.protocol.ldap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.drill.exec.store.pcap.protocol.DecoderContext;
import org.apache.drill.test.BaseTest;
import org.junit.Test;

public class TestLdapDecoder extends BaseTest {

  private static final class Context implements DecoderContext {
    final List<String> warnings = new ArrayList<>();
    final boolean expose;

    Context(boolean expose) {
      this.expose = expose;
    }

    @Override
    public boolean exposeCredentials() {
      return expose;
    }

    @Override
    public void warn(String message) {
      warnings.add(message);
    }
  }

  // ---- BER builders ----

  private static byte[] tlv(int tag, byte[]... parts) {
    ByteArrayOutputStream body = new ByteArrayOutputStream();
    for (byte[] p : parts) {
      body.write(p, 0, p.length);
    }
    byte[] content = body.toByteArray();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.write(tag);
    if (content.length < 128) {
      out.write(content.length);
    } else {
      out.write(0x82);
      out.write(content.length >> 8);
      out.write(content.length & 0xFF);
    }
    out.write(content, 0, content.length);
    return out.toByteArray();
  }

  private static byte[] integer(long v) {
    return tlv(0x02, java.math.BigInteger.valueOf(v).toByteArray());
  }

  private static byte[] enumerated(long v) {
    return tlv(0x0A, java.math.BigInteger.valueOf(v).toByteArray());
  }

  private static byte[] bool(boolean v) {
    return tlv(0x01, new byte[] {(byte) (v ? 0xFF : 0x00)});
  }

  private static byte[] octets(String s) {
    return tlv(0x04, s.getBytes(StandardCharsets.UTF_8));
  }

  private static byte[] seq(byte[]... parts) {
    return tlv(0x30, parts);
  }

  private static byte[] app(int n, byte[]... parts) {
    return tlv(0x60 | n, parts);
  }

  private static byte[] ctxPrim(int n, byte[] content) {
    return tlv(0x80 | n, content);
  }

  private static byte[] ctxCons(int n, byte[]... parts) {
    return tlv(0xA0 | n, parts);
  }

  private static byte[] ldapMessage(int id, byte[] protocolOp) {
    return seq(integer(id), protocolOp);
  }

  private static byte[] eq(String attr, String value) {
    return ctxCons(3, octets(attr), octets(value));
  }

  private static byte[] concat(byte[]... parts) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (byte[] p : parts) {
      out.write(p, 0, p.length);
    }
    return out.toByteArray();
  }

  private byte[] simpleBind(int id, String dn, String password) {
    return ldapMessage(id, app(0, integer(3), octets(dn),
        ctxPrim(0, password.getBytes(StandardCharsets.UTF_8))));
  }

  private byte[] bindResponse(int id, int code) {
    // BindResponse uses IMPLICIT tagging: [APPLICATION 1] replaces the SEQUENCE tag.
    return ldapMessage(id, app(1, enumerated(code), octets(""), octets("")));
  }

  private byte[] searchRequest(int id, String base, int scope, byte[] filter) {
    return ldapMessage(id, app(3, octets(base), enumerated(scope), enumerated(0),
        integer(0), integer(0), bool(false), filter, seq()));
  }

  private byte[] searchEntry(int id, String dn) {
    return ldapMessage(id, app(4, octets(dn), seq()));
  }

  @Test
  public void testBindAndSearch() {
    byte[] filter = ctxCons(0, eq("objectClass", "user"), eq("sAMAccountName", "admin"));
    byte[] client = concat(
        simpleBind(1, "cn=admin,dc=example,dc=com", "secret"),
        searchRequest(2, "dc=example,dc=com", 2, filter));
    byte[] server = concat(
        bindResponse(1, 0),
        searchEntry(2, "cn=alice,dc=example,dc=com"),
        searchEntry(2, "cn=bob,dc=example,dc=com"));

    LdapSession s = LdapParser.parse(client, -1, server, -1, new Context(false));
    assertEquals(Integer.valueOf(3), s.version);
    assertEquals("cn=admin,dc=example,dc=com", s.bindDn);
    assertEquals("simple", s.authType);
    assertTrue(s.passwordPresent);
    assertNull(s.password);
    assertEquals(Integer.valueOf(0), s.bindResultCode);
    assertEquals("success", s.bindResult);
    assertEquals(1, s.searches.size());
    assertEquals("dc=example,dc=com", s.searches.get(0).baseDn);
    assertEquals("sub", s.searches.get(0).scope);
    assertEquals("(&(objectClass=user)(sAMAccountName=admin))", s.searches.get(0).filter);
    assertEquals(2, s.entriesReturned.size());
    assertEquals("cn=alice,dc=example,dc=com", s.entriesReturned.get(0));
    assertEquals(2, s.operationCount);
  }

  @Test
  public void testExposedPassword() {
    byte[] client = simpleBind(1, "cn=admin", "s3cret");
    LdapSession s = LdapParser.parse(client, -1, new byte[0], -1, new Context(true));
    assertEquals("s3cret", s.password);
  }

  @Test
  public void testSaslBindAndFailure() {
    byte[] sasl = app(0, integer(3), octets(""),
        ctxCons(3, octets("GSSAPI")));
    byte[] client = ldapMessage(1, sasl);
    byte[] server = bindResponse(1, 49);
    LdapSession s = LdapParser.parse(client, -1, server, -1, new Context(false));
    assertEquals("GSSAPI", s.authType);
    assertFalse(s.passwordPresent);
    assertNull(s.bindDn);
    assertEquals(Integer.valueOf(49), s.bindResultCode);
    assertEquals("invalidCredentials", s.bindResult);
  }

  @Test
  public void testPresentAndSubstringFilters() {
    byte[] present = tlv(0x87, "cn".getBytes(StandardCharsets.UTF_8));
    byte[] subs = ctxCons(4, octets("cn"), seq(ctxPrim(0, "ab".getBytes(StandardCharsets.UTF_8)),
        ctxPrim(1, "c".getBytes(StandardCharsets.UTF_8)), ctxPrim(2, "d".getBytes(StandardCharsets.UTF_8))));
    byte[] client = concat(
        searchRequest(1, "dc=x", 0, present),
        searchRequest(2, "dc=x", 1, subs));
    LdapSession s = LdapParser.parse(client, -1, new byte[0], -1, new Context(false));
    assertEquals("(cn=*)", s.searches.get(0).filter);
    assertEquals("base", s.searches.get(0).scope);
    assertEquals("(cn=ab*c*d)", s.searches.get(1).filter);
    assertEquals("one", s.searches.get(1).scope);
  }

  @Test
  public void testNotLdap() {
    assertNull(LdapParser.parse("HELLO there\r\n".getBytes(StandardCharsets.UTF_8), -1, new byte[0], -1,
        new Context(false)));
    // A SEQUENCE whose first element is not an integer
    assertNull(LdapParser.parse(seq(octets("x"), app(0, integer(3))), -1, new byte[0], -1, new Context(false)));
    assertNull(LdapParser.parse(new byte[0], -1, new byte[0], -1, new Context(false)));
  }

  @Test
  public void testMalformedTail() {
    // A valid bind then a truncated second message: the bind is kept and a warning is recorded.
    byte[] bind = simpleBind(1, "cn=admin", "pw");
    byte[] truncated = new byte[] {0x30, 0x20, 0x02, 0x01, 0x02};
    Context context = new Context(false);
    LdapSession s = LdapParser.parse(concat(bind, truncated), -1, new byte[0], -1, context);
    assertEquals("cn=admin", s.bindDn);
    assertEquals(1, context.warnings.size());
  }
}
