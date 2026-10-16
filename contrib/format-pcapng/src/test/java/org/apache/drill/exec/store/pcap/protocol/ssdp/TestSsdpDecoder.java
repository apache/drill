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
package org.apache.drill.exec.store.pcap.protocol.ssdp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.drill.exec.store.pcap.protocol.DecoderContext;
import org.apache.drill.exec.store.pcap.protocol.TestPackets;
import org.apache.drill.test.BaseTest;
import org.junit.Test;

public class TestSsdpDecoder extends BaseTest {

  private static final class Context implements DecoderContext {
    final List<String> warnings = new ArrayList<>();

    @Override
    public boolean exposeCredentials() {
      return false;
    }

    @Override
    public void warn(String message) {
      warnings.add(message);
    }
  }

  private static SsdpMessage parse(String text, Context context) {
    return SsdpParser.parse(text.getBytes(StandardCharsets.ISO_8859_1), context);
  }

  @Test
  public void testSearch() {
    SsdpMessage m = parse("M-SEARCH * HTTP/1.1\r\nHOST: 239.255.255.250:1900\r\nMAN: \"ssdp:discover\"\r\n"
        + "MX: 2\r\nST: ssdp:all\r\nUSER-AGENT: Linux/5 UPnP/1.1 test/1.0\r\n\r\n", new Context());
    assertEquals("M-SEARCH", m.method);
    assertNull(m.statusCode);
    assertEquals("\"ssdp:discover\"", m.man);
    assertEquals(2, (int) m.mx);
    assertEquals("ssdp:all", m.st);
    assertEquals("Linux/5 UPnP/1.1 test/1.0", m.userAgent);
    assertEquals(5, m.headers.size());
    assertEquals("HOST", m.headers.get(0)[0]);
  }

  @Test
  public void testNotifyWithBareLineFeeds() {
    SsdpMessage m = parse("NOTIFY * HTTP/1.1\nHost: 239.255.255.250:1900\nCache-Control: max-age=1800\n"
        + "Location: http://192.168.1.10:49152/desc.xml\nNT: upnp:rootdevice\nNTS: ssdp:alive\n"
        + "Server: Linux/3.14 UPnP/1.0 IpBridge/1.26.0\nUSN: uuid:2f402f80-da50-11e1-9b23-001788255acc::upnp:rootdevice\n\n",
        new Context());
    assertEquals("NOTIFY", m.method);
    assertEquals("max-age=1800", m.cacheControl);
    assertEquals("http://192.168.1.10:49152/desc.xml", m.location);
    assertEquals("upnp:rootdevice", m.nt);
    assertEquals("ssdp:alive", m.nts);
    assertEquals("Linux/3.14 UPnP/1.0 IpBridge/1.26.0", m.server);
    assertEquals("uuid:2f402f80-da50-11e1-9b23-001788255acc::upnp:rootdevice", m.usn);
  }

  @Test
  public void testResponse() {
    Context context = new Context();
    SsdpMessage m = parse("HTTP/1.1 200 OK\r\nCACHE-CONTROL: max-age=100\r\nEXT:\r\nST: upnp:rootdevice\r\nMX: x\r\n\r\n",
        context);
    assertNull(m.method);
    assertEquals(200, (int) m.statusCode);
    assertEquals("upnp:rootdevice", m.st);
    assertEquals("", m.headers.get(1)[1]);
    assertNull(m.mx);
    assertEquals(1, context.warnings.size());
  }

  @Test
  public void testHeaderCap() {
    StringBuilder text = new StringBuilder("NOTIFY * HTTP/1.1\r\n");
    for (int i = 0; i < 70; i++) {
      text.append("X-").append(i).append(": v\r\n");
    }
    Context context = new Context();
    SsdpMessage m = parse(text.append("NT: last\r\n\r\n").toString(), context);
    assertEquals(64, m.headers.size());
    assertEquals("last", m.nt);
    assertEquals(1, context.warnings.size());
  }

  @Test
  public void testNotSsdp() {
    assertNull(parse("GET / HTTP/1.1\r\n\r\n", new Context()));
    assertNull(parse("M-SEARCH /x HTTP/1.1\r\n\r\n", new Context()));
    assertNull(parse("hello", new Context()));
    assertNull(SsdpParser.parse(new byte[] {1, 2, 3, 4}, new Context()));
  }

  @Test
  public void testMalformedHeader() {
    try {
      parse("NOTIFY * HTTP/1.1\r\nNT: upnp:rootdevice\r\nthis is not a header\r\n\r\n", new Context());
      fail("expected malformed SSDP");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("malformed header line 2"));
    }
  }

  @Test
  public void testDecoderAccepts() {
    SsdpDecoder decoder = new SsdpDecoder();
    byte[] data = "NOTIFY * HTTP/1.1\r\n\r\n".getBytes(StandardCharsets.ISO_8859_1);
    assertTrue(decoder.accepts(TestPackets.udp("10.0.0.1", 40000, "239.255.255.250", 1900, data)));
    assertTrue(!decoder.accepts(TestPackets.udp("10.0.0.1", 40000, "10.0.0.2", 1901, data)));
    assertTrue(!decoder.accepts(TestPackets.tcp("10.0.0.1", 40000, "10.0.0.2", 1900, 1, TestPackets.ACK, data)));
  }
}
