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
package org.apache.drill.exec.store.pcap.protocol;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.apache.drill.exec.store.pcap.decoder.Packet;
import org.apache.drill.exec.store.pcap.decoder.TcpSession;
import org.apache.drill.test.BaseTest;
import org.junit.Test;

public class TestTcpStream extends BaseTest {

  private static Packet seg(long seq, String data) {
    return TestPackets.tcp("10.0.0.1", 40000, "10.0.0.2", 80, seq, TestPackets.ACK, data.getBytes(StandardCharsets.US_ASCII));
  }

  private static String text(TcpStream stream) {
    return new String(stream.data(), StandardCharsets.US_ASCII);
  }

  @Test
  public void testOutOfOrderAndRetransmitted() {
    TcpStream stream = TcpStream.of(Arrays.asList(seg(1006, "world"), seg(1000, "hello "), seg(1000, "hello "), seg(1003, "lo w")));
    assertEquals("hello world", text(stream));
    assertFalse(stream.hasGaps());
    assertEquals(-1, stream.firstGap());
  }

  @Test
  public void testGapStopsData() {
    TcpStream stream = TcpStream.of(Arrays.asList(seg(1000, "abc"), seg(1010, "xyz")));
    assertEquals("abc", text(stream));
    assertTrue(stream.hasGaps());
    assertEquals(3, stream.firstGap());
  }

  @Test
  public void testSequenceWraparound() {
    TcpStream stream = TcpStream.of(Arrays.asList(seg(0xFFFFFFFEL, "ab"), seg(0, "cd")));
    assertEquals("abcd", text(stream));
  }

  @Test
  public void testSynConsumesOneSequenceNumber() {
    Packet syn = TestPackets.tcp("10.0.0.1", 40000, "10.0.0.2", 80, 999, TestPackets.SYN, new byte[0]);
    TcpStream stream = TcpStream.of(Arrays.asList(seg(1005, "later"), syn, seg(1000, "first")));
    assertEquals("firstlater", text(stream));
  }

  @Test
  public void testEmpty() {
    assertArrayEquals(new byte[0], TcpStream.of(Arrays.asList()).data());
  }

  @Test
  public void testClientIsSynSender() {
    // First packet seen is from the server; the SYN identifies the client
    Packet fromServer = TestPackets.tcp("10.0.0.2", 80, "10.0.0.1", 40000, 5000, TestPackets.ACK,
        "HTTP/1.1 200 OK\r\n\r\n".getBytes(StandardCharsets.US_ASCII));
    Packet syn = TestPackets.tcp("10.0.0.1", 40000, "10.0.0.2", 80, 999, TestPackets.SYN, new byte[0]);
    Packet request = seg(1000, "GET / HTTP/1.1\r\n\r\n");
    TcpSession session = new TcpSession(fromServer.getSessionHash());
    session.addPacket(fromServer);
    session.addPacket(syn);
    session.addPacket(request);
    TcpStream[] streams = TcpStream.clientServer(session);
    assertTrue(text(streams[0]).startsWith("GET"));
    assertTrue(text(streams[1]).startsWith("HTTP"));
  }

  @Test
  public void testServerIsLowerPortWithoutHandshake() {
    Packet fromServer = TestPackets.tcp("10.0.0.2", 80, "10.0.0.1", 40000, 5000, TestPackets.ACK,
        "HTTP/1.1 200 OK\r\n\r\n".getBytes(StandardCharsets.US_ASCII));
    Packet request = seg(1000, "GET / HTTP/1.1\r\n\r\n");
    TcpSession session = new TcpSession(fromServer.getSessionHash());
    session.addPacket(fromServer);
    session.addPacket(request);
    TcpStream[] streams = TcpStream.clientServer(session);
    assertTrue(text(streams[0]).startsWith("GET"));
    assertEquals(2, streams.length);
  }
}
