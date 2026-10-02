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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;

import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.pcap.decoder.Packet;
import org.apache.drill.test.BaseTest;
import org.junit.Test;

public class TestProtocolDecoders extends BaseTest {

  private static Packet echo(String text) {
    return TestPackets.udp("10.0.0.1", 4000, "10.0.0.2", 7, text.getBytes(StandardCharsets.US_ASCII));
  }

  private static DecodeResult decode(ProtocolDecoders decoders, Packet packet, RowDecoderContext context) {
    return decoders.decodePacket(packet, packet.getData(), context);
  }

  @Test
  public void testServiceLoaderFindsTestDecoder() {
    assertTrue(ProtocolDecoders.get().packetDecoders().stream().anyMatch(d -> d.protocol().equals("echo_test")));
    assertNotNull(ProtocolDecoders.get().packetDataSchema().metadata("echo_test"));
  }

  @Test
  public void testDecoderFieldsSizedSparse() {
    // Most rows have no decoded data, so vectors start small instead of reserving room for every row
    TupleMetadata dns = ProtocolDecoders.get().packetDataSchema().metadata("dns").tupleSchema();
    ColumnMetadata answers = dns.metadata("answers");
    assertEquals(1, answers.expectedElementCount());
    assertEquals(ProtocolDecoders.SPARSE_WIDTH, answers.tupleSchema().metadata("data").expectedWidth());
  }

  @Test
  public void testParsed() {
    ProtocolDecoders decoders = new ProtocolDecoders(Collections.singletonList(new EchoTestDecoder()));
    RowDecoderContext context = new RowDecoderContext(false);
    DecodeResult result = decode(decoders, echo("ECHO:hi"), context);
    assertEquals("echo_test", result.protocol());
    assertEquals("hi", result.parsed());
    assertNull(result.error());
    assertTrue(context.warnings().isEmpty());
  }

  @Test
  public void testNotThisProtocol() {
    ProtocolDecoders decoders = new ProtocolDecoders(Collections.singletonList(new EchoTestDecoder()));
    assertNull(decode(decoders, echo("hello"), new RowDecoderContext(false)));
    Packet otherPort = TestPackets.udp("10.0.0.1", 4000, "10.0.0.2", 9, "ECHO:hi".getBytes(StandardCharsets.US_ASCII));
    assertNull(decode(decoders, otherPort, new RowDecoderContext(false)));
  }

  @Test
  public void testParseFailureClaimsPacket() {
    ProtocolDecoders decoders = new ProtocolDecoders(Collections.singletonList(new EchoTestDecoder()));
    DecodeResult result = decode(decoders, echo("ECHO!x"), new RowDecoderContext(false));
    assertEquals("echo_test", result.protocol());
    assertNull(result.parsed());
    assertEquals("echo_test: bad echo", result.error());
  }

  @Test
  public void testWarningsArePrefixed() {
    ProtocolDecoders decoders = new ProtocolDecoders(Collections.singletonList(new EchoTestDecoder()));
    RowDecoderContext context = new RowDecoderContext(false);
    decode(decoders, echo("ECHO:a long echo text"), context);
    assertEquals(Collections.singletonList("echo_test: long text"), context.warnings());
  }

  @Test
  public void testDuplicateProtocolKeepsFirst() {
    EchoTestDecoder first = new EchoTestDecoder();
    ProtocolDecoders decoders = new ProtocolDecoders(Arrays.asList(first, new EchoTestDecoder()));
    assertEquals(1, decoders.packetDecoders().size());
  }

  @Test
  public void testHigherPriorityFirst() {
    EchoTestDecoder low = new EchoTestDecoder() {
      @Override
      public String protocol() {
        return "low";
      }
    };
    EchoTestDecoder high = new EchoTestDecoder() {
      @Override
      public String protocol() {
        return "high";
      }

      @Override
      public int priority() {
        return 10;
      }
    };
    ProtocolDecoders decoders = new ProtocolDecoders(Arrays.asList(low, high));
    assertEquals("high", decode(decoders, echo("ECHO:hi"), new RowDecoderContext(false)).protocol());
  }

  @Test
  public void testEachReaderGetsItsOwnDataSchema() {
    // Map metadata binds its member schema to a parent, so readers must not share one instance
    ProtocolDecoders decoders = ProtocolDecoders.get();
    SchemaBuilder first = new SchemaBuilder();
    ProtocolColumns.addColumns(first, ProtocolColumns.Mode.PACKET, decoders);
    SchemaBuilder second = new SchemaBuilder();
    ProtocolColumns.addColumns(second, ProtocolColumns.Mode.PACKET, decoders);
    TupleMetadata a = first.buildSchema().metadata(ProtocolColumns.PARSED_DATA).tupleSchema();
    TupleMetadata b = second.buildSchema().metadata(ProtocolColumns.PARSED_DATA).tupleSchema();
    assertNotSame(a, b);
    assertNotSame(decoders.packetDataSchema(), a);
    assertEquals(decoders.packetDataSchema().size(), a.size());
    // The copy keeps the sparse sizing that keeps batches within their memory budget
    assertEquals(1, a.metadata("dns").tupleSchema().metadata("answers").expectedElementCount());
  }
}
