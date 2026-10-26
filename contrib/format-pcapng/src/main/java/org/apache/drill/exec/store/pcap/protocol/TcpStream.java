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

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.drill.exec.store.pcap.decoder.Packet;
import org.apache.drill.exec.store.pcap.decoder.TcpSession;

/**
 * The reassembled bytes of one direction of a TCP session: ordered by sequence number,
 * retransmissions removed, and cut at the first missing range.
 */
public final class TcpStream {
  private final byte[] data;
  private final long firstGap;

  private TcpStream(byte[] data, long firstGap) {
    this.data = data;
    this.firstGap = firstGap;
  }

  /** Contiguous bytes from the start of the stream up to the first gap. */
  public byte[] data() {
    return data;
  }

  public boolean hasGaps() {
    return firstGap >= 0;
  }

  /** Offset of the first missing byte, or -1. */
  public long firstGap() {
    return firstGap;
  }

  /** The client stream and the server stream of a session, in that order. */
  public static TcpStream[] clientServer(TcpSession session) {
    List<Packet> sender = session.getPacketsFromSender();
    List<Packet> receiver = session.getPacketsFromReceiver();
    boolean senderIsClient;
    if (hasInitialSyn(receiver)) {
      senderIsClient = false;
    } else if (hasInitialSyn(sender)) {
      senderIsClient = true;
    } else {
      // No handshake captured: the endpoint with the lower port is taken as the server
      senderIsClient = session.getSrcPort() > session.getDstPort();
    }
    TcpStream fromSender = of(sender);
    TcpStream fromReceiver = of(receiver);
    return senderIsClient ? new TcpStream[] {fromSender, fromReceiver} : new TcpStream[] {fromReceiver, fromSender};
  }

  private static boolean hasInitialSyn(List<Packet> packets) {
    for (Packet p : packets) {
      if (p.getSynFlag() && !p.getAckFlag()) {
        return true;
      }
    }
    return false;
  }

  public static TcpStream of(List<Packet> packets) {
    if (packets.isEmpty()) {
      return new TcpStream(new byte[0], -1);
    }
    // Sequence numbers relative to the first packet, as signed 32-bit differences, handle wraparound
    long reference = packets.get(0).getSequenceNumber() & 0xFFFFFFFFL;
    Integer base = null;
    for (Packet p : packets) {
      if (p.getSynFlag()) {
        base = relative(p, reference) + 1;
        break;
      }
    }
    List<long[]> ranges = new ArrayList<>();
    List<byte[]> payloads = new ArrayList<>();
    for (Packet p : packets) {
      byte[] payload = p.getData();
      if (payload == null || payload.length == 0) {
        continue;
      }
      ranges.add(new long[] {relative(p, reference), payloads.size()});
      payloads.add(payload);
    }
    if (payloads.isEmpty()) {
      return new TcpStream(new byte[0], -1);
    }
    ranges.sort((a, b) -> Long.compare(a[0], b[0]));
    long start = base != null ? base : ranges.get(0)[0];
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    long next = start;
    for (long[] range : ranges) {
      byte[] payload = payloads.get((int) range[1]);
      long segmentStart = range[0];
      long segmentEnd = segmentStart + payload.length;
      if (segmentEnd <= next) {
        continue; // retransmission of bytes already taken
      }
      if (segmentStart > next) {
        return new TcpStream(out.toByteArray(), next - start);
      }
      int skip = (int) (next - segmentStart);
      out.write(payload, skip, payload.length - skip);
      next = segmentEnd;
    }
    return new TcpStream(out.toByteArray(), -1);
  }

  private static int relative(Packet p, long reference) {
    return (int) ((p.getSequenceNumber() & 0xFFFFFFFFL) - reference);
  }
}
