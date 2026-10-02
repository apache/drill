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
package org.apache.drill.exec.store.pcap.protocol.rdp;

import java.nio.charset.StandardCharsets;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.store.pcap.decoder.TcpSession;
import org.apache.drill.exec.store.pcap.protocol.DecoderContext;
import org.apache.drill.exec.store.pcap.protocol.SessionProtocolDecoder;
import org.apache.drill.exec.store.pcap.protocol.TcpStream;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;

/**
 * RDP (TCP port 3389). The X.224 Connection Request, the first client PDU, carries the username the client
 * offers in clear in its {@code Cookie: mstshash=...} routing token and the security protocols it requests
 * in the RDP Negotiation Request. The server's Connection Confirm carries the selected protocol or a
 * negotiation failure. Everything after this exchange is normally TLS, so only the initial clear bytes are
 * read.
 */
public class RdpSessionDecoder implements SessionProtocolDecoder<RdpSession> {
  static final int PORT = 3389;
  static final int MAX_STRING = 4096;

  private static final String COOKIE_PREFIX = "Cookie: mstshash=";
  private static final int TPKT_VERSION = 3;
  private static final int X224_CR = 0xE0;
  private static final int X224_CC = 0xD0;
  private static final int RDP_NEG_REQ = 0x01;
  private static final int RDP_NEG_RSP = 0x02;
  private static final int RDP_NEG_FAILURE = 0x03;
  /** 4 TPKT bytes + X.224 fixed header (LI, code, DST-REF, SRC-REF, class). */
  private static final int USER_DATA_OFFSET = 11;

  @Override
  public String protocol() {
    return "rdp";
  }

  @Override
  public void defineSchema(SchemaBuilder fields) {
    fields.addNullable("cookie", MinorType.VARCHAR)
        .addArray("requested_protocols", MinorType.VARCHAR)
        .addNullable("selected_protocol", MinorType.VARCHAR)
        .addNullable("negotiation_failure", MinorType.VARCHAR);
  }

  @Override
  public boolean accepts(TcpSession session) {
    return session.getSrcPort() == PORT || session.getDstPort() == PORT;
  }

  @Override
  public RdpSession parse(TcpStream fromClient, TcpStream fromServer, DecoderContext context) {
    return parseStreams(fromClient.data(), fromServer.data(), context);
  }

  /** @return null if the client stream does not open with an X.224 Connection Request. */
  static RdpSession parseStreams(byte[] client, byte[] server, DecoderContext context) {
    if (!isConnectionRequest(client)) {
      return null;
    }
    RdpSession s = new RdpSession();
    parseRequest(client, s);
    parseConfirm(server, s);
    return s;
  }

  /** True if the client stream opens with a TPKT version 3 wrapping an X.224 Connection Request. */
  private static boolean isConnectionRequest(byte[] data) {
    return data.length >= 6 && (data[0] & 0xFF) == TPKT_VERSION && (data[1] & 0xFF) == 0
        && (data[5] & 0xF0) == X224_CR;
  }

  private static int tpktLimit(byte[] data) {
    int len = ((data[2] & 0xFF) << 8) | (data[3] & 0xFF);
    return Math.min(len, data.length);
  }

  /** @throws IllegalArgumentException if the request is RDP but its negotiation block is malformed. */
  private static void parseRequest(byte[] data, RdpSession s) {
    int limit = tpktLimit(data);
    int p = USER_DATA_OFFSET;
    if (startsWith(data, p, limit, COOKIE_PREFIX)) {
      p += COOKIE_PREFIX.length();
      int crlf = indexOfCrlf(data, p, limit);
      if (crlf < 0) {
        throw new IllegalArgumentException("unterminated mstshash cookie");
      }
      s.cookie = cap(new String(data, p, crlf - p, StandardCharsets.US_ASCII));
      p = crlf + 2;
    }
    if (p < limit && (data[p] & 0xFF) == RDP_NEG_REQ) {
      if (p + 8 > limit) {
        throw new IllegalArgumentException("truncated negotiation request");
      }
      long flags = u32le(data, p + 4);
      addRequestedProtocols(s, flags);
    }
  }

  private static void parseConfirm(byte[] data, RdpSession s) {
    if (data.length < 6 || (data[0] & 0xFF) != TPKT_VERSION || (data[1] & 0xFF) != 0
        || (data[5] & 0xF0) != X224_CC) {
      return;
    }
    int limit = tpktLimit(data);
    int p = USER_DATA_OFFSET;
    if (p + 8 > limit) {
      return;
    }
    int type = data[p] & 0xFF;
    long value = u32le(data, p + 4);
    if (type == RDP_NEG_RSP) {
      s.selectedProtocol = selectedProtocolName(value);
    } else if (type == RDP_NEG_FAILURE) {
      s.negotiationFailure = failureName(value);
    }
  }

  private static void addRequestedProtocols(RdpSession s, long flags) {
    if ((flags & 0x01) != 0) {
      s.requestedProtocols.add("TLS");
    }
    if ((flags & 0x02) != 0) {
      s.requestedProtocols.add("CredSSP");
    }
    if ((flags & 0x04) != 0) {
      s.requestedProtocols.add("RDSTLS");
    }
    if ((flags & 0x08) != 0) {
      s.requestedProtocols.add("HYBRID_EX");
    }
    if (s.requestedProtocols.isEmpty()) {
      s.requestedProtocols.add("RDP");
    }
  }

  private static String selectedProtocolName(long value) {
    if (value == 0) {
      return "RDP";
    } else if (value == 0x01) {
      return "TLS";
    } else if (value == 0x02) {
      return "CredSSP";
    } else if (value == 0x04) {
      return "RDSTLS";
    } else if (value == 0x08) {
      return "HYBRID_EX";
    }
    return "0x" + Long.toHexString(value);
  }

  private static String failureName(long value) {
    switch ((int) value) {
      case 1: return "SSL_REQUIRED_BY_SERVER";
      case 2: return "SSL_NOT_ALLOWED_BY_SERVER";
      case 3: return "SSL_CERT_NOT_ON_SERVER";
      case 4: return "INCONSISTENT_FLAGS";
      case 5: return "HYBRID_REQUIRED_BY_SERVER";
      case 6: return "SSL_WITH_USER_AUTH_REQUIRED_BY_SERVER";
      default: return "FAILURE_" + value;
    }
  }

  private static boolean startsWith(byte[] data, int at, int limit, String prefix) {
    if (limit - at < prefix.length()) {
      return false;
    }
    for (int i = 0; i < prefix.length(); i++) {
      if ((data[at + i] & 0xFF) != prefix.charAt(i)) {
        return false;
      }
    }
    return true;
  }

  private static int indexOfCrlf(byte[] data, int from, int limit) {
    for (int i = from; i + 1 < limit; i++) {
      if (data[i] == '\r' && data[i + 1] == '\n') {
        return i;
      }
    }
    return -1;
  }

  private static long u32le(byte[] b, int at) {
    return (b[at] & 0xFFL) | ((b[at + 1] & 0xFFL) << 8) | ((b[at + 2] & 0xFFL) << 16) | ((b[at + 3] & 0xFFL) << 24);
  }

  private static String cap(String s) {
    return s.length() > MAX_STRING ? s.substring(0, MAX_STRING) : s;
  }

  @Override
  public void write(RdpSession s, TupleWriter fields) {
    setString(fields, "cookie", s.cookie);
    ArrayWriter requested = fields.array("requested_protocols");
    for (String p : s.requestedProtocols) {
      requested.scalar().setString(p);
    }
    setString(fields, "selected_protocol", s.selectedProtocol);
    setString(fields, "negotiation_failure", s.negotiationFailure);
  }

  private static void setString(TupleWriter w, String name, String value) {
    if (value != null) {
      w.scalar(name).setString(value);
    }
  }
}
