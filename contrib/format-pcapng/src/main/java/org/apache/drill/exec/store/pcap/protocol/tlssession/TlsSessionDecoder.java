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
package org.apache.drill.exec.store.pcap.protocol.tlssession;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.store.pcap.decoder.TcpSession;
import org.apache.drill.exec.store.pcap.protocol.DecoderContext;
import org.apache.drill.exec.store.pcap.protocol.SessionProtocolDecoder;
import org.apache.drill.exec.store.pcap.protocol.TcpStream;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;

/**
 * The cleartext TLS handshake of a TCP session: ClientHello, ServerHello and, for TLS 1.2 and earlier,
 * the server's certificate chain. Nothing after the switch to encryption is read.
 */
public class TlsSessionDecoder implements SessionProtocolDecoder<TlsHandshake> {
  /** HTTPS, SMTPS, NNTPS, LDAPS, DNS over TLS, FTPS, telnets, IMAPS, IRCS, POP3S, SIP over TLS, alternate HTTPS. */
  static final Set<Integer> PORTS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
      443, 465, 563, 636, 853, 989, 990, 992, 993, 994, 995, 5061, 8443)));

  private static final int CLIENT_HELLO = 1;
  private static final int SERVER_HELLO = 2;
  private static final int CERTIFICATE = 11;
  private static final int SERVER_HELLO_DONE = 14;

  @Override
  public String protocol() {
    return "tls";
  }

  @Override
  public void defineSchema(SchemaBuilder fields) {
    fields.addNullable("client_version", MinorType.VARCHAR)
        .addArray("client_supported_versions", MinorType.VARCHAR)
        .addNullable("server_version", MinorType.VARCHAR)
        .addNullable("sni", MinorType.VARCHAR)
        .addArray("alpn_offered", MinorType.VARCHAR)
        .addNullable("alpn_selected", MinorType.VARCHAR)
        .addNullable("cipher_suite", MinorType.INT)
        .addNullable("cipher_suite_name", MinorType.VARCHAR)
        .addNullable("session_resumed", MinorType.BIT)
        .addNullable("hello_retry_request", MinorType.BIT)
        .addNullable("certificate_encrypted", MinorType.BIT)
        .addNullable("certificate_count", MinorType.INT)
        .addMapArray("certificates")
          .addNullable("subject", MinorType.VARCHAR)
          .addNullable("issuer", MinorType.VARCHAR)
          .addNullable("serial", MinorType.VARCHAR)
          .addNullable("not_before", MinorType.TIMESTAMP)
          .addNullable("not_after", MinorType.TIMESTAMP)
          .addArray("subject_alt_names", MinorType.VARCHAR)
          .addNullable("signature_algorithm", MinorType.VARCHAR)
          .addNullable("public_key_algorithm", MinorType.VARCHAR)
          .addNullable("public_key_bits", MinorType.INT)
          .addNullable("sha256", MinorType.VARCHAR)
          .addNullable("is_self_signed", MinorType.BIT)
          .resumeSchema();
  }

  @Override
  public boolean accepts(TcpSession session) {
    return PORTS.contains(session.getSrcPort()) || PORTS.contains(session.getDstPort());
  }

  @Override
  public TlsHandshake parse(TcpStream fromClient, TcpStream fromServer, DecoderContext context) {
    return parseStreams(fromClient.data(), fromClient.firstGap(), fromServer.data(), fromServer.firstGap(), context);
  }

  /**
   * @param clientGap offset of the first missing byte of the client stream, or -1
   * @param serverGap the same for the server stream
   * @return null if the session is not TLS
   */
  static TlsHandshake parseStreams(byte[] client, long clientGap, byte[] server, long serverGap,
                                   DecoderContext context) {
    if (client.length > 0 ? !TlsRecordReader.startsWith(client, CLIENT_HELLO)
        : !TlsRecordReader.startsWith(server, SERVER_HELLO)) {
      return null;
    }
    TlsHandshake h = new TlsHandshake();
    readClient(client, clientGap, h, context);
    readServer(server, serverGap, h, context);
    if (h.serverVersion != null) {
      boolean tls13 = h.selectedVersion == TlsParser.TLS13;
      h.certificateEncrypted = tls13;
      if (tls13) {
        // The legacy session id is always echoed in TLS 1.3; resumption shows as an accepted pre-shared key
        h.sessionResumed = h.pskAccepted;
      } else if (h.clientHelloSeen) {
        h.sessionResumed = h.clientSessionId.length > 0 && Arrays.equals(h.clientSessionId, h.serverSessionId);
      }
    }
    return h;
  }

  private static void readClient(byte[] data, long gap, TlsHandshake h, DecoderContext context) {
    TlsRecordReader reader = new TlsRecordReader(data, "client", context);
    TlsRecordReader.Message m = reader.next();
    if (m != null && m.type == CLIENT_HELLO) {
      try {
        TlsParser.parseClientHello(m.body, h, context);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("malformed ClientHello: " + e.getMessage());
      }
      return; // Nothing else the client sends is decoded
    }
    endOfStream(reader, "client", gap, context);
  }

  private static void readServer(byte[] data, long gap, TlsHandshake h, DecoderContext context) {
    TlsRecordReader reader = new TlsRecordReader(data, "server", context);
    for (TlsRecordReader.Message m = reader.next(); m != null; m = reader.next()) {
      switch (m.type) {
        case SERVER_HELLO:
          if (h.serverVersion != null && !Boolean.TRUE.equals(h.helloRetryRequest)) {
            break;
          }
          try {
            // After a HelloRetryRequest the real ServerHello follows a compatibility ChangeCipherSpec
            reader.continueAfterChangeCipherSpec(TlsParser.parseServerHello(m.body, h));
          } catch (IllegalArgumentException e) {
            context.warn("malformed ServerHello: " + e.getMessage());
            return;
          }
          break;
        case CERTIFICATE:
          if (h.selectedVersion == TlsParser.TLS13 || h.certificateCount != null) {
            break;
          }
          try {
            TlsParser.parseCertificates(m.body, h, context);
          } catch (IllegalArgumentException e) {
            h.certificates.clear();
            context.warn("malformed Certificate: " + e.getMessage());
            return;
          }
          break;
        case SERVER_HELLO_DONE:
          return;
        default:
          break;
      }
    }
    endOfStream(reader, "server", gap, context);
  }

  private static void endOfStream(TlsRecordReader reader, String direction, long gap, DecoderContext context) {
    if (!reader.reachedEnd()) {
      return;
    }
    if (gap >= 0) {
      context.warn("stopped at missing data in " + direction + " stream at byte " + gap);
    } else if (reader.endedInside() != null) {
      context.warn(direction + " stream ends inside a handshake " + reader.endedInside());
    }
  }

  @Override
  public void write(TlsHandshake h, TupleWriter fields) {
    setString(fields, "client_version", h.clientVersion);
    writeStrings(fields.array("client_supported_versions"), h.clientSupportedVersions);
    setString(fields, "server_version", h.serverVersion);
    setString(fields, "sni", h.sni);
    writeStrings(fields.array("alpn_offered"), h.alpnOffered);
    setString(fields, "alpn_selected", h.alpnSelected);
    if (h.cipherSuite != null) {
      fields.scalar("cipher_suite").setInt(h.cipherSuite);
    }
    setString(fields, "cipher_suite_name", h.cipherSuiteName);
    setBoolean(fields, "session_resumed", h.sessionResumed);
    setBoolean(fields, "hello_retry_request", h.helloRetryRequest);
    setBoolean(fields, "certificate_encrypted", h.certificateEncrypted);
    if (h.certificateCount != null) {
      fields.scalar("certificate_count").setInt(h.certificateCount);
    }
    ArrayWriter certificates = fields.array("certificates");
    for (TlsCertificate c : h.certificates) {
      TupleWriter t = certificates.tuple();
      setString(t, "subject", c.subject);
      setString(t, "issuer", c.issuer);
      setString(t, "serial", c.serial);
      if (c.notBefore != null) {
        t.scalar("not_before").setTimestamp(c.notBefore);
      }
      if (c.notAfter != null) {
        t.scalar("not_after").setTimestamp(c.notAfter);
      }
      writeStrings(t.array("subject_alt_names"), c.subjectAltNames);
      setString(t, "signature_algorithm", c.signatureAlgorithm);
      setString(t, "public_key_algorithm", c.publicKeyAlgorithm);
      if (c.publicKeyBits != null) {
        t.scalar("public_key_bits").setInt(c.publicKeyBits);
      }
      setString(t, "sha256", c.sha256);
      setBoolean(t, "is_self_signed", c.isSelfSigned);
      certificates.save();
    }
  }

  private static void setString(TupleWriter fields, String name, String value) {
    if (value != null) {
      fields.scalar(name).setString(value);
    }
  }

  private static void setBoolean(TupleWriter fields, String name, Boolean value) {
    if (value != null) {
      fields.scalar(name).setBoolean(value);
    }
  }

  private static void writeStrings(ArrayWriter array, List<String> values) {
    ScalarWriter element = array.scalar();
    for (String v : values) {
      element.setString(v);
    }
  }
}
