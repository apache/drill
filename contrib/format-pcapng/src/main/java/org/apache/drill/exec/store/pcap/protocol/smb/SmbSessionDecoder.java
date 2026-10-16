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
package org.apache.drill.exec.store.pcap.protocol.smb;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.store.pcap.decoder.TcpSession;
import org.apache.drill.exec.store.pcap.protocol.DecoderContext;
import org.apache.drill.exec.store.pcap.protocol.SessionProtocolDecoder;
import org.apache.drill.exec.store.pcap.protocol.TcpStream;
import org.apache.drill.exec.store.pcap.protocol.ntlm.NtlmMessage;
import org.apache.drill.exec.store.pcap.protocol.ntlm.NtlmParser;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;

/**
 * The cleartext negotiation and authentication of an SMB2/SMB3 session (MS-SMB2) on TCP ports 445 and
 * 139. Frames are a 4-byte NetBIOS/Direct-TCP length followed by one or more SMB2 messages (magic
 * {@code 0xFE 'SMB'}), which may be compounded through NextCommand. The decoder reports the negotiated
 * and offered dialects, signing and encryption state, the server and client GUIDs, and the user, domain
 * and workstation taken from the NTLMSSP blob inside the SPNEGO token of SESSION_SETUP.
 *
 * <p>An SMB1 negotiate ({@code 0xFF 'SMB'}) that is upgraded to SMB2 still matches; a session that never
 * leaves SMB1 is reported only as {@code dialect = SMB1}. Once an SMB3 transform header ({@code 0xFD 'SMB'})
 * is seen the rest of that direction is encrypted and is not read. Kerberos authentication is reported as
 * {@code auth_type = kerberos} with no identity fields, since the ticket is opaque. No challenges, NT/LM
 * responses or session keys are ever read out, in keeping with the NTLM helper's identity-only contract.</p>
 */
public class SmbSessionDecoder implements SessionProtocolDecoder<SmbSession> {

  static final int MAX_MESSAGES = 1000;
  static final int MAX_COMPOUNDED = 64;
  static final int MAX_DIALECTS = 64;
  private static final int HEADER_SIZE = 64;

  private static final int CMD_NEGOTIATE = 0;
  private static final int CMD_SESSION_SETUP = 1;
  private static final int FLAG_RESPONSE = 0x00000001;
  private static final int SIGNING_REQUIRED = 0x0002;

  // The Kerberos v5 mechanism OID 1.2.840.113554.1.2.2, as it appears DER-encoded in a SPNEGO token.
  private static final byte[] KERBEROS_OID = {0x2a, (byte) 0x86, 0x48, (byte) 0x86, (byte) 0xf7, 0x12, 0x01, 0x02, 0x02};

  @Override
  public String protocol() {
    return "smb";
  }

  @Override
  public void defineSchema(SchemaBuilder fields) {
    fields.addNullable("dialect", MinorType.VARCHAR)
        .addArray("client_dialects", MinorType.VARCHAR)
        .addNullable("signing_required", MinorType.BIT)
        .addNullable("encryption", MinorType.BIT)
        .addNullable("server_guid", MinorType.VARCHAR)
        .addNullable("client_guid", MinorType.VARCHAR)
        .addNullable("auth_type", MinorType.VARCHAR)
        .addNullable("user_name", MinorType.VARCHAR)
        .addNullable("domain_name", MinorType.VARCHAR)
        .addNullable("workstation", MinorType.VARCHAR)
        .addNullable("ntlm_version", MinorType.VARCHAR);
  }

  @Override
  public boolean accepts(TcpSession session) {
    return isSmbPort(session.getSrcPort()) || isSmbPort(session.getDstPort());
  }

  private static boolean isSmbPort(int port) {
    return port == 445 || port == 139;
  }

  @Override
  public SmbSession parse(TcpStream fromClient, TcpStream fromServer, DecoderContext context) {
    SmbSession s = parseStreams(fromClient.data(), fromServer.data(), context);
    if (s == null) {
      return null;
    }
    if (fromClient.hasGaps()) {
      context.warn("stopped at missing data in client stream at byte " + fromClient.firstGap());
    }
    if (fromServer.hasGaps()) {
      context.warn("stopped at missing data in server stream at byte " + fromServer.firstGap());
    }
    return s;
  }

  /**
   * @return null if neither direction starts with an SMB frame; otherwise the parsed session, whose
   *         dialect is {@code SMB1} when no SMB2 message was seen
   */
  static SmbSession parseStreams(byte[] client, byte[] server, DecoderContext context) {
    if (firstFrameMagic(client) == 0 && firstFrameMagic(server) == 0) {
      return null;
    }
    Scan scan = new Scan();
    SmbSession s = new SmbSession();
    scan.run(client, true, s, context);
    scan.run(server, false, s, context);
    if (!scan.sawSmb2 && s.dialect == null) {
      s.dialect = "SMB1";
    }
    return s;
  }

  /** @return 1 for SMB1, 2 for SMB2, 3 for an SMB3 transform header, 0 for neither or no frame */
  private static int firstFrameMagic(byte[] data) {
    if (data.length < 8 || (data[0] & 0xFF) != 0) {
      return 0;
    }
    int body = 4;
    int b = data[body] & 0xFF;
    if (data[body + 1] == 'S' && data[body + 2] == 'M' && data[body + 3] == 'B') {
      if (b == 0xFE) {
        return 2;
      }
      if (b == 0xFF) {
        return 1;
      }
      if (b == 0xFD) {
        return 3;
      }
    }
    return 0;
  }

  /** Walks one direction's frames, filling the shared session. State that spans both directions lives here. */
  private static final class Scan {
    boolean sawSmb2;

    void run(byte[] data, boolean isClient, SmbSession s, DecoderContext context) {
      String direction = isClient ? "client" : "server";
      int pos = 0;
      int frames = 0;
      while (pos + 4 <= data.length && frames < MAX_MESSAGES) {
        if ((data[pos] & 0xFF) != 0) {
          break; // not a NetBIOS session message
        }
        int len = ((data[pos + 1] & 0xFF) << 16) | ((data[pos + 2] & 0xFF) << 8) | (data[pos + 3] & 0xFF);
        int frameStart = pos + 4;
        if (len <= 0) {
          break;
        }
        if (frameStart + len > data.length) {
          context.warn("truncated message in " + direction + " stream at byte " + pos);
          break;
        }
        if (!frame(data, frameStart, frameStart + len, isClient, s, direction, context)) {
          break; // encrypted or non-SMB2 content: nothing more is readable in this direction
        }
        pos = frameStart + len;
        frames++;
      }
    }

    /** @return false when the rest of the direction is no longer readable SMB2 (encrypted or SMB1) */
    private boolean frame(byte[] data, int start, int limit, boolean isClient, SmbSession s, String direction,
                          DecoderContext context) {
      int mstart = start;
      for (int i = 0; i < MAX_COMPOUNDED; i++) {
        if (limit - mstart < 4) {
          return true;
        }
        if (!(data[mstart + 1] == 'S' && data[mstart + 2] == 'M' && data[mstart + 3] == 'B')) {
          return true;
        }
        int magic = data[mstart] & 0xFF;
        if (magic == 0xFD) {
          s.encryption = true;
          return false; // transform header: the message content is opaque
        }
        if (magic == 0xFF) {
          return false; // SMB1 message; nothing to read for the SMB2 decoder
        }
        if (magic != 0xFE) {
          return true;
        }
        sawSmb2 = true;
        if (limit - mstart < HEADER_SIZE) {
          context.warn("truncated SMB2 header in " + direction + " stream at byte " + mstart);
          return true;
        }
        message(data, mstart, limit, isClient, s, context);
        long next = u32(data, mstart + 20);
        if (next == 0 || next < HEADER_SIZE || mstart + next >= limit) {
          return true;
        }
        mstart += (int) next;
      }
      return true;
    }

    private void message(byte[] data, int mstart, int limit, boolean isClient, SmbSession s, DecoderContext context) {
      int command = u16(data, mstart + 12);
      boolean isResponse = (u32(data, mstart + 16) & FLAG_RESPONSE) != 0;
      int body = mstart + HEADER_SIZE;
      if (command == CMD_NEGOTIATE) {
        if (isResponse) {
          negotiateResponse(data, body, limit, s);
        } else {
          negotiateRequest(data, body, limit, s);
        }
      } else if (command == CMD_SESSION_SETUP) {
        sessionSetup(data, mstart, body, limit, isResponse, s, context);
      }
    }

    private void negotiateRequest(byte[] data, int body, int limit, SmbSession s) {
      if (limit - body < 36) {
        return;
      }
      int dialectCount = u16(data, body + 2);
      if (limit - (body + 12) >= 16 && s.clientGuid == null) {
        s.clientGuid = guid(data, body + 12);
      }
      int dialects = body + 36;
      int count = Math.min(dialectCount, MAX_DIALECTS);
      s.clientDialects.clear();
      for (int i = 0; i < count; i++) {
        int at = dialects + i * 2;
        if (at + 2 > limit) {
          break;
        }
        s.clientDialects.add(dialectName(u16(data, at)));
      }
    }

    private void negotiateResponse(byte[] data, int body, int limit, SmbSession s) {
      if (limit - body < 24) {
        return;
      }
      int securityMode = u16(data, body + 2);
      int dialectRevision = u16(data, body + 4);
      s.dialect = dialectName(dialectRevision);
      s.signingRequired = (securityMode & SIGNING_REQUIRED) != 0;
      s.serverGuid = guid(data, body + 8);
    }

    private void sessionSetup(byte[] data, int mstart, int body, int limit, boolean isResponse, SmbSession s,
                              DecoderContext context) {
      int fieldBase = isResponse ? body + 4 : body + 12;
      if (fieldBase + 4 > limit) {
        return;
      }
      int secOffset = u16(data, fieldBase);
      int secLen = u16(data, fieldBase + 2);
      int blobStart = mstart + secOffset;
      if (secLen <= 0 || blobStart < body || blobStart + secLen > data.length) {
        return;
      }
      securityBlob(data, blobStart, blobStart + secLen, s, context);
    }

    private void securityBlob(byte[] data, int from, int to, SmbSession s, DecoderContext context) {
      int sig = NtlmParser.findSignature(data, from, to);
      if (sig >= 0) {
        NtlmMessage m = NtlmParser.parse(data, sig, to - sig);
        if (m != null) {
          s.authType = "ntlmssp";
          if (m.messageType == NtlmMessage.AUTHENTICATE) {
            if (notEmpty(m.userName)) {
              s.userName = m.userName;
            }
            if (notEmpty(m.domainName)) {
              s.domainName = m.domainName;
            }
            if (notEmpty(m.workstation)) {
              s.workstation = m.workstation;
            }
            if (m.ntlmVersion != null) {
              s.ntlmVersion = m.ntlmVersion;
            }
          }
          return;
        }
        context.warn("unparseable NTLMSSP blob in session setup");
        return;
      }
      if (s.authType == null && contains(data, from, to, KERBEROS_OID)) {
        s.authType = "kerberos";
      }
    }
  }

  private static boolean notEmpty(String s) {
    return s != null && !s.isEmpty();
  }

  private static boolean contains(byte[] data, int from, int to, byte[] pattern) {
    int limit = to - pattern.length;
    for (int i = from; i <= limit; i++) {
      boolean match = true;
      for (int j = 0; j < pattern.length; j++) {
        if (data[i + j] != pattern[j]) {
          match = false;
          break;
        }
      }
      if (match) {
        return true;
      }
    }
    return false;
  }

  private static String dialectName(int revision) {
    switch (revision) {
      case 0x0202:
        return "2.0.2";
      case 0x0210:
        return "2.1.0";
      case 0x0222:
        return "2.2.2";
      case 0x0224:
        return "2.2.4";
      case 0x0300:
        return "3.0.0";
      case 0x0302:
        return "3.0.2";
      case 0x0310:
        return "3.1.0";
      case 0x0311:
        return "3.1.1";
      case 0x02FF:
        return "2.???";
      default:
        return String.format("0x%04x", revision);
    }
  }

  /** Formats 16 bytes as a little-endian mixed-endian GUID, as Windows and Wireshark display SMB GUIDs. */
  private static String guid(byte[] d, int at) {
    int[] order = {3, 2, 1, 0, 5, 4, 7, 6, 8, 9, 10, 11, 12, 13, 14, 15};
    StringBuilder sb = new StringBuilder(36);
    for (int i = 0; i < order.length; i++) {
      if (i == 4 || i == 6 || i == 8 || i == 10) {
        sb.append('-');
      }
      sb.append(String.format("%02x", d[at + order[i]] & 0xFF));
    }
    return sb.toString();
  }

  private static int u16(byte[] b, int at) {
    return (b[at] & 0xFF) | ((b[at + 1] & 0xFF) << 8);
  }

  private static long u32(byte[] b, int at) {
    return (b[at] & 0xFFL) | ((b[at + 1] & 0xFFL) << 8) | ((b[at + 2] & 0xFFL) << 16) | ((b[at + 3] & 0xFFL) << 24);
  }

  @Override
  public void write(SmbSession s, TupleWriter fields) {
    setString(fields, "dialect", s.dialect);
    if (!s.clientDialects.isEmpty()) {
      ArrayWriter dialects = fields.array("client_dialects");
      for (String d : s.clientDialects) {
        dialects.scalar().setString(d);
      }
    }
    if (s.signingRequired != null) {
      fields.scalar("signing_required").setBoolean(s.signingRequired);
    }
    fields.scalar("encryption").setBoolean(s.encryption);
    setString(fields, "server_guid", s.serverGuid);
    setString(fields, "client_guid", s.clientGuid);
    setString(fields, "auth_type", s.authType);
    setString(fields, "user_name", s.userName);
    setString(fields, "domain_name", s.domainName);
    setString(fields, "workstation", s.workstation);
    setString(fields, "ntlm_version", s.ntlmVersion);
  }

  private static void setString(TupleWriter w, String name, String value) {
    if (value != null) {
      w.scalar(name).setString(value);
    }
  }
}
