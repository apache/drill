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
package org.apache.drill.exec.store.pcap.protocol.kerberos;

import java.util.HashMap;
import java.util.Map;

import org.apache.drill.exec.store.pcap.protocol.DecoderContext;
import org.apache.drill.exec.store.pcap.protocol.asn1.Asn1Reader;
import org.apache.drill.exec.store.pcap.protocol.asn1.Asn1Reader.Element;

/**
 * Parses the cleartext metadata of Kerberos AS-REQ, AS-REP, TGS-REQ, TGS-REP and KRB-ERROR messages
 * (RFC 4120) carried over UDP or a single TCP segment. Encrypted parts (the ticket's enc-part and
 * EncKDCRepPart) are never read; only their etype is recorded, which is the Kerberoasting signal.
 */
public final class KerberosParser {
  static final int MAX_ITEMS = 64;

  // Outer APPLICATION tags for the message types we decode.
  private static final int AS_REQ = 0x6A;    // [APPLICATION 10]
  private static final int AS_REP = 0x6B;    // [APPLICATION 11]
  private static final int TGS_REQ = 0x6C;   // [APPLICATION 12]
  private static final int TGS_REP = 0x6D;   // [APPLICATION 13]
  private static final int KRB_ERROR = 0x7E; // [APPLICATION 30]
  private static final int TICKET = 0x61;    // [APPLICATION 1]

  private static final Map<Integer, String> ERROR_NAMES = new HashMap<>();

  static {
    ERROR_NAMES.put(0, "KDC_ERR_NONE");
    ERROR_NAMES.put(1, "KDC_ERR_NAME_EXP");
    ERROR_NAMES.put(2, "KDC_ERR_SERVICE_EXP");
    ERROR_NAMES.put(3, "KDC_ERR_BAD_PVNO");
    ERROR_NAMES.put(6, "KDC_ERR_C_PRINCIPAL_UNKNOWN");
    ERROR_NAMES.put(7, "KDC_ERR_S_PRINCIPAL_UNKNOWN");
    ERROR_NAMES.put(8, "KDC_ERR_PRINCIPAL_NOT_UNIQUE");
    ERROR_NAMES.put(9, "KDC_ERR_NULL_KEY");
    ERROR_NAMES.put(10, "KDC_ERR_CANNOT_POSTDATE");
    ERROR_NAMES.put(11, "KDC_ERR_NEVER_VALID");
    ERROR_NAMES.put(12, "KDC_ERR_POLICY");
    ERROR_NAMES.put(13, "KDC_ERR_BADOPTION");
    ERROR_NAMES.put(14, "KDC_ERR_ETYPE_NOSUPP");
    ERROR_NAMES.put(15, "KDC_ERR_SUMTYPE_NOSUPP");
    ERROR_NAMES.put(16, "KDC_ERR_PADATA_TYPE_NOSUPP");
    ERROR_NAMES.put(17, "KDC_ERR_TRTYPE_NOSUPP");
    ERROR_NAMES.put(18, "KDC_ERR_CLIENT_REVOKED");
    ERROR_NAMES.put(19, "KDC_ERR_SERVICE_REVOKED");
    ERROR_NAMES.put(20, "KDC_ERR_TGT_REVOKED");
    ERROR_NAMES.put(21, "KDC_ERR_CLIENT_NOTYET");
    ERROR_NAMES.put(22, "KDC_ERR_SERVICE_NOTYET");
    ERROR_NAMES.put(23, "KDC_ERR_KEY_EXPIRED");
    ERROR_NAMES.put(24, "KDC_ERR_PREAUTH_FAILED");
    ERROR_NAMES.put(25, "KDC_ERR_PREAUTH_REQUIRED");
    ERROR_NAMES.put(26, "KDC_ERR_SERVER_NOMATCH");
    ERROR_NAMES.put(31, "KRB_AP_ERR_BAD_INTEGRITY");
    ERROR_NAMES.put(32, "KRB_AP_ERR_TKT_EXPIRED");
    ERROR_NAMES.put(33, "KRB_AP_ERR_TKT_NYV");
    ERROR_NAMES.put(34, "KRB_AP_ERR_REPEAT");
    ERROR_NAMES.put(35, "KRB_AP_ERR_NOT_US");
    ERROR_NAMES.put(36, "KRB_AP_ERR_BADMATCH");
    ERROR_NAMES.put(37, "KRB_AP_ERR_SKEW");
    ERROR_NAMES.put(38, "KRB_AP_ERR_BADADDR");
    ERROR_NAMES.put(39, "KRB_AP_ERR_BADVERSION");
    ERROR_NAMES.put(40, "KRB_AP_ERR_MSG_TYPE");
    ERROR_NAMES.put(41, "KRB_AP_ERR_MODIFIED");
    ERROR_NAMES.put(52, "KRB_ERR_RESPONSE_TOO_BIG");
    ERROR_NAMES.put(60, "KRB_ERR_GENERIC");
    ERROR_NAMES.put(61, "KRB_ERR_FIELD_TOOLONG");
  }

  private final DecoderContext context;

  private KerberosParser(DecoderContext context) {
    this.context = context;
  }

  /**
   * @return the parsed message, or null if the data is not a Kerberos message we recognise
   * @throws IllegalArgumentException if the data is Kerberos but malformed or truncated
   */
  public static KerberosMessage parse(byte[] b, DecoderContext context) {
    if (b == null || b.length < 2) {
      return null;
    }
    int offset = 0;
    if (!isMessageTag(b[0] & 0xFF)) {
      // TCP carries a 4-byte length prefix. Handle the single-message case; defer anything else.
      if (b.length < 5) {
        return null;
      }
      long prefix = ((b[0] & 0xFFL) << 24) | ((b[1] & 0xFFL) << 16) | ((b[2] & 0xFFL) << 8) | (b[3] & 0xFFL);
      if (prefix != b.length - 4L || !isMessageTag(b[4] & 0xFF)) {
        return null;
      }
      offset = 4;
    }
    int appTag = b[offset] & 0xFF;
    // From here the framing is confidently Kerberos (a known APPLICATION tag, and for TCP a length prefix
    // that matches the captured bytes), so a header that overruns the data is a truncation error, not a
    // "not mine". A multi-segment TCP message was already rejected above when its prefix did not match.
    Element outer = Asn1Reader.header(b, offset, b.length);
    Asn1Reader outerReader = new Asn1Reader(b, offset, b.length, 1);
    Asn1Reader seq = outerReader.enter(outer);
    if (!seq.hasMore()) {
      return null;
    }
    Element body = seq.next();
    if (body.tag != Asn1Reader.SEQUENCE) {
      return null;
    }
    try {
      return new KerberosParser(context).message(appTag, seq.enter(body));
    } catch (NotKerberos e) {
      // Recognised framing but not a valid Kerberos message (for example pvno is not 5).
      return null;
    }
  }

  private static boolean isMessageTag(int tag) {
    return tag == AS_REQ || tag == AS_REP || tag == TGS_REQ || tag == TGS_REP || tag == KRB_ERROR;
  }

  private KerberosMessage message(int appTag, Asn1Reader r) {
    KerberosMessage m = new KerberosMessage();
    switch (appTag) {
      case AS_REQ:
        m.messageType = "AS-REQ";
        kdcReq(r, m);
        break;
      case TGS_REQ:
        m.messageType = "TGS-REQ";
        kdcReq(r, m);
        break;
      case AS_REP:
        m.messageType = "AS-REP";
        kdcRep(r, m);
        break;
      case TGS_REP:
        m.messageType = "TGS-REP";
        kdcRep(r, m);
        break;
      default:
        m.messageType = "KRB-ERROR";
        krbError(r, m);
        break;
    }
    return m;
  }

  /** KDC-REQ: pvno [1], msg-type [2], padata [3] OPTIONAL, req-body [4]. */
  private void kdcReq(Asn1Reader r, KerberosMessage m) {
    boolean pvnoSeen = false;
    m.preAuthPresent = false;
    while (r.hasMore()) {
      Element f = r.next();
      switch (f.number()) {
        case 1:
          requirePvno(r, f);
          pvnoSeen = true;
          break;
        case 3:
          m.preAuthPresent = true;
          break;
        case 4:
          Asn1Reader bodyWrapper = r.enter(f);
          reqBody(bodyWrapper.enter(bodyWrapper.expect(Asn1Reader.SEQUENCE, "req-body")), m);
          break;
        default:
          break;
      }
    }
    if (!pvnoSeen) {
      // Without a confirmed pvno 5 this is not a Kerberos message we trust.
      throw new NotKerberos();
    }
  }

  /** KDC-REQ-BODY: kdc-options [0], cname [1], realm [2], sname [3], ... till [5], ... etype [8]. */
  private void reqBody(Asn1Reader r, KerberosMessage m) {
    while (r.hasMore()) {
      Element f = r.next();
      switch (f.number()) {
        case 1:
          m.clientName = principalName(r.enter(f));
          break;
        case 2:
          m.realm = realm(r.enter(f));
          break;
        case 3:
          m.serverName = principalName(r.enter(f));
          break;
        case 5:
          Asn1Reader tillWrapper = r.enter(f);
          m.till = tillWrapper.generalizedTime(innerValue(tillWrapper));
          break;
        case 8:
          etypes(r.enter(f), m);
          break;
        default:
          break;
      }
    }
  }

  /** KDC-REP: pvno [0], msg-type [1], padata [2] OPTIONAL, crealm [3], cname [4], ticket [5], enc-part [6]. */
  private void kdcRep(Asn1Reader r, KerberosMessage m) {
    boolean pvnoSeen = false;
    m.preAuthPresent = false;
    while (r.hasMore()) {
      Element f = r.next();
      switch (f.number()) {
        case 0:
          requirePvno(r, f);
          pvnoSeen = true;
          break;
        case 2:
          m.preAuthPresent = true;
          break;
        case 3:
          m.realm = realm(r.enter(f));
          break;
        case 4:
          m.clientName = principalName(r.enter(f));
          break;
        case 5:
          ticket(r.enter(f), m);
          break;
        default:
          break;
      }
    }
    if (!pvnoSeen) {
      throw new NotKerberos();
    }
  }

  /** Ticket ::= [APPLICATION 1] SEQUENCE { tkt-vno [0], realm [1], sname [2], enc-part [3] }. */
  private void ticket(Asn1Reader wrapper, KerberosMessage m) {
    Element app = wrapper.expect(TICKET, "Ticket");
    Asn1Reader seq = wrapper.enter(app);
    Element body = seq.expect(Asn1Reader.SEQUENCE, "Ticket body");
    Asn1Reader r = seq.enter(body);
    while (r.hasMore()) {
      Element f = r.next();
      switch (f.number()) {
        case 2:
          m.serverName = principalName(r.enter(f));
          break;
        case 3:
          m.ticketEncryptionType = encryptedDataEtype(r.enter(f));
          break;
        default:
          break;
      }
    }
  }

  /** EncryptedData ::= SEQUENCE { etype [0] Int32, kvno [1] OPTIONAL, cipher [2] OCTET STRING }. */
  private int encryptedDataEtype(Asn1Reader r) {
    Element seq = r.expect(Asn1Reader.SEQUENCE, "EncryptedData");
    Asn1Reader enc = r.enter(seq);
    Element etype = enc.expect(0xA0, "etype");
    Asn1Reader inner = enc.enter(etype);
    return inner.intValue(inner.expect(Asn1Reader.INTEGER, "etype value"), "etype");
  }

  /** KRB-ERROR: ... error-code [6], crealm [7] OPT, cname [8] OPT, realm [9], sname [10], e-text [11] OPT. */
  private void krbError(Asn1Reader r, KerberosMessage m) {
    boolean pvnoSeen = false;
    String serviceRealm = null;
    String clientRealm = null;
    while (r.hasMore()) {
      Element f = r.next();
      switch (f.number()) {
        case 0:
          requirePvno(r, f);
          pvnoSeen = true;
          break;
        case 6:
          Asn1Reader codeWrapper = r.enter(f);
          m.errorCode = codeWrapper.intValue(innerValue(codeWrapper), "error-code");
          m.errorText = ERROR_NAMES.get(m.errorCode);
          break;
        case 7:
          clientRealm = realm(r.enter(f));
          break;
        case 8:
          m.clientName = principalName(r.enter(f));
          break;
        case 9:
          serviceRealm = realm(r.enter(f));
          break;
        case 10:
          m.serverName = principalName(r.enter(f));
          break;
        default:
          break;
      }
    }
    if (!pvnoSeen) {
      throw new NotKerberos();
    }
    m.realm = serviceRealm != null ? serviceRealm : clientRealm;
  }

  private void requirePvno(Asn1Reader parent, Element field) {
    Asn1Reader inner = parent.enter(field);
    int pvno = inner.intValue(inner.expect(Asn1Reader.INTEGER, "pvno"), "pvno");
    if (pvno != 5) {
      throw new NotKerberos();
    }
  }

  /** PrincipalName ::= SEQUENCE { name-type [0] Int32, name-string [1] SEQUENCE OF KerberosString }. */
  private String principalName(Asn1Reader wrapper) {
    Element seq = wrapper.expect(Asn1Reader.SEQUENCE, "PrincipalName");
    Asn1Reader pn = wrapper.enter(seq);
    StringBuilder out = new StringBuilder();
    while (pn.hasMore()) {
      Element f = pn.next();
      if (f.number() != 1) {
        continue;
      }
      Asn1Reader list = pn.enter(f);
      Element names = list.expect(Asn1Reader.SEQUENCE, "name-string");
      Asn1Reader strings = list.enter(names);
      int count = 0;
      while (strings.hasMore()) {
        Element s = strings.next();
        if (++count > MAX_ITEMS) {
          context.warn("principal name truncated to " + MAX_ITEMS + " components");
          break;
        }
        if (out.length() > 0) {
          out.append('/');
        }
        out.append(strings.string(s));
      }
    }
    return out.length() == 0 ? null : cap(out.toString());
  }

  private String realm(Asn1Reader wrapper) {
    return wrapper.string(innerValue(wrapper));
  }

  private void etypes(Asn1Reader wrapper, KerberosMessage m) {
    Element seq = wrapper.expect(Asn1Reader.SEQUENCE, "etype list");
    Asn1Reader list = wrapper.enter(seq);
    int count = 0;
    while (list.hasMore()) {
      Element e = list.next();
      if (++count > MAX_ITEMS) {
        context.warn("encryption_types truncated to " + MAX_ITEMS);
        break;
      }
      m.encryptionTypes.add(list.intValue(e, "etype"));
    }
  }

  /** The single value inside an explicitly tagged context wrapper. */
  private Element innerValue(Asn1Reader wrapper) {
    if (!wrapper.hasMore()) {
      throw new IllegalArgumentException("empty tagged field");
    }
    return wrapper.next();
  }

  private static String cap(String s) {
    return s.length() > Asn1Reader.MAX_STRING ? s.substring(0, Asn1Reader.MAX_STRING) : s;
  }

  /** Signals that recognised framing does not actually hold a valid Kerberos message. */
  static final class NotKerberos extends RuntimeException {
    NotKerberos() {
      super("not kerberos");
    }
  }
}
