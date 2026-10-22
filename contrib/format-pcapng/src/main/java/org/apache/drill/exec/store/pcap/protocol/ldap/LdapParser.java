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

import java.util.HashMap;
import java.util.Map;

import org.apache.drill.exec.store.pcap.protocol.DecoderContext;
import org.apache.drill.exec.store.pcap.protocol.asn1.Asn1Reader;
import org.apache.drill.exec.store.pcap.protocol.asn1.Asn1Reader.Element;

/**
 * Parses the reassembled byte streams of an LDAP session (RFC 4511). Reads bind credentials, search bases
 * and RFC 4515 filters, and returned entry DNs. Problems after the session is recognised are reported as
 * warnings so that whatever parsed before them is kept.
 */
public final class LdapParser {
  static final int MAX_ITEMS = 64;
  static final int MAX_MESSAGES = 1000;

  private static final int APPLICATION = 1;
  // protocolOp application numbers.
  private static final int BIND_REQUEST = 0;
  private static final int BIND_RESPONSE = 1;
  private static final int SEARCH_REQUEST = 3;
  private static final int SEARCH_RESULT_ENTRY = 4;

  private static final String[] SCOPES = {"base", "one", "sub"};
  private static final Map<Integer, String> RESULT_NAMES = new HashMap<>();

  static {
    RESULT_NAMES.put(0, "success");
    RESULT_NAMES.put(1, "operationsError");
    RESULT_NAMES.put(2, "protocolError");
    RESULT_NAMES.put(3, "timeLimitExceeded");
    RESULT_NAMES.put(4, "sizeLimitExceeded");
    RESULT_NAMES.put(5, "compareFalse");
    RESULT_NAMES.put(6, "compareTrue");
    RESULT_NAMES.put(7, "authMethodNotSupported");
    RESULT_NAMES.put(8, "strongerAuthRequired");
    RESULT_NAMES.put(10, "referral");
    RESULT_NAMES.put(11, "adminLimitExceeded");
    RESULT_NAMES.put(12, "unavailableCriticalExtension");
    RESULT_NAMES.put(13, "confidentialityRequired");
    RESULT_NAMES.put(14, "saslBindInProgress");
    RESULT_NAMES.put(16, "noSuchAttribute");
    RESULT_NAMES.put(17, "undefinedAttributeType");
    RESULT_NAMES.put(18, "inappropriateMatching");
    RESULT_NAMES.put(19, "constraintViolation");
    RESULT_NAMES.put(20, "attributeOrValueExists");
    RESULT_NAMES.put(21, "invalidAttributeSyntax");
    RESULT_NAMES.put(32, "noSuchObject");
    RESULT_NAMES.put(33, "aliasProblem");
    RESULT_NAMES.put(34, "invalidDNSyntax");
    RESULT_NAMES.put(48, "inappropriateAuthentication");
    RESULT_NAMES.put(49, "invalidCredentials");
    RESULT_NAMES.put(50, "insufficientAccessRights");
    RESULT_NAMES.put(51, "busy");
    RESULT_NAMES.put(52, "unavailable");
    RESULT_NAMES.put(53, "unwillingToPerform");
    RESULT_NAMES.put(54, "loopDetect");
    RESULT_NAMES.put(64, "namingViolation");
    RESULT_NAMES.put(65, "objectClassViolation");
    RESULT_NAMES.put(66, "notAllowedOnNonLeaf");
    RESULT_NAMES.put(67, "notAllowedOnRDN");
    RESULT_NAMES.put(68, "entryAlreadyExists");
    RESULT_NAMES.put(69, "objectClassModsProhibited");
    RESULT_NAMES.put(71, "affectsMultipleDSAs");
    RESULT_NAMES.put(80, "other");
  }

  private final DecoderContext context;
  private final LdapSession session;
  private boolean bindSeen;
  private boolean bindResultSeen;
  private boolean searchesCapped;
  private boolean entriesCapped;
  private int messageCount;

  private LdapParser(DecoderContext context, LdapSession session) {
    this.context = context;
    this.session = session;
  }

  /**
   * @return the parsed session, or null if neither stream begins with a valid LDAPMessage
   */
  public static LdapSession parse(byte[] client, long clientGap, byte[] server, long serverGap,
                                  DecoderContext context) {
    if (!looksLikeLdap(client) && !looksLikeLdap(server)) {
      return null;
    }
    LdapSession session = new LdapSession();
    LdapParser parser = new LdapParser(context, session);
    parser.parseStream(client, clientGap, "client");
    parser.parseStream(server, serverGap, "server");
    return session;
  }

  /** The match rule: a SEQUENCE whose first element is an INTEGER and whose second is an application tag. */
  private static boolean looksLikeLdap(byte[] data) {
    if (data == null || data.length < 2 || (data[0] & 0xFF) != Asn1Reader.SEQUENCE) {
      return false;
    }
    try {
      Element msg = Asn1Reader.header(data, 0, data.length);
      Asn1Reader r = new Asn1Reader(data, msg.start, msg.end(), 1);
      Element id = r.next();
      if (id.tag != Asn1Reader.INTEGER || id.length < 1 || id.length > 8) {
        return false;
      }
      if (!r.hasMore()) {
        return false;
      }
      Element op = r.next();
      return ((op.tag >> 6) & 3) == APPLICATION;
    } catch (RuntimeException e) {
      return false;
    }
  }

  private void parseStream(byte[] data, long gap, String direction) {
    int pos = 0;
    while (pos < data.length) {
      if (messageCount >= MAX_MESSAGES) {
        context.warn("session truncated to " + MAX_MESSAGES + " messages");
        return;
      }
      Element msg;
      try {
        msg = Asn1Reader.header(data, pos, data.length);
      } catch (IllegalArgumentException e) {
        if (gap >= 0) {
          context.warn("stopped at missing data in " + direction + " stream at byte " + gap);
        } else {
          context.warn("truncated message in " + direction + " stream at byte " + pos);
        }
        return;
      }
      if (msg.tag != Asn1Reader.SEQUENCE) {
        context.warn("unexpected data in " + direction + " stream at byte " + pos);
        return;
      }
      try {
        message(data, msg);
      } catch (IllegalArgumentException e) {
        context.warn("malformed message in " + direction + " stream at byte " + pos + ": " + e.getMessage());
        return;
      }
      messageCount++;
      pos = msg.end();
    }
  }

  private void message(byte[] data, Element msg) {
    Asn1Reader r = new Asn1Reader(data, msg.start, msg.end(), 1);
    Element id = r.expect(Asn1Reader.INTEGER, "messageID");
    r.integer(id, "messageID");
    if (!r.hasMore()) {
      throw new IllegalArgumentException("missing protocolOp");
    }
    Element op = r.next();
    if (((op.tag >> 6) & 3) != APPLICATION) {
      throw new IllegalArgumentException(String.format("protocolOp has non-application tag 0x%02x", op.tag));
    }
    switch (op.number()) {
      case BIND_REQUEST:
        session.operationCount++;
        bindRequest(r.enter(op));
        break;
      case BIND_RESPONSE:
        bindResponse(r.enter(op));
        break;
      case SEARCH_REQUEST:
        session.operationCount++;
        searchRequest(r.enter(op));
        break;
      case SEARCH_RESULT_ENTRY:
        searchResultEntry(r.enter(op));
        break;
      default:
        // Other requests still count as operations; responses (odd-numbered results) do not.
        if (isRequest(op.number())) {
          session.operationCount++;
        }
        break;
    }
  }

  private static boolean isRequest(int opNumber) {
    switch (opNumber) {
      case 2:  // unbindRequest
      case 6:  // modifyRequest
      case 8:  // addRequest
      case 10: // delRequest
      case 12: // modifyDNRequest
      case 14: // compareRequest
      case 16: // abandonRequest
      case 23: // extendedRequest
        return true;
      default:
        return false;
    }
  }

  /** BindRequest ::= SEQUENCE { version INTEGER, name LDAPDN, authentication AuthenticationChoice }. */
  private void bindRequest(Asn1Reader r) {
    int version = r.intValue(r.expect(Asn1Reader.INTEGER, "version"), "version");
    String name = r.string(r.expect(Asn1Reader.OCTET_STRING, "bind name"));
    if (!r.hasMore()) {
      throw new IllegalArgumentException("missing authentication");
    }
    Element auth = r.next();
    if (((auth.tag >> 6) & 3) != 2) { // context class
      throw new IllegalArgumentException(String.format("authentication has tag 0x%02x", auth.tag));
    }
    String authType;
    boolean present;
    String password = null;
    if (auth.number() == 0) {
      // simple [0] OCTET STRING: a cleartext password
      authType = "simple";
      present = auth.length > 0;
      if (present && context.exposeCredentials()) {
        password = r.string(auth);
      }
    } else if (auth.number() == 3) {
      // sasl [3] SaslCredentials ::= SEQUENCE { mechanism LDAPString, credentials OCTET STRING OPTIONAL }
      Asn1Reader sasl = r.enter(auth);
      authType = r.string(sasl.expect(Asn1Reader.OCTET_STRING, "SASL mechanism"));
      present = sasl.hasMore() && sasl.next().length > 0;
    } else {
      authType = "auth-" + auth.number();
      present = auth.length > 0;
    }
    if (bindSeen) {
      return; // keep the first bind of the session
    }
    bindSeen = true;
    session.version = version;
    session.bindDn = name.isEmpty() ? null : name;
    session.authType = authType;
    session.passwordPresent = present;
    session.password = password;
  }

  /** BindResponse ::= SEQUENCE { resultCode ENUMERATED, matchedDN, diagnosticMessage, ... }. */
  private void bindResponse(Asn1Reader r) {
    int code = r.intValue(r.expect(Asn1Reader.ENUMERATED, "resultCode"), "resultCode");
    if (bindResultSeen) {
      return;
    }
    bindResultSeen = true;
    session.bindResultCode = code;
    session.bindResult = RESULT_NAMES.get(code);
  }

  /** SearchRequest ::= SEQUENCE { baseObject, scope, derefAliases, sizeLimit, timeLimit, typesOnly, filter, ... }. */
  private void searchRequest(Asn1Reader r) {
    String baseDn = r.string(r.expect(Asn1Reader.OCTET_STRING, "baseObject"));
    int scope = r.intValue(r.expect(Asn1Reader.ENUMERATED, "scope"), "scope");
    r.expect(Asn1Reader.ENUMERATED, "derefAliases");
    r.expect(Asn1Reader.INTEGER, "sizeLimit");
    r.expect(Asn1Reader.INTEGER, "timeLimit");
    r.expect(Asn1Reader.BOOLEAN, "typesOnly");
    if (!r.hasMore()) {
      throw new IllegalArgumentException("missing filter");
    }
    String filter = renderFilter(r, r.next());
    if (session.searches.size() >= MAX_ITEMS) {
      if (!searchesCapped) {
        searchesCapped = true;
        context.warn("searches truncated to " + MAX_ITEMS);
      }
      return;
    }
    LdapSession.Search search = new LdapSession.Search();
    search.baseDn = baseDn.isEmpty() ? null : baseDn;
    search.scope = scope >= 0 && scope < SCOPES.length ? SCOPES[scope] : String.valueOf(scope);
    search.filter = filter;
    session.searches.add(search);
  }

  /** SearchResultEntry ::= SEQUENCE { objectName LDAPDN, attributes PartialAttributeList }. */
  private void searchResultEntry(Asn1Reader r) {
    String dn = r.string(r.expect(Asn1Reader.OCTET_STRING, "objectName"));
    if (session.entriesReturned.size() >= MAX_ITEMS) {
      if (!entriesCapped) {
        entriesCapped = true;
        context.warn("entries_returned truncated to " + MAX_ITEMS);
      }
      return;
    }
    session.entriesReturned.add(dn);
  }

  // ---- RFC 4515 filter rendering ----

  private String renderFilter(Asn1Reader parent, Element f) {
    switch (f.number()) {
      case 0: // and
        return "(&" + renderFilterSet(parent.enter(f)) + ")";
      case 1: // or
        return "(|" + renderFilterSet(parent.enter(f)) + ")";
      case 2: { // not
        Asn1Reader inner = parent.enter(f);
        if (!inner.hasMore()) {
          return "(!)";
        }
        return "(!" + renderFilter(inner, inner.next()) + ")";
      }
      case 3: // equalityMatch
        return "(" + assertion(parent.enter(f), "=") + ")";
      case 4: // substrings
        return substrings(parent.enter(f));
      case 5: // greaterOrEqual
        return "(" + assertion(parent.enter(f), ">=") + ")";
      case 6: // lessOrEqual
        return "(" + assertion(parent.enter(f), "<=") + ")";
      case 7: // present (primitive): content is the attribute description
        return "(" + parent.string(f) + "=*)";
      case 8: // approxMatch
        return "(" + assertion(parent.enter(f), "~=") + ")";
      case 9: // extensibleMatch
        return extensible(parent.enter(f));
      default:
        return "(?)";
    }
  }

  private String renderFilterSet(Asn1Reader r) {
    StringBuilder out = new StringBuilder();
    int count = 0;
    while (r.hasMore()) {
      if (++count > MAX_ITEMS) {
        context.warn("filter truncated to " + MAX_ITEMS + " terms");
        break;
      }
      out.append(renderFilter(r, r.next()));
    }
    return out.toString();
  }

  private String assertion(Asn1Reader r, String op) {
    String attr = r.string(r.expect(Asn1Reader.OCTET_STRING, "attributeDesc"));
    String value = escape(r.string(r.expect(Asn1Reader.OCTET_STRING, "assertionValue")));
    return attr + op + value;
  }

  private String substrings(Asn1Reader r) {
    String attr = r.string(r.expect(Asn1Reader.OCTET_STRING, "substring type"));
    Asn1Reader list = r.enter(r.expect(Asn1Reader.SEQUENCE, "substrings"));
    StringBuilder initial = new StringBuilder();
    StringBuilder middle = new StringBuilder();
    StringBuilder last = new StringBuilder();
    int count = 0;
    while (list.hasMore()) {
      Element e = list.next();
      if (++count > MAX_ITEMS) {
        context.warn("substring filter truncated to " + MAX_ITEMS + " parts");
        break;
      }
      String part = escape(list.string(e));
      switch (e.number()) {
        case 0:
          initial.append(part);
          break;
        case 1:
          middle.append(part).append('*');
          break;
        default:
          last.append(part);
          break;
      }
    }
    return "(" + attr + "=" + initial + "*" + middle + last + ")";
  }

  private String extensible(Asn1Reader r) {
    String rule = null;
    String type = null;
    String value = "";
    boolean dnAttributes = false;
    while (r.hasMore()) {
      Element e = r.next();
      switch (e.number()) {
        case 1:
          rule = r.string(e);
          break;
        case 2:
          type = r.string(e);
          break;
        case 3:
          value = escape(r.string(e));
          break;
        case 4:
          dnAttributes = r.booleanValue(e);
          break;
        default:
          break;
      }
    }
    StringBuilder out = new StringBuilder("(");
    if (type != null) {
      out.append(type);
    }
    if (dnAttributes) {
      out.append(":dn");
    }
    if (rule != null) {
      out.append(':').append(rule);
    }
    out.append(":=").append(value).append(')');
    return out.toString();
  }

  /** Escapes the RFC 4515 special characters in an assertion value. */
  private static String escape(String s) {
    StringBuilder out = new StringBuilder(s.length());
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      switch (c) {
        case '*':
          out.append("\\2a");
          break;
        case '(':
          out.append("\\28");
          break;
        case ')':
          out.append("\\29");
          break;
        case '\\':
          out.append("\\5c");
          break;
        case '\0':
          out.append("\\00");
          break;
        default:
          out.append(c);
          break;
      }
    }
    return out.toString();
  }
}
