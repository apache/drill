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
package org.apache.drill.exec.store.pcap.protocol.asn1;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;

/**
 * A bounded reader of ASN.1 BER/DER elements shared by the Kerberos and LDAP decoders. It reads
 * single-byte tags (universal, application and context classes) and definite lengths only, checks every
 * length against the enclosing element, and limits nesting. BER lengths come from untrusted capture data,
 * so every read is bounds checked.
 *
 * <p>Modelled on the SNMP decoder's package-private {@code BerReader}, promoted to a shared, public helper
 * with value accessors (integers, strings, OIDs, GeneralizedTime) that both ASN.1 decoders need.
 */
public final class Asn1Reader {
  public static final int MAX_DEPTH = 32;
  public static final int MAX_STRING = 4096;

  // Universal tags.
  public static final int BOOLEAN = 0x01;
  public static final int INTEGER = 0x02;
  public static final int BIT_STRING = 0x03;
  public static final int OCTET_STRING = 0x04;
  public static final int NULL = 0x05;
  public static final int OID = 0x06;
  public static final int ENUMERATED = 0x0A;
  public static final int UTF8_STRING = 0x0C;
  public static final int SEQUENCE = 0x30;
  public static final int SET = 0x31;
  public static final int GENERAL_STRING = 0x1B;
  public static final int GENERALIZED_TIME = 0x18;

  /** One element: its tag, and the range of its content. */
  public static final class Element {
    public final int tag;
    public final int start;
    public final int length;

    Element(int tag, int start, int length) {
      this.tag = tag;
      this.start = start;
      this.length = length;
    }

    public int end() {
      return start + length;
    }

    /** The low five bits of the tag: the universal type, or the application/context number. */
    public int number() {
      return tag & 0x1F;
    }

    public boolean isConstructed() {
      return (tag & 0x20) != 0;
    }
  }

  private final byte[] b;
  private final int limit;
  private final int depth;
  private int pos;

  public Asn1Reader(byte[] b, int start, int limit, int depth) {
    if (depth > MAX_DEPTH) {
      throw new IllegalArgumentException("nesting deeper than " + MAX_DEPTH);
    }
    this.b = b;
    this.pos = start;
    this.limit = limit;
    this.depth = depth;
  }

  public byte[] bytes() {
    return b;
  }

  public boolean hasMore() {
    return pos < limit;
  }

  /** Reads the next element and moves past it. */
  public Element next() {
    Element e = header(b, pos, limit);
    pos = e.end();
    return e;
  }

  /** Reads the next element, which must have the given tag. */
  public Element expect(int tag, String what) {
    if (!hasMore()) {
      throw new IllegalArgumentException("missing " + what);
    }
    Element e = next();
    if (e.tag != tag) {
      throw new IllegalArgumentException(
          String.format("expected %s (tag 0x%02x) but found tag 0x%02x", what, tag, e.tag));
    }
    return e;
  }

  /** A reader over the content of a constructed element. */
  public Asn1Reader enter(Element e) {
    return new Asn1Reader(b, e.start, e.end(), depth + 1);
  }

  /**
   * Reads a tag and length at {@code at}, checking that the content fits before {@code limit}.
   *
   * @throws IllegalArgumentException if the header is invalid or the content overruns the limit
   */
  public static Element header(byte[] b, int at, int limit) {
    if (at + 2 > limit) {
      throw new IllegalArgumentException("truncated element at offset " + at);
    }
    int tag = b[at] & 0xFF;
    if ((tag & 0x1F) == 0x1F) {
      throw new IllegalArgumentException("unsupported multi-byte tag at offset " + at);
    }
    int first = b[at + 1] & 0xFF;
    int p = at + 2;
    long length;
    if (first < 0x80) {
      length = first;
    } else {
      int count = first & 0x7F;
      if (count == 0) {
        throw new IllegalArgumentException("indefinite length at offset " + at);
      }
      if (count > 4 || p + count > limit) {
        throw new IllegalArgumentException("bad length encoding at offset " + at);
      }
      length = 0;
      for (int i = 0; i < count; i++) {
        length = (length << 8) | (b[p + i] & 0xFF);
      }
      p += count;
    }
    if (p + length > limit) {
      throw new IllegalArgumentException("length " + length + " at offset " + at + " overruns its container");
    }
    return new Element(tag, p, (int) length);
  }

  /** Reads a signed integer, which must be no longer than eight bytes. */
  public long integer(Element e, String what) {
    if (e.length < 1 || e.length > 8) {
      throw new IllegalArgumentException(what + " has bad integer length " + e.length);
    }
    return new BigInteger(Arrays.copyOfRange(b, e.start, e.end())).longValue();
  }

  /** Reads an integer that must fit in an int. */
  public int intValue(Element e, String what) {
    long v = integer(e, what);
    if (v < Integer.MIN_VALUE || v > Integer.MAX_VALUE) {
      throw new IllegalArgumentException(what + " out of range: " + v);
    }
    return (int) v;
  }

  public boolean booleanValue(Element e) {
    for (int i = e.start; i < e.end(); i++) {
      if (b[i] != 0) {
        return true;
      }
    }
    return false;
  }

  /** The element's content as a UTF-8 string, capped at {@link #MAX_STRING} characters. */
  public String string(Element e) {
    return cap(new String(b, e.start, Math.min(e.length, MAX_STRING), StandardCharsets.UTF_8));
  }

  /** The element's content as lower-case hex, capped. */
  public String hex(Element e) {
    StringBuilder out = new StringBuilder();
    for (int i = e.start; i < e.end() && out.length() < MAX_STRING; i++) {
      out.append(String.format("%02x", b[i]));
    }
    return out.toString();
  }

  /**
   * Parses a GeneralizedTime (such as a Kerberos {@code YYYYMMDDHHMMSSZ}) into an Instant, or null if it
   * is not in the expected form. Never throws: a malformed time is simply absent.
   */
  public Instant generalizedTime(Element e) {
    String s = new String(b, e.start, Math.min(e.length, 32), StandardCharsets.US_ASCII);
    if (s.length() < 14) {
      return null;
    }
    try {
      int year = Integer.parseInt(s.substring(0, 4));
      int month = Integer.parseInt(s.substring(4, 6));
      int day = Integer.parseInt(s.substring(6, 8));
      int hour = Integer.parseInt(s.substring(8, 10));
      int minute = Integer.parseInt(s.substring(10, 12));
      int second = Integer.parseInt(s.substring(12, 14));
      return LocalDateTime.of(year, month, day, hour, minute, second).toInstant(ZoneOffset.UTC);
    } catch (RuntimeException ex) {
      return null;
    }
  }

  private static String cap(String s) {
    return s.length() > MAX_STRING ? s.substring(0, MAX_STRING) : s;
  }
}
