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
package org.apache.drill.exec.store.pcap.protocol.snmp;

/**
 * A bounded reader of BER elements: single-byte tags, definite lengths only, every length checked against
 * the enclosing element, and at most 16 levels of nesting.
 */
final class BerReader {
  static final int MAX_DEPTH = 16;

  /** One element: tag and content range. */
  static final class Element {
    final int tag;
    final int start;
    final int length;

    Element(int tag, int start, int length) {
      this.tag = tag;
      this.start = start;
      this.length = length;
    }

    int end() {
      return start + length;
    }
  }

  private final byte[] b;
  private final int limit;
  private final int depth;
  private int pos;

  BerReader(byte[] b, int start, int limit, int depth) {
    if (depth > MAX_DEPTH) {
      throw new IllegalArgumentException("nesting deeper than " + MAX_DEPTH);
    }
    this.b = b;
    this.pos = start;
    this.limit = limit;
    this.depth = depth;
  }

  byte[] bytes() {
    return b;
  }

  boolean hasMore() {
    return pos < limit;
  }

  /** Reads the next element and moves past it. */
  Element next() {
    Element e = header(b, pos, limit);
    pos = e.end();
    return e;
  }

  /** Reads the next element, which must have the given tag. */
  Element expect(int tag, String what) {
    if (!hasMore()) {
      throw new IllegalArgumentException("missing " + what);
    }
    Element e = next();
    if (e.tag != tag) {
      throw new IllegalArgumentException(String.format("expected %s (tag 0x%02x) but found tag 0x%02x", what, tag, e.tag));
    }
    return e;
  }

  /** A reader over the content of a constructed element. */
  BerReader enter(Element e) {
    return new BerReader(b, e.start, e.end(), depth + 1);
  }

  /**
   * Reads a tag and length at at, checking that the content fits before limit.
   *
   * @throws IllegalArgumentException if the header is invalid or the content overruns limit
   */
  static Element header(byte[] b, int at, int limit) {
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
}
