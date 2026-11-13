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

import org.apache.drill.exec.store.pcap.protocol.DecoderContext;

/**
 * Reads the cleartext handshake messages of one direction of a TLS connection. Handshake records are
 * reassembled, so a message may span records and a record may hold several messages. Reading stops at
 * the first ChangeCipherSpec (unless told to continue past it), at the first application data record,
 * at anything that is not a TLS record, or after {@link #MAX_HANDSHAKE_BYTES} of handshake data.
 */
final class TlsRecordReader {
  static final int MAX_HANDSHAKE_BYTES = 65536;
  // 2^14 plus the allowance for compression and encryption overhead
  private static final int MAX_RECORD = 16384 + 2048;
  private static final int CHANGE_CIPHER_SPEC = 20;
  private static final int ALERT = 21;
  private static final int HANDSHAKE = 22;
  private static final int HEARTBEAT = 24;

  /** A complete handshake message. */
  static final class Message {
    final int type;
    final byte[] body;

    Message(int type, byte[] body) {
      this.type = type;
      this.body = body;
    }
  }

  private final byte[] data;
  private final String direction;
  private final DecoderContext context;
  private int offset;
  private byte[] buffer = new byte[0];
  private int bufferLength;
  private int position;
  private boolean done;
  private boolean continueAfterChangeCipherSpec;
  private boolean reachedEnd;
  private String endedInside;

  TlsRecordReader(byte[] data, String direction, DecoderContext context) {
    this.data = data;
    this.direction = direction;
    this.context = context;
  }

  /** True if the data starts with a handshake record whose first message has the given type. */
  static boolean startsWith(byte[] data, int handshakeType) {
    if (data.length < 6) {
      return false;
    }
    int length = ((data[3] & 0xFF) << 8) | (data[4] & 0xFF);
    return data[0] == HANDSHAKE && data[1] == 3 && (data[2] & 0xFF) <= 4 && length >= 4 && length <= MAX_RECORD
        && (data[5] & 0xFF) == handshakeType;
  }

  /** In TLS 1.3 a ChangeCipherSpec is sent only for compatibility; cleartext handshake may follow it. */
  void continueAfterChangeCipherSpec(boolean value) {
    continueAfterChangeCipherSpec = value;
  }

  /** True if reading consumed all the data, as opposed to stopping at the end of the cleartext handshake. */
  boolean reachedEnd() {
    return reachedEnd;
  }

  /** "record" or "message" if the data ended inside one, otherwise null. */
  String endedInside() {
    return endedInside;
  }

  /** The next complete handshake message, or null at the end of the cleartext handshake. */
  Message next() {
    while (true) {
      int available = bufferLength - position;
      if (available >= 4) {
        int type = buffer[position] & 0xFF;
        int length = ((buffer[position + 1] & 0xFF) << 16) | ((buffer[position + 2] & 0xFF) << 8)
            | (buffer[position + 3] & 0xFF);
        if (available - 4 >= length) {
          byte[] body = Arrays.copyOfRange(buffer, position + 4, position + 4 + length);
          position += 4 + length;
          return new Message(type, body);
        }
        if (!done && length > MAX_HANDSHAKE_BYTES) {
          context.warn("handshake message of " + length + " bytes in " + direction + " stream exceeds the "
              + MAX_HANDSHAKE_BYTES + "-byte limit");
          done = true;
        }
      }
      if (done) {
        return null;
      }
      readRecord();
    }
  }

  private void readRecord() {
    if (offset == data.length) {
      reachedEnd = true;
      if (bufferLength > position) {
        endedInside = "message";
      }
      done = true;
      return;
    }
    if (data.length - offset < 5) {
      reachedEnd = true;
      endedInside = "record";
      done = true;
      return;
    }
    int type = data[offset] & 0xFF;
    int length = ((data[offset + 3] & 0xFF) << 8) | (data[offset + 4] & 0xFF);
    if (type < CHANGE_CIPHER_SPEC || type > HEARTBEAT || data[offset + 1] != 3 || (data[offset + 2] & 0xFF) > 4
        || length > MAX_RECORD) {
      context.warn("unexpected data in " + direction + " stream at byte " + offset);
      done = true;
      return;
    }
    if (data.length - offset - 5 < length) {
      reachedEnd = true;
      endedInside = "record";
      done = true;
      return;
    }
    int start = offset + 5;
    offset = start + length;
    switch (type) {
      case HANDSHAKE:
        if (bufferLength + length > MAX_HANDSHAKE_BYTES) {
          context.warn("stopped after " + MAX_HANDSHAKE_BYTES + " bytes of handshake data in " + direction + " stream");
          done = true;
          return;
        }
        if (bufferLength + length > buffer.length) {
          buffer = Arrays.copyOf(buffer, Math.min(MAX_HANDSHAKE_BYTES, Math.max(bufferLength + length, buffer.length * 2)));
        }
        System.arraycopy(data, start, buffer, bufferLength, length);
        bufferLength += length;
        break;
      case CHANGE_CIPHER_SPEC:
        if (!continueAfterChangeCipherSpec) {
          done = true;
        }
        break;
      case ALERT:
        break;
      default:
        // Application data or heartbeat: the handshake is over or encrypted from here on
        done = true;
        break;
    }
  }
}
