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
package org.apache.drill.exec.store.pcap.protocol.ntp;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

import org.apache.drill.exec.store.pcap.protocol.DecoderContext;

/**
 * Parses NTP messages (RFC 5905 and earlier versions). Modes 1 to 5 carry the 48-byte
 * header; extension fields and a MAC may follow and are ignored. Modes 6 (control, as
 * used by ntpq) and 7 (private, as used by ntpdc, including monlist) have their own short
 * headers, of which only the request code is returned.
 */
public final class NtpParser {
  static final int HEADER_LENGTH = 48;
  private static final long NTP_UNIX_OFFSET = 2208988800L;
  private static final long ERA_SECONDS = 1L << 32;
  private static final int MAX_STRATUM = 16;
  private static final int CONTROL_HEADER = 12;
  private static final int PRIVATE_HEADER = 8;
  private static final String[] MODES = {
      null, "symmetric_active", "symmetric_passive", "client", "server", "broadcast", "control", "private"};

  private NtpParser() { }

  /**
   * @return the message, or null if the data is not NTP
   * @throws IllegalArgumentException if a control or private message is shorter than its header says
   */
  public static NtpMessage parse(byte[] data, DecoderContext context) {
    if (data == null || data.length < 1) {
      return null;
    }
    int first = data[0] & 0xFF;
    int version = (first >> 3) & 0x07;
    int mode = first & 0x07;
    if (version == 0 || version > 4 || mode == 0) {
      return null;
    }
    NtpMessage m = new NtpMessage();
    m.version = version;
    m.mode = MODES[mode];
    if (mode == 6) {
      return control(data, m);
    }
    if (mode == 7) {
      return privateMode(data, m);
    }
    if (data.length < HEADER_LENGTH || (data[1] & 0xFF) > MAX_STRATUM) {
      return null;
    }
    m.leapIndicator = first >> 6;
    m.stratum = data[1] & 0xFF;
    m.poll = (int) data[2];
    m.precision = (int) data[3];
    m.rootDelay = u32(data, 4) / 65536.0;
    m.rootDispersion = u32(data, 8) / 65536.0;
    m.referenceId = referenceId(data, m.stratum);
    m.referenceTime = timestamp(data, 16);
    m.originTime = timestamp(data, 24);
    m.receiveTime = timestamp(data, 32);
    m.transmitTime = timestamp(data, 40);
    return m;
  }

  private static NtpMessage control(byte[] data, NtpMessage m) {
    if (data.length < CONTROL_HEADER) {
      return null;
    }
    int count = u16(data, 10);
    if (CONTROL_HEADER + count > data.length) {
      throw new IllegalArgumentException("truncated control message: count " + count + " exceeds "
          + (data.length - CONTROL_HEADER) + " data bytes");
    }
    m.requestCode = data[1] & 0x1F;
    return m;
  }

  private static NtpMessage privateMode(byte[] data, NtpMessage m) {
    if (data.length < PRIVATE_HEADER) {
      return null;
    }
    int items = u16(data, 4) & 0x0FFF;
    int itemSize = u16(data, 6) & 0x0FFF;
    if (PRIVATE_HEADER + items * itemSize > data.length) {
      throw new IllegalArgumentException("truncated private message: " + items + " items of " + itemSize
          + " bytes exceed " + (data.length - PRIVATE_HEADER) + " data bytes");
    }
    m.requestCode = data[3] & 0xFF;
    return m;
  }

  /** Kiss code or reference clock name for stratum 0 and 1, otherwise an IPv4 address. Null if zero. */
  private static String referenceId(byte[] data, int stratum) {
    if (u32(data, 12) == 0) {
      return null;
    }
    if (stratum <= 1) {
      int length = 0;
      while (length < 4 && data[12 + length] != 0) {
        int c = data[12 + length] & 0xFF;
        if (c < 0x20 || c > 0x7E) {
          length = -1;
          break;
        }
        length++;
      }
      if (length > 0) {
        return new String(data, 12, length, StandardCharsets.US_ASCII).trim();
      }
    }
    return (data[12] & 0xFF) + "." + (data[13] & 0xFF) + "." + (data[14] & 0xFF) + "." + (data[15] & 0xFF);
  }

  /**
   * NTP timestamps count seconds from 1900. Seconds with the high bit clear are taken to be in
   * era 1 (from 2036), as RFC 4330 suggests. A zero timestamp means "not set" and is null.
   */
  static Instant timestamp(byte[] data, int offset) {
    long seconds = u32(data, offset);
    long fraction = u32(data, offset + 4);
    if (seconds == 0 && fraction == 0) {
      return null;
    }
    if ((seconds & 0x80000000L) == 0) {
      seconds += ERA_SECONDS;
    }
    return Instant.ofEpochSecond(seconds - NTP_UNIX_OFFSET, (fraction * 1_000_000_000L) >>> 32);
  }

  private static int u16(byte[] data, int offset) {
    return ((data[offset] & 0xFF) << 8) | (data[offset + 1] & 0xFF);
  }

  private static long u32(byte[] data, int offset) {
    return ((long) u16(data, offset) << 16) | u16(data, offset + 2);
  }
}
