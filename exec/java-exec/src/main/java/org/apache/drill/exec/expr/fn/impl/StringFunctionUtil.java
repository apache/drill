/**
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
package org.apache.drill.exec.expr.fn.impl;

import org.apache.drill.common.exceptions.DrillRuntimeException;

import io.netty.buffer.ByteBuf;

public class StringFunctionUtil {

  /* Decode the input bytebuf using UTF-8, and return the number of characters
   */
  public static int getUTF8CharLength(ByteBuf buffer, int start, int end) {
    int charCount = 0;

    for (int idx = start, charLen = 0; idx < end; idx += charLen) {
      charLen = utf8CharLen(buffer, idx);
      ++charCount;  //Advance the counter, since we find one char.
    }
    return charCount;
  }

  /* Decode the input bytebuf using UTF-8. Search in the range of [start, end], find
   * the position of the first byte of next char after we see "charLength" chars.
   *
   */
  public static int getUTF8CharPosition(ByteBuf buffer, int start, int end, int charLength) {
    int charCount = 0;

    if (start >= end)
      return -1;  //wrong input here.

    for (int idx = start, charLen = 0; idx < end; idx += charLen) {
      charLen = utf8CharLen(buffer, idx);
      ++charCount;  //Advance the counter, since we find one char.
      if (charCount == charLength + 1) {
        return idx;
      }
    }
    return end;
  }

  public static int stringLeftMatchUTF8(ByteBuf str, int strStart, int strEnd,
                                    ByteBuf substr, int subStart, int subEnd) {
    for (int i = strStart; i <= strEnd - (subEnd - subStart); i++) {
      int j = subStart;
      for (; j< subEnd; j++) {
        if (str.getByte(i + j - subStart) != substr.getByte(j))
          break;
      }

      if (j == subEnd  && j!= subStart) {  // found a matched substr (non-empty) in str.
        return i;   // found a match.
      }
    }

    return -1;
  }

  /**
   * Return a printable representation of a byte buffer, escaping the non-printable
   * bytes as '\\xNN' where NN is the hexadecimal representation of such bytes.
   *
   * This function does not modify  the {@code readerIndex} and {@code writerIndex}
   * of the byte buffer.
   */
  public static String toBinaryString(ByteBuf buf, int strStart, int strEnd) {
    StringBuilder result = new StringBuilder();
    for (int i = strStart; i < strEnd ; ++i) {
      int ch = buf.getByte(i) & 0xFF;
      if ( (ch >= '0' && ch <= '9')
          || (ch >= 'A' && ch <= 'Z')
          || (ch >= 'a' && ch <= 'z')
          || " `~!@#$%^&*()-_=+[]{}|;:'\",.<>/?".indexOf(ch) >= 0 ) {
          result.append((char)ch);
      } else {
        result.append(String.format("\\x%02X", ch));
      }
    }
    return result.toString();
  }

  /**
   * In-place parsing of a hex encoded binary string.
   *
   * This function does not modify  the {@code readerIndex} and {@code writerIndex}
   * of the byte buffer.
   *
   * @return Index in the byte buffer just after the last written byte.
   */
  public static int parseBinaryString(ByteBuf str, int strStart, int strEnd) {
    int length = (strEnd - strStart);
    int dstEnd = strStart;
    for (int i = strStart; i < length ; i++) {
      byte b = str.getByte(i);
      if (b == '\\'
          && length > i+3
          && (str.getByte(i+1) == 'x' || str.getByte(i+1) == 'X')) {
        // ok, take next 2 hex digits.
        byte hd1 = str.getByte(i+2);
        byte hd2 = str.getByte(i+3);
        if (isHexDigit(hd1) && isHexDigit(hd2)) { // [a-fA-F0-9]
          // turn hex ASCII digit -> number
          b = (byte) ((toBinaryFromHex(hd1) << 4) + toBinaryFromHex(hd2));
          i += 3; // skip 3
        }
      }
      str.setByte(dstEnd++, b);
    }
    return dstEnd;
  }

  /**
   * Takes a ASCII digit in the range A-F0-9 and returns
   * the corresponding integer/ordinal value.
   * @param ch  The hex digit.
   * @return The converted hex value as a byte.
   */
  private static byte toBinaryFromHex(byte ch) {
    if ( ch >= 'A' && ch <= 'F' )
      return (byte) ((byte)10 + (byte) (ch - 'A'));
    else if ( ch >= 'a' && ch <= 'f' )
      return (byte) ((byte)10 + (byte) (ch - 'a'));
    return (byte) (ch - '0');
  }

  private static boolean isHexDigit(byte c) {
    return (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F') || (c >= '0' && c <= '9');
  }

  private static int utf8CharLen(ByteBuf buffer, int idx) {
    byte firstByte = buffer.getByte(idx);
    if (firstByte >= 0) { // 1-byte char. First byte is 0xxxxxxx.
      return 1;
    } else if ((firstByte & 0xE0) == 0xC0) { // 2-byte char. First byte is 110xxxxx
      return 2;
    } else if ((firstByte & 0xF0) == 0xE0) { // 3-byte char. First byte is 1110xxxx
      return 3;
    } else if ((firstByte & 0xF8) == 0xF0) { //4-byte char. First byte is 11110xxx
      return 4;
    }
    throw new DrillRuntimeException("Unexpected byte 0x" + Integer.toString((int)firstByte & 0xff, 16)
        + " at position " + idx + " encountered while decoding UTF8 string.");
  }

}
