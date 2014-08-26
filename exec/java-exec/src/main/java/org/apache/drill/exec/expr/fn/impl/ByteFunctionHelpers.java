/*******************************************************************************

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
 ******************************************************************************/
package org.apache.drill.exec.expr.fn.impl;

import com.google.common.primitives.UnsignedLongs;

import io.netty.util.internal.PlatformDependent;
import org.apache.drill.exec.util.DecimalUtility;

public class ByteFunctionHelpers {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ByteFunctionHelpers.class);

  public static final int equal(final long laddr, int lStart, int lEnd, final long raddr, int rStart,
      final int rEnd) {

    int n = lEnd - lStart;
    if (n == rEnd - rStart) {
      long lPos = laddr + lStart;
      long rPos = raddr + rStart;

      while (n > 7) {
        long leftLong = PlatformDependent.getLong(lPos);
        long rightLong = PlatformDependent.getLong(rPos);
        if (leftLong != rightLong) {
          return 0;
        }
        lPos += 8;
        rPos += 8;
        n -= 8;
      }
      while (n-- != 0) {
        byte leftByte = PlatformDependent.getByte(lPos);
        byte rightByte = PlatformDependent.getByte(rPos);
        if (leftByte != rightByte) {
          return 0;
        }
        lPos++;
        rPos++;
      }
      return 1;
    } else {
      return 0;
    }
  }

  public static final int compare(final long laddr, int lStart, int lEnd, final long raddr, int rStart, final int rEnd) {
    int lLen = lEnd - lStart;
    int rLen = rEnd - rStart;
    int n = Math.min(rLen, lLen);
    long lPos = laddr + lStart;
    long rPos = raddr + rStart;

    while (n > 7) {
      long leftLong = PlatformDependent.getLong(lPos);
      long rightLong = PlatformDependent.getLong(rPos);
      if(leftLong != rightLong){
        return UnsignedLongs.compare(Long.reverseBytes(leftLong), Long.reverseBytes(rightLong));
      }
      lPos += 8;
      rPos += 8;
      n -= 8;
    }

    while (n-- != 0) {
      byte leftByte = PlatformDependent.getByte(lPos);
      byte rightByte = PlatformDependent.getByte(rPos);
      if (leftByte != rightByte) {
        return ((leftByte & 0xFF) - (rightByte & 0xFF)) > 0 ? 1 : -1;
      }
      lPos++;
      rPos++;
    }

    if (lLen == rLen) return 0;

    return lLen > rLen ? 1 : -1;

  }

  public static final int compare(final long laddr, int lStart, int lEnd, final byte[] right, int rStart, final int rEnd) {
    int lLen = lEnd - lStart;
    int rLen = rEnd - rStart;
    int n = Math.min(rLen, lLen);
    long lPos = laddr + lStart;
    int rPos = rStart;



    while (n-- != 0) {
      byte leftByte = PlatformDependent.getByte(lPos);
      byte rightByte = right[rPos];
      if (leftByte != rightByte) {
        return ((leftByte & 0xFF) - (rightByte & 0xFF)) > 0 ? 1 : -1;
      }
      lPos++;
      rPos++;
    }

    if (lLen == rLen) return 0;

    return lLen > rLen ? 1 : -1;

  }
  // Get the big endian integer
  public static int getInteger(byte[] b, int index) {
    int startIndex = index * DecimalUtility.integerSize;

    if (index == 0) {
      return (b[startIndex + 3] & 0xFF) |
             (b[startIndex + 2] & 0xFF) << 8 |
             (b[startIndex + 1] & 0xFF) << 16 |
             (b[startIndex] & 0x7F) << 24;
    }

    return ((b[startIndex + 3] & 0xFF) |
        (b[startIndex + 2] & 0xFF) << 8 |
        (b[startIndex + 1] & 0xFF) << 16 |
        (b[startIndex] & 0xFF) << 24);

    }

  // Set the big endian bytes for the input integer
  public static void setInteger(byte[] b, int index, int value) {
    int startIndex = index * DecimalUtility.integerSize;
    b[startIndex] = (byte) ((value >> 24) & 0xFF);
    b[startIndex + 1] = (byte) ((value >> 16) & 0xFF);
    b[startIndex + 2] = (byte) ((value >> 8) & 0xFF);
    b[startIndex + 3] = (byte) ((value) & 0xFF);
  }

  // Set the sign in a sparse decimal representation
  public static void setSign(byte[] b, boolean sign) {
    int value = getInteger(b, 0);
    if (sign == true) {
      setInteger(b, 0, value | 0x80000000);
    } else {
      setInteger(b, 0, value & 0x7FFFFFFF);
    }
  }

  // Get the sign
  public static boolean getSign(byte[] b) {
    return ((b[0] & 0x80) > 0);
  }
}
