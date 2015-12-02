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

import io.netty.buffer.DrillBuf;
import io.netty.util.internal.PlatformDependent;

import org.apache.drill.exec.memory.BoundsChecking;
import org.apache.drill.exec.util.DecimalUtility;

import com.google.common.primitives.UnsignedLongs;

public class ByteFunctionHelpers {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ByteFunctionHelpers.class);

  /**
   * Helper function to check for equality of bytes in two DrillBuffers
   *
   * @param left Left DrillBuf for comparison
   * @param lStart start offset in the buffer
   * @param lEnd end offset in the buffer
   * @param right Right DrillBuf for comparison
   * @param rStart start offset in the buffer
   * @param rEnd end offset in the buffer
   * @return 1 if left input is greater, -1 if left input is smaller, 0 otherwise
   */
  public static final int equal(final DrillBuf left, int lStart, int lEnd, final DrillBuf right, int rStart, int rEnd){
    if (BoundsChecking.BOUNDS_CHECKING_ENABLED) {
      left.checkBytes(lStart, lEnd);
      right.checkBytes(rStart, rEnd);
    }
    return memEqual(left.memoryAddress(), lStart, lEnd, right.memoryAddress(), rStart, rEnd);
  }

  private static final int memEqual(final long laddr, int lStart, int lEnd, final long raddr, int rStart,
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

  /**
   * Helper function to compare a set of bytes in two DrillBuffers.
   *
   * Function will check data before completing in the case that
   *
   * @param left Left DrillBuf to compare
   * @param lStart start offset in the buffer
   * @param lEnd end offset in the buffer
   * @param right Right DrillBuf to compare
   * @param rStart start offset in the buffer
   * @param rEnd end offset in the buffer
   * @return 1 if left input is greater, -1 if left input is smaller, 0 otherwise
   */
  public static final int compare(final DrillBuf left, int lStart, int lEnd, final DrillBuf right, int rStart, int rEnd){
    if (BoundsChecking.BOUNDS_CHECKING_ENABLED) {
      left.checkBytes(lStart, lEnd);
      right.checkBytes(rStart, rEnd);
    }
    return memcmp(left.memoryAddress(), lStart, lEnd, right.memoryAddress(), rStart, rEnd);
  }

  private static final int memcmp(final long laddr, int lStart, int lEnd, final long raddr, int rStart, final int rEnd) {
    int lLen = lEnd - lStart;
    int rLen = rEnd - rStart;
    int n = Math.min(rLen, lLen);
    long lPos = laddr + lStart;
    long rPos = raddr + rStart;

    while (n > 7) {
      long leftLong = PlatformDependent.getLong(lPos);
      long rightLong = PlatformDependent.getLong(rPos);
      if (leftLong != rightLong) {
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

    if (lLen == rLen) {
      return 0;
    }

    return lLen > rLen ? 1 : -1;

  }

  /**
   * Helper function to compare a set of bytes in DrillBuf to a ByteArray.
   *
   * @param left Left DrillBuf for comparison purposes
   * @param lStart start offset in the buffer
   * @param lEnd end offset in the buffer
   * @param right second input to be compared
   * @param rStart start offset in the byte array
   * @param rEnd end offset in the byte array
   * @return 1 if left input is greater, -1 if left input is smaller, 0 otherwise
   */
  public static final int compare(final DrillBuf left, int lStart, int lEnd, final byte[] right, int rStart, final int rEnd) {
    if (BoundsChecking.BOUNDS_CHECKING_ENABLED) {
      left.checkBytes(lStart, lEnd);
    }
    return memcmp(left.memoryAddress(), lStart, lEnd, right, rStart, rEnd);
  }


  private static final int memcmp(final long laddr, int lStart, int lEnd, final byte[] right, int rStart, final int rEnd) {
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

    if (lLen == rLen) {
      return 0;
    }

    return lLen > rLen ? 1 : -1;
  }

  /*
   * Following are helper functions to interact with sparse decimal represented in a byte array.
   */

  // Get the integer ignore the sign
  public static int getInteger(byte[] b, int index) {
    return getInteger(b, index, true);
  }
  // Get the integer, ignore the sign
  public static int getInteger(byte[] b, int index, boolean ignoreSign) {
    int startIndex = index * DecimalUtility.INTEGER_SIZE;

    if (index == 0 && ignoreSign == true) {
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

  // Set integer in the byte array
  public static void setInteger(byte[] b, int index, int value) {
    int startIndex = index * DecimalUtility.INTEGER_SIZE;
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
    return ((getInteger(b, 0, false) & 0x80000000) != 0);
  }

}
