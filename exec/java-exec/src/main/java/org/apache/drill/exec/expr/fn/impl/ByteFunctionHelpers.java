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

    return lLen > rLen ? 1 : 0;

  }


}
