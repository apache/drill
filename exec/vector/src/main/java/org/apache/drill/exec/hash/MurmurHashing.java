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
package org.apache.drill.exec.hash;

import io.netty.buffer.DrillBuf;
import io.netty.util.internal.PlatformDependent;

/**
 * 32bit hashing for string and so on.
 * murmurhash3
 */
public class MurmurHashing
{
  public static int hash32(int bStart, int bEnd, DrillBuf buffer, int seed)
  {
    final long c1 = 0xcc9e2d51L;
    final long c2 = 0x1b873593L;
    long start = buffer.memoryAddress() + bStart;
    long length = bEnd - bStart;
    long UINT_MASK=0xffffffffL;
    long lh1 = seed;
    long roundedEnd = start + (length & 0xfffffffc);  // round down to 4 byte block

    for (long i=start; i<roundedEnd; i+=4) {
      // little endian load order
      long lk1 = (PlatformDependent.getByte(i) & 0xff) | ((PlatformDependent.getByte(i + 1) & 0xff) << 8) |
                 ((PlatformDependent.getByte(i+2) & 0xff) << 16) | (PlatformDependent.getByte(i+3) << 24);

      //k1 *= c1;
      lk1 *= c1;
      lk1 &= UINT_MASK;

      lk1 = ((lk1 << 15) & UINT_MASK) | (lk1 >>> 17);

      lk1 *= c2;
      lk1 = lk1 & UINT_MASK;
      lh1 ^= lk1;
      lh1 = ((lh1 << 13) & UINT_MASK) | (lh1 >>> 19);

      lh1 = lh1*5+0xe6546b64L;
      lh1 = UINT_MASK & lh1;
    }

    // tail
    long lk1 = 0;

    switch((byte)length & 0x03) {
      case 3:
        lk1 = (PlatformDependent.getByte(roundedEnd + 2) & 0xff) << 16;
      case 2:
        lk1 |= (PlatformDependent.getByte(roundedEnd + 1) & 0xff) << 8;
      case 1:
        lk1 |= (PlatformDependent.getByte(roundedEnd) & 0xff);
        lk1 *= c1;
        lk1 = UINT_MASK & lk1;
        lk1 = ((lk1 << 15) & UINT_MASK) | (lk1 >>> 17);

        lk1 *= c2;
        lk1 = lk1 & UINT_MASK;

        lh1 ^= lk1;
    }

    // finalization
    lh1 ^= length;

    lh1 ^= lh1 >>> 16;
    lh1 *= 0x85ebca6b;
    lh1 = UINT_MASK & lh1;
    lh1 ^= lh1 >>> 13;

    lh1 *= 0xc2b2ae35;
    lh1 = UINT_MASK & lh1;
    lh1 ^= lh1 >>> 16;

    return (int)(lh1 & UINT_MASK);
  }

  public static int murmur3_32(long val, int seed) {
    final long c1 = 0xcc9e2d51L;
    final long c2 = 0x1b873593;
    long length = 8;
    long UINT_MASK=0xffffffffL;
    long lh1 = seed & UINT_MASK;
    for (int i=0; i<2; i++) {
      //int ik1 = (int)((val >> i*32) & UINT_MASK);
      long lk1 = ((val >> i*32) & UINT_MASK);

      //k1 *= c1;
      lk1 *= c1;
      lk1 &= UINT_MASK;

      lk1 = ((lk1 << 15) & UINT_MASK) | (lk1 >>> 17);

      lk1 *= c2;
      lk1 &= UINT_MASK;

      lh1 ^= lk1;
      lh1 = ((lh1 << 13) & UINT_MASK) | (lh1 >>> 19);

      lh1 = lh1*5+0xe6546b64L;
      lh1 = UINT_MASK & lh1;
    }
    // finalization
    lh1 ^= length;

    lh1 ^= lh1 >>> 16;
    lh1 *= 0x85ebca6bL;
    lh1 = UINT_MASK & lh1;
    lh1 ^= lh1 >>> 13;
    lh1 *= 0xc2b2ae35L;
    lh1 = UINT_MASK & lh1;
    lh1 ^= lh1 >>> 16;

    return (int)lh1;
  }

  public static int hash32(double val, long seed) {
    return murmur3_32(Double.doubleToLongBits(val), (int) seed);
  }
}
