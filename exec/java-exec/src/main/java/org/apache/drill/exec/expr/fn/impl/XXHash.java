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

import org.apache.drill.exec.util.AssertionUtil;

import io.netty.buffer.DrillBuf;
import io.netty.util.internal.PlatformDependent;

import com.google.common.primitives.UnsignedLongs;

public final class XXHash {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(XXHash.class);

  private static final boolean BOUNDS_CHECKING_ENABLED = AssertionUtil.BOUNDS_CHECKING_ENABLED;

  static final long PRIME64_1 = UnsignedLongs.decode("11400714785074694791");
  static final long PRIME64_2 = UnsignedLongs.decode("14029467366897019727");
  static final long PRIME64_3 = UnsignedLongs.decode("1609587929392839161");
  static final long PRIME64_4 = UnsignedLongs.decode("9650029242287828579");
  static final long PRIME64_5 = UnsignedLongs.decode("2870177450012600261");

  private static long hash64(long start, long bEnd, long seed) {
    long len = bEnd - start;
    long h64;
    long p = start;

    // for long strings (greater than 32 bytes)
    if (len >= 32) {
      final long limit = bEnd - 32;
      long v1 = seed + PRIME64_1 + PRIME64_2;
      long v2 = seed + PRIME64_2;
      long v3 = seed + 0;
      long v4 = seed - PRIME64_1;

      do {
        v1 += PlatformDependent.getLong(p) * PRIME64_2;
        p = p + 8;
        v1 = Long.rotateLeft(v1, 31);
        v1 *= PRIME64_1;

        v2 += PlatformDependent.getLong(p) * PRIME64_2;
        p = p + 8;
        v2 = Long.rotateLeft(v2, 31);
        v2 *= PRIME64_1;

        v3 += PlatformDependent.getLong(p) * PRIME64_2;
        p = p + 8;
        v3 = Long.rotateLeft(v3, 31);
        v3 *= PRIME64_1;

        v4 += PlatformDependent.getLong(p) * PRIME64_2;
        p = p + 8;
        v4 = Long.rotateLeft(v4, 31);
        v4 *= PRIME64_1;
      } while (p <= limit);

      h64 = Long.rotateLeft(v1, 1) + Long.rotateLeft(v2, 7) + Long.rotateLeft(v3, 12) + Long.rotateLeft(v4, 18);

      v1 *= PRIME64_2;
      v1 = Long.rotateLeft(v1, 31);
      v1 *= PRIME64_1;
      h64 ^= v1;

      h64 = h64 * PRIME64_1 + PRIME64_4;

      v2 *= PRIME64_2;
      v2 = Long.rotateLeft(v2, 31);
      v2 *= PRIME64_1;
      h64 ^= v2;

      h64 = h64 * PRIME64_1 + PRIME64_4;

      v3 *= PRIME64_2;
      v3 = Long.rotateLeft(v3, 31);
      v3 *= PRIME64_1;
      h64 ^= v3;

      h64 = h64 * PRIME64_1 + PRIME64_4;

      v4 *= PRIME64_2;
      v4 = Long.rotateLeft(v4, 31);
      v4 *= PRIME64_1;
      h64 ^= v4;

      h64 = h64 * PRIME64_1 + PRIME64_4;
    } else {
      h64 = seed + PRIME64_5;
    }

    h64 += len;

    while (p + 8 <= bEnd) {
      long k1 = PlatformDependent.getLong(p);
      k1 *= PRIME64_2;
      k1 = Long.rotateLeft(k1, 31);
      k1 *= PRIME64_1;
      h64 ^= k1;
      h64 = Long.rotateLeft(h64, 27) * PRIME64_1 + PRIME64_4;
      p += 8;
    }

    if (p + 4 <= bEnd) {
      h64 ^= PlatformDependent.getInt(p) * PRIME64_1;
      h64 = Long.rotateLeft(h64, 23) * PRIME64_2 + PRIME64_3;
      p += 4;
    }
    while (p < bEnd) {
      h64 ^= PlatformDependent.getByte(p) * PRIME64_5;
      h64 = Long.rotateLeft(h64, 11) * PRIME64_1;
      p++;
    }

    return applyFinalHashComputation(h64);
  }

  public static int hash32(int start, int end, DrillBuf buffer){
    if(BOUNDS_CHECKING_ENABLED){
      buffer.checkBytes(start, end);
    }

    long s = buffer.memoryAddress() + start;
    long e = buffer.memoryAddress() + end;

    return hash32(s, e, 0);
  }

  public static int hash32(int val, int seed){
    long h64 = seed + PRIME64_5;
    h64 += 4; // add length (4 bytes) to hash value
    h64 ^= val * PRIME64_1;
    h64 = Long.rotateLeft(h64, 23) * PRIME64_2 + PRIME64_3;
    return (int) applyFinalHashComputation(h64);
  }

  public static int hash32(float val, int seed){
    return hash32(Float.floatToIntBits(val), seed);
  }

  public static int hash32(double val, int seed){
    return hash32(Double.doubleToLongBits(val), seed);
  }

  public static int hash32(long val, int seed){
    long h64 = seed + PRIME64_5;
    h64 += 8; // add length (8 bytes) to hash value
    long k1 = val* PRIME64_2;
    k1 = Long.rotateLeft(k1, 31);
    k1 *= PRIME64_1;
    h64 ^= k1;
    h64 = Long.rotateLeft(h64, 27) * PRIME64_1 + PRIME64_4;
    return (int) applyFinalHashComputation(h64);
  }

  private static int hash32(long start, long bEnd, long seed){
    return (int) hash64(start, bEnd, seed);
  }

  private static long applyFinalHashComputation(long h64) {
    h64 ^= h64 >> 33;
    h64 *= PRIME64_2;
    h64 ^= h64 >> 29;
    h64 *= PRIME64_3;
    h64 ^= h64 >> 32;
    return h64;
  }
}
