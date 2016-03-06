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

import io.netty.buffer.DrillBuf;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class HashHelper {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HashHelper.class);

  public static int hash32(int val, long seed) {
    double converted = val;
    return hash32(converted, seed);
  }
  public static int hash32(long val, long seed) {
    double converted = val;
    return hash32(converted, seed);
  }

  public static int hash32(float val, long seed){
    double converted = val;
    return hash32(converted, seed);
  }

  public static int hash32(double val, long seed) {
    //return com.google.common.hash.Hashing.murmur3_128().hashLong(Double.doubleToLongBits(val)).asInt();
    return org.apache.drill.exec.expr.fn.impl.MurmurHash3.murmur3_32(Double.doubleToLongBits(val), (int)seed);
  }
  public static int hash32(int start, int end, DrillBuf buffer, int seed){
    return org.apache.drill.exec.expr.fn.impl.MurmurHash3.murmur3_32(start, end, buffer, seed);
  }

  public static long hash64(float val, long seed){
    double converted = val;
    return hash64(converted, seed);
  }

  public static long hash64(double val, long seed){
    return org.apache.drill.exec.expr.fn.impl.MurmurHash3.murmur3_64(Double.doubleToLongBits(val), (int)seed);
  }

  public static long hash64(long val, long seed){
    double converted = val;
    return hash64(converted, seed);

  }

  public static long hash64(long start, long end, DrillBuf buffer, long seed){
    return org.apache.drill.exec.expr.fn.impl.MurmurHash3.murmur3_64(start, end, buffer, (int)seed);
  }
}
