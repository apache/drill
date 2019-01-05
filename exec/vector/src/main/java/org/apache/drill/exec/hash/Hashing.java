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

public class Hashing
{
  public static int hash32(char key) {
    int intv = key;
    return hash32(intv);
  }

  public static int hash32(int key) {
    int hashv = IntegerHashing.hash32(key);
    return hashv;
  }

  public static int hash32(long key) {
    int hashv = IntegerHashing.hash32(key);
    return hashv;
  }

  public static int hash32(float key) {
    double doublev = key;
    return hash32(doublev);
  }

  public static int hash32(double key) {
    int hashv = MurmurHashing.hash32(key, 0);
    return hashv;
  }

  /**
   * for variable length data type
   * @param bstart
   * @param bend
   * @param data
   * @return
   */
  public static int hash32(int bstart, int bend, DrillBuf data) {
    return MurmurHashing.hash32(bstart, bend, data, 0);
  }

  /**
   * a java version of C++ Boost hash_combine
   * @param seed
   * @param hashv
   * @return
   */
  public static int hashCombine(int seed, int hashv) {
    return seed ^ (hashv + 0x9e3779b9 + (seed << 6) + (seed >> 2));
  }
}
