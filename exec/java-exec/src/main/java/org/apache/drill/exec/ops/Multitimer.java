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
package org.apache.drill.exec.ops;

import org.slf4j.Logger;

public class Multitimer<T extends Enum<T>> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Multitimer.class);

  private final long start;
  private final long[] times;
  private final Class<T> clazz;

  public Multitimer(Class<T> clazz){
    this.times = new long[clazz.getEnumConstants().length];
    this.start = System.nanoTime();
    this.clazz = clazz;
  }

  public void mark(T timer){
    times[timer.ordinal()] = System.nanoTime();
  }

  public void log(Logger logger){

  }
}
