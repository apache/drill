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
package org.apache.drill.exec.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;

/**
 * Wraps a parent counter so that local in-thread metrics can be collected while collecting for a global counter. Note
 * that this is one writer, many reader safe.
 */
public class SingleThreadNestedCounter {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SingleThreadNestedCounter.class);

  private volatile long count;
  private final Counter counter;

  public SingleThreadNestedCounter(MetricRegistry registry, String name) {
    super();
    this.counter = registry.counter(name);
  }

  public long inc(long n) {
    counter.inc(n);
    count += n;
    return count;
  }

  public long dec(long n) {
    counter.dec(n);
    count -= n;
    return count;
  }

  public long get() {
    return count;
  }

}
