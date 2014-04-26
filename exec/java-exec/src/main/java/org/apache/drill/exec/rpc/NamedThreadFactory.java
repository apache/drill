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
package org.apache.drill.exec.rpc;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class NamedThreadFactory implements ThreadFactory {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(NamedThreadFactory.class);
  private final AtomicInteger nextId = new AtomicInteger();
  private final String prefix;

  public NamedThreadFactory(String prefix) {
    this.prefix = prefix;
  }

  @Override
  public Thread newThread(Runnable r) {
    Thread t = new Thread(r, prefix + nextId.incrementAndGet());
    try {
      if (t.isDaemon()) {
        t.setDaemon(true);
      }
      if (t.getPriority() != Thread.MAX_PRIORITY) {
        t.setPriority(Thread.MAX_PRIORITY);
      }
    } catch (Exception ignored) {
      // Doesn't matter even if failed to set.
    }
    return t;
  }

}
