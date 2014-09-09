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
package org.apache.drill.exec.physical.impl;

import java.util.concurrent.Semaphore;

/**
 * Account for whether all messages sent have been completed. Necessary before finishing a task so we don't think
 * buffers are hanging when they will be released.
 *
 * TODO: Need to update to use long for number of pending messages.
 */
public class SendingAccountor {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SendingAccountor.class);

  private int batchesSent = 0;
  private Semaphore wait = new Semaphore(0);

  public void increment() {
    batchesSent++;
  }

  public void decrement() {
    wait.release();
  }

  public synchronized void waitForSendComplete() {
    try {
      wait.acquire(batchesSent);
      batchesSent = 0;
    } catch (InterruptedException e) {
      logger.warn("Failure while waiting for send complete.", e);
    }
  }
}
