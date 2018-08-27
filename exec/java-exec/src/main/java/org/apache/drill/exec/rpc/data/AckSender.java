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
package org.apache.drill.exec.rpc.data;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.drill.exec.rpc.ResponseSender;

import org.apache.drill.shaded.guava.com.google.common.annotations.VisibleForTesting;

/**
 * Utility class that allows a group of receivers to confirm reception of a record batch as a single unit. Response
 * isn't send upstream until all receivers have successfully consumed data.
 */
public class AckSender {

  private AtomicInteger count = new AtomicInteger(0);
  private ResponseSender sender;

  @VisibleForTesting
  public AckSender(ResponseSender sender) {
    this.sender = sender;
  }

  /**
   * Add another sender to wait for.
   */
  void increment() {
    count.incrementAndGet();
  }

  /**
   * Disable any sending of the ok message.
   */
  void clear() {
    count.set(-100000);
  }

  /**
   * Decrement the number of references still holding on to this response. When the number of references hit zero, send
   * response upstream.
   */
  public void sendOk() {
    if (0 == count.decrementAndGet()) {
      sender.send(DataRpcConfig.OK);
    }
  }
}
