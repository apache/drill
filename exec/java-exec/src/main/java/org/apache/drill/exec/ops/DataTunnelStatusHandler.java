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
package org.apache.drill.exec.ops;

import io.netty.buffer.ByteBuf;
import org.apache.drill.exec.proto.BitData;
import org.apache.drill.exec.rpc.Acks;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.RpcOutcomeListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Listener that keeps track of the status of batches sent, and updates the SendingAccountor when status is received
 * for each batch
 */
public class DataTunnelStatusHandler implements RpcOutcomeListener<BitData.AckWithCredit> {
  private static final Logger logger = LoggerFactory.getLogger(DataTunnelStatusHandler.class);
  private final SendingAccountor sendingAccountor;
  private final Consumer<RpcException> consumer;

  public DataTunnelStatusHandler(Consumer<RpcException> consumer, SendingAccountor sendingAccountor) {
    this.consumer = consumer;
    this.sendingAccountor = sendingAccountor;
  }

  @Override
  public void failed(RpcException ex) {
    sendingAccountor.decrement();
    consumer.accept(ex);
  }

  @Override
  public void success(BitData.AckWithCredit value, ByteBuf buffer) {
    sendingAccountor.decrement();
    if (value.getAllowedCredit() != Acks.FAIL_CREDIT) {
      return;
    }

    logger.error("Data not accepted downstream. Stopping future sends. The receiver has failed to solve the query");
    // if we didn't get ack ok, we'll need to kill the query.
    consumer.accept(new RpcException("Data not accepted downstream."));
  }

  @Override
  public void interrupted(final InterruptedException e) {
    sendingAccountor.decrement();
    consumer.interrupt(e);
  }
}
