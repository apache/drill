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
package org.apache.drill.exec.physical.impl.partitionsender;


import io.netty.buffer.ByteBuf;

import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.SendingAccountor;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.rpc.BaseRpcOutcomeListener;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.work.ErrorHelper;

public class StatusHandler extends BaseRpcOutcomeListener<Ack> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StatusHandler.class);
  RpcException ex;
  SendingAccountor sendCount;
  FragmentContext context;
  boolean ok = true;

  public StatusHandler(SendingAccountor sendCount, FragmentContext context) {
    this.sendCount = sendCount;
    this.context = context;
  }

  @Override
  public void success(Ack value, ByteBuf buffer) {
    sendCount.decrement();
    super.success(value, buffer);
  }

  @Override
  public void failed(RpcException ex) {
    sendCount.decrement();
    logger.error("Failure while sending data to user.", ex);
    ErrorHelper.logAndConvertError(context.getIdentity(), "Failure while sending fragment to client.", ex, logger);
    ok = false;
    this.ex = ex;
  }

  public boolean isOk() {
    return ok;
  }

  public RpcException getException() {
    return ex;
  }

}
