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

import io.netty.buffer.ByteBuf;

import com.google.protobuf.MessageLite;

public abstract class ListeningCommand<T extends MessageLite, C extends RemoteConnection> implements RpcCommand<T, C> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ListeningCommand.class);

  private final RpcOutcomeListener<T> listener;

  public ListeningCommand(RpcOutcomeListener<T> listener) {
    this.listener = listener;
  }

  public abstract void doRpcCall(RpcOutcomeListener<T> outcomeListener, C connection);

  @Override
  public void connectionAvailable(C connection) {

    doRpcCall(new DeferredRpcOutcome(), connection);
  }

  @Override
  public void connectionSucceeded(C connection) {
    connectionAvailable(connection);
  }

  private class DeferredRpcOutcome implements RpcOutcomeListener<T> {

    @Override
    public void failed(RpcException ex) {
      listener.failed(ex);
    }

    @Override
    public void success(T value, ByteBuf buf) {
      listener.success(value, buf);
    }

    @Override
    public void interrupted(final InterruptedException e) {
      listener.interrupted(e);
    }
  }

  @Override
  public void connectionFailed(FailureType type, Throwable t) {
    listener.failed(RpcException.mapException(
        String.format("Command failed while establishing connection.  Failure type %s.", type), t));
  }

}