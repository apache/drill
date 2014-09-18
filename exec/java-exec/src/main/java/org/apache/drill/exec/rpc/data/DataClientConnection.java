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
package org.apache.drill.exec.rpc.data;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

import java.util.UUID;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.BitData.RpcType;
import org.apache.drill.exec.rpc.RemoteConnection;
import org.apache.drill.exec.rpc.RpcOutcomeListener;

import com.google.common.io.Closeables;
import com.google.protobuf.MessageLite;

public class DataClientConnection extends RemoteConnection{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DataClientConnection.class);

  private final DataClient client;
  private final UUID id;

  public DataClientConnection(Channel channel, DataClient client) {
    super(channel);
    this.client = client;
    // we use a local listener pool unless a global one is provided.
    this.id = UUID.randomUUID();
  }

  @Override
  public BufferAllocator getAllocator() {
    return client.getAllocator();
  }

  public <SEND extends MessageLite, RECEIVE extends MessageLite> void send(RpcOutcomeListener<RECEIVE> outcomeListener, RpcType rpcType,
      SEND protobufBody, Class<RECEIVE> clazz, ByteBuf... dataBodies) {
    client.send(outcomeListener, this, rpcType, protobufBody, clazz, dataBodies);

  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((id == null) ? 0 : id.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    DataClientConnection other = (DataClientConnection) obj;
    if (id == null) {
      if (other.id != null) {
        return false;
      }
    } else if (!id.equals(other.id)) {
      return false;
    }
    return true;
  }

  public void shutdownIfClient() {
    Closeables.closeQuietly(client);
  }

}
