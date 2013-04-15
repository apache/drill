/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.exec.rpc;

import io.netty.buffer.ByteBuf;

import org.apache.drill.exec.proto.GeneralRPCProtos.RpcMode;

import com.google.protobuf.Internal.EnumLite;
import com.google.protobuf.MessageLite;

class OutboundRpcMessage extends RpcMessage{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(OutboundRpcMessage.class);

  final MessageLite pBody;
  
  public OutboundRpcMessage(RpcMode mode, EnumLite rpcType, int coordinationId, MessageLite pBody, ByteBuf dBody) {
    super(mode, rpcType.getNumber(), coordinationId, dBody);
    this.pBody = pBody;
  }
  
  public int getBodySize(){
    int len = pBody.getSerializedSize();
    len += RpcEncoder.getRawVarintSize(len);
    if(dBody != null) len += dBody.capacity();
    return len;
  }

  @Override
  public String toString() {
    return "OutboundRpcMessage [pBody=" + pBody + ", mode=" + mode + ", rpcType=" + rpcType + ", coordinationId="
        + coordinationId + ", dBody=" + dBody + "]";
  }

  
}
