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
package org.apache.drill.exec.rpc.control;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.DrillBuf;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.drill.common.concurrent.AutoCloseableLock;
import org.apache.drill.exec.proto.BitControl.CustomMessage;
import org.apache.drill.exec.proto.BitControl.RpcType;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.rpc.Response;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.UserRpcException;
import org.apache.drill.exec.rpc.control.Controller.CustomMessageHandler;
import org.apache.drill.exec.rpc.control.Controller.CustomResponse;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;

public class CustomHandlerRegistry {
  // private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CustomHandlerRegistry.class);

  private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
  private final AutoCloseableLock read = new AutoCloseableLock(readWriteLock.readLock());
  private final AutoCloseableLock write = new AutoCloseableLock(readWriteLock.writeLock());
  private final IntObjectOpenHashMap<ParsingHandler<?>> handlers = new IntObjectOpenHashMap<>();
  private volatile DrillbitEndpoint endpoint;

  public CustomHandlerRegistry() {
  }

  public void setEndpoint(DrillbitEndpoint endpoint) {
    this.endpoint = endpoint;
  }

  public <SEND extends MessageLite> void registerCustomHandler(int messageTypeId,
      CustomMessageHandler<SEND, ?> handler,
      Parser<SEND> parser) {
    Preconditions.checkNotNull(handler);
    Preconditions.checkNotNull(parser);
    try (AutoCloseableLock lock = write.open()) {
      ParsingHandler<?> parsingHandler = handlers.get(messageTypeId);
      if (parsingHandler != null) {
        throw new IllegalStateException(String.format(
            "Only one handler can be registered for a given custom message type. You tried to register a handler for "
                + "the %d message type but one had already been registered.",
            messageTypeId));
      }

      parsingHandler = new ParsingHandler<SEND>(handler, parser);
      handlers.put(messageTypeId, parsingHandler);
    }
  }

  public Response handle(CustomMessage message, DrillBuf dBody) throws RpcException {
    final ParsingHandler<?> handler;
    try (AutoCloseableLock lock = read.open()) {
      handler = handlers.get(message.getType());
    }

    if (handler == null) {
      throw new UserRpcException(
          endpoint, "Unable to handle message.",
          new IllegalStateException(String.format(
              "Unable to handle message. The message type provided [%d] did not have a registered handler.",
              message.getType())));
    }
    final CustomResponse<?> customResponse = handler.onMessage(message.getMessage(), dBody);
    final CustomMessage responseMessage = CustomMessage.newBuilder()
        .setMessage(customResponse.getMessage().toByteString())
        .setType(message.getType())
        .build();
    // make sure we don't pass in a null array.
    final ByteBuf[] dBodies = customResponse.getBodies() == null ? new DrillBuf[0] : customResponse.getBodies();
    return new Response(RpcType.RESP_CUSTOM, responseMessage, dBodies);

  }

  private class ParsingHandler<SEND extends MessageLite> {
    private final CustomMessageHandler<SEND, ?> handler;
    private final Parser<SEND> parser;

    public ParsingHandler(CustomMessageHandler<SEND, ?> handler, Parser<SEND> parser) {
      super();
      this.handler = handler;
      this.parser = parser;
    }

    public CustomResponse<?> onMessage(ByteString pBody, DrillBuf dBody) throws UserRpcException {

      try {
        final SEND message = parser.parseFrom(pBody);
        return handler.onMessage(message, dBody);

      } catch (InvalidProtocolBufferException e) {
        throw new UserRpcException(endpoint, "Failure parsing message.", e);
      }

    }
  }
}
