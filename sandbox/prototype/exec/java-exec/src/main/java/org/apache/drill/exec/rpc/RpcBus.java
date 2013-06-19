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
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundMessageHandlerAdapter;
import io.netty.util.concurrent.GenericFutureListener;

import java.io.Closeable;
import java.util.Arrays;
import java.util.concurrent.CancellationException;

import org.apache.drill.exec.proto.GeneralRPCProtos.RpcFailure;
import org.apache.drill.exec.proto.GeneralRPCProtos.RpcMode;
import org.slf4j.Logger;

import com.google.common.base.Preconditions;
import com.google.protobuf.Internal.EnumLite;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;

/**
 * The Rpc Bus deals with incoming and outgoing communication and is used on both the server and the client side of a
 * system.
 * 
 * @param <T>
 */
public abstract class RpcBus<T extends EnumLite, C extends RemoteConnection> implements Closeable {
  final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(this.getClass());

  protected final CoordinationQueue queue = new CoordinationQueue(16, 16);

  protected abstract MessageLite getResponseDefaultInstance(int rpcType) throws RpcException;

  protected abstract Response handle(C connection, int rpcType, ByteBuf pBody, ByteBuf dBody) throws RpcException;

  public abstract boolean isClient();

  protected final RpcConfig rpcConfig;

  public RpcBus(RpcConfig rpcConfig) {
    this.rpcConfig = rpcConfig;
  }

  public <SEND extends MessageLite, RECEIVE extends MessageLite> DrillRpcFuture<RECEIVE> send(C connection, T rpcType,
      SEND protobufBody, Class<RECEIVE> clazz, ByteBuf... dataBodies) {
    DrillRpcFutureImpl<RECEIVE> rpcFuture = new DrillRpcFutureImpl<RECEIVE>();
    this.send(rpcFuture, connection, rpcType, protobufBody, clazz, dataBodies);
    return rpcFuture;
  }  
  
  public <SEND extends MessageLite, RECEIVE extends MessageLite> void send(RpcOutcomeListener<RECEIVE> listener, C connection, T rpcType,
      SEND protobufBody, Class<RECEIVE> clazz, ByteBuf... dataBodies) {
  
    


    assert !Arrays.asList(dataBodies).contains(null);
    assert rpcConfig.checkSend(rpcType, protobufBody.getClass(), clazz);

    ByteBuf pBuffer = null;
    boolean completed = false;

    try {
      Preconditions.checkNotNull(protobufBody);
      ChannelListenerWithCoordinationId futureListener = queue.get(listener, clazz);
      OutboundRpcMessage m = new OutboundRpcMessage(RpcMode.REQUEST, rpcType, futureListener.getCoordinationId(), protobufBody, dataBodies);
      logger.debug("Outbound RPC message: {}.    DATA BODIES: {}", m, dataBodies);
      ChannelFuture channelFuture = connection.getChannel().write(m);
      channelFuture.addListener(futureListener);
      completed = true;
    } finally {
      if (!completed) {
        if (pBuffer != null) pBuffer.release();
        if (dataBodies != null) {
          for (ByteBuf b : dataBodies) {
            b.release();
          }

        }
      }
      ;
    }
  }

  public abstract C initRemoteConnection(Channel channel);

  public class ChannelClosedHandler implements GenericFutureListener<ChannelFuture> {
    @Override
    public void operationComplete(ChannelFuture future) throws Exception {
      logger.info("Channel closed between local {} and remote {}", future.channel().localAddress(), future.channel()
          .remoteAddress());
      closeQueueDueToChannelClose();
    }
  }

  protected void closeQueueDueToChannelClose() {
    if (this.isClient()) queue.channelClosed(new ChannelClosedException());
  }

  protected GenericFutureListener<ChannelFuture> getCloseHandler(C clientConnection) {
    return new ChannelClosedHandler();
  }

  protected class InboundHandler extends ChannelInboundMessageHandlerAdapter<InboundRpcMessage> {

    private final C connection;
    public InboundHandler(C connection) {
      super();
      this.connection = connection;
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, InboundRpcMessage msg) throws Exception {
      if (!ctx.channel().isOpen()) return;
      if (RpcConstants.EXTRA_DEBUGGING) logger.debug("Received message {}", msg);
      switch (msg.mode) {
      case REQUEST:
        // handle message and ack.
        Response r = handle(connection, msg.rpcType, msg.pBody, msg.dBody);
        assert rpcConfig.checkResponseSend(r.rpcType, r.pBody.getClass());
        OutboundRpcMessage outMessage = new OutboundRpcMessage(RpcMode.RESPONSE, r.rpcType, msg.coordinationId,
            r.pBody, r.dBodies);
        if (RpcConstants.EXTRA_DEBUGGING) logger.debug("Adding message to outbound buffer. {}", outMessage);
        ctx.write(outMessage);
        break;

      case RESPONSE:
        MessageLite m = getResponseDefaultInstance(msg.rpcType);
        assert rpcConfig.checkReceive(msg.rpcType, m.getClass());
        RpcOutcome<?> rpcFuture = queue.getFuture(msg.rpcType, msg.coordinationId, m.getClass());
        Parser<?> parser = m.getParserForType();
        Object value = parser.parseFrom(new ByteBufInputStream(msg.pBody, msg.pBody.readableBytes()));
        rpcFuture.set(value);
        if (RpcConstants.EXTRA_DEBUGGING) logger.debug("Updated rpc future {} with value {}", rpcFuture, value);

        break;

      case RESPONSE_FAILURE:
        RpcFailure failure = RpcFailure.parseFrom(new ByteBufInputStream(msg.pBody, msg.pBody.readableBytes()));
        queue.updateFailedFuture(msg.coordinationId, failure);
        if (RpcConstants.EXTRA_DEBUGGING)
          logger.debug("Updated rpc future with coordinationId {} with failure ", msg.coordinationId, failure);
        break;

      default:
        throw new UnsupportedOperationException();
      }
    }

  }

//  private class Listener implements GenericFutureListener<ChannelFuture> {
//
//    private int coordinationId;
//    private Class<?> clazz;
//
//    public Listener(int coordinationId, Class<?> clazz) {
//      this.coordinationId = coordinationId;
//      this.clazz = clazz;
//    }
//
//    @Override
//    public void operationComplete(ChannelFuture channelFuture) throws Exception {
//      // logger.debug("Completed channel write.");
//
//      if (channelFuture.isCancelled()) {
//        RpcOutcome<?> rpcFuture = queue.getFuture(-1, coordinationId, clazz);
//        rpcFuture.setException(new CancellationException("Socket operation was canceled."));
//      } else if (!channelFuture.isSuccess()) {
//        try {
//          channelFuture.get();
//          throw new IllegalStateException("Future was described as completed and not succesful but did not throw an exception.");
//        } catch (Exception e) {
//          logger.error("Error occurred during Rpc", e);
//          RpcOutcome<?> rpcFuture = queue.getFuture(-1, coordinationId, clazz);
//          rpcFuture.setException(e);
//        }
//      } else {
//        // send was successful. No need to modify DrillRpcFuture.
//        return;
//      }
//    }
//
//  }

  public static <T> T get(ByteBuf pBody, Parser<T> parser) throws RpcException{
    try {
      ByteBufInputStream is = new ByteBufInputStream(pBody);
      return parser.parseFrom(is);
    } catch (InvalidProtocolBufferException e) {
      throw new RpcException(String.format("Failure while decoding message with parser of type. %s", parser.getClass().getCanonicalName()), e);
    }
  }
}
