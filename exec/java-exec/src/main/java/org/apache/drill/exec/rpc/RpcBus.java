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
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.util.concurrent.GenericFutureListener;

import java.io.Closeable;
import java.util.Arrays;
import java.util.List;

import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.GeneralRPCProtos.RpcMode;
import org.apache.drill.exec.proto.UserBitShared.DrillPBError;
import org.apache.drill.exec.work.ErrorHelper;

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

  protected void handle(C connection, int rpcType, ByteBuf pBody, ByteBuf dBody, ResponseSender sender) throws RpcException{
    sender.send(handle(connection, rpcType, pBody, dBody));
  }

  protected abstract Response handle(C connection, int rpcType, ByteBuf pBody, ByteBuf dBody) throws RpcException;

  public abstract boolean isClient();

  protected final RpcConfig rpcConfig;

  public RpcBus(RpcConfig rpcConfig) {
    this.rpcConfig = rpcConfig;
  }

  <SEND extends MessageLite, RECEIVE extends MessageLite> DrillRpcFuture<RECEIVE> send(C connection, T rpcType,
      SEND protobufBody, Class<RECEIVE> clazz, ByteBuf... dataBodies) {
    DrillRpcFutureImpl<RECEIVE> rpcFuture = new DrillRpcFutureImpl<RECEIVE>();
    this.send(rpcFuture, connection, rpcType, protobufBody, clazz, dataBodies);
    return rpcFuture;
  }

  public <SEND extends MessageLite, RECEIVE extends MessageLite> void send(RpcOutcomeListener<RECEIVE> listener, C connection, T rpcType,
      SEND protobufBody, Class<RECEIVE> clazz, ByteBuf... dataBodies) {
    send(listener, connection, rpcType, protobufBody, clazz, false, dataBodies);
  }

  public <SEND extends MessageLite, RECEIVE extends MessageLite> void send(RpcOutcomeListener<RECEIVE> listener, C connection, T rpcType,
      SEND protobufBody, Class<RECEIVE> clazz, boolean allowInEventLoop, ByteBuf... dataBodies) {

    if (!allowInEventLoop) {
      if (connection.inEventLoop()) {
        throw new IllegalStateException("You attempted to send while inside the rpc event thread.  This isn't allowed because sending will block if the channel is backed up.");
      }

      if (!connection.blockOnNotWritable(listener)) {
        return;
      }
    }

    ByteBuf pBuffer = null;
    boolean completed = false;

    try {

      assert !Arrays.asList(dataBodies).contains(null);
      assert rpcConfig.checkSend(rpcType, protobufBody.getClass(), clazz);

      Preconditions.checkNotNull(protobufBody);
      ChannelListenerWithCoordinationId futureListener = queue.get(listener, clazz, connection);
      OutboundRpcMessage m = new OutboundRpcMessage(RpcMode.REQUEST, rpcType, futureListener.getCoordinationId(), protobufBody, dataBodies);
      ChannelFuture channelFuture = connection.getChannel().writeAndFlush(m);
      channelFuture.addListener(futureListener);
      channelFuture.addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
      completed = true;
    } catch (Exception | AssertionError e) {
      listener.failed(new RpcException("Failure sending message.", e));
    } finally {
      if (!completed) {
        if (pBuffer != null) {
          pBuffer.release();
        }
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
    if (this.isClient()) {
      queue.channelClosed(new ChannelClosedException("Queue closed due to channel closure."));
    }
  }

  protected GenericFutureListener<ChannelFuture> getCloseHandler(C clientConnection) {
    return new ChannelClosedHandler();
  }

  private class ResponseSenderImpl implements ResponseSender {

    RemoteConnection connection;
    int coordinationId;

    public ResponseSenderImpl(RemoteConnection connection, int coordinationId) {
      super();
      this.connection = connection;
      this.coordinationId = coordinationId;
    }

    public void send(Response r) {
      assert rpcConfig.checkResponseSend(r.rpcType, r.pBody.getClass());
      OutboundRpcMessage outMessage = new OutboundRpcMessage(RpcMode.RESPONSE, r.rpcType, coordinationId,
          r.pBody, r.dBodies);
      if (RpcConstants.EXTRA_DEBUGGING) {
        logger.debug("Adding message to outbound buffer. {}", outMessage);
      }
      connection.getChannel().writeAndFlush(outMessage);
    }

  }

  protected class InboundHandler extends MessageToMessageDecoder<InboundRpcMessage> {

    private final C connection;
    public InboundHandler(C connection) {
      super();
      this.connection = connection;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, InboundRpcMessage msg, List<Object> output) throws Exception {
      if (!ctx.channel().isOpen()) {
        return;
      }
      if (RpcConstants.EXTRA_DEBUGGING) {
        logger.debug("Received message {}", msg);
      }
      switch (msg.mode) {
      case REQUEST: {
        // handle message and ack.
        ResponseSender sender = new ResponseSenderImpl(connection, msg.coordinationId);
        try {
          handle(connection, msg.rpcType, msg.pBody, msg.dBody, sender);
        } catch(UserRpcException e){
          DrillPBError error = ErrorHelper.logAndConvertError(e.getEndpoint(), e.getUserMessage(), e, logger);
          OutboundRpcMessage outMessage = new OutboundRpcMessage(RpcMode.RESPONSE_FAILURE, 0, msg.coordinationId, error);
          if (RpcConstants.EXTRA_DEBUGGING) {
            logger.debug("Adding message to outbound buffer. {}", outMessage);
          }
          connection.getChannel().writeAndFlush(outMessage);
        }
        msg.release();  // we release our ownership.  Handle could have taken over ownership.
        break;
      }

      case RESPONSE:
        try{
        MessageLite m = getResponseDefaultInstance(msg.rpcType);
        assert rpcConfig.checkReceive(msg.rpcType, m.getClass());
        RpcOutcome<?> rpcFuture = queue.getFuture(msg.rpcType, msg.coordinationId, m.getClass());
        Parser<?> parser = m.getParserForType();
        Object value = parser.parseFrom(new ByteBufInputStream(msg.pBody, msg.pBody.readableBytes()));
        rpcFuture.set(value, msg.dBody);
        msg.release();  // we release our ownership.  Handle could have taken over ownership.
        if (RpcConstants.EXTRA_DEBUGGING) {
          logger.debug("Updated rpc future {} with value {}", rpcFuture, value);
        }
        }catch(Exception ex) {
          logger.error("Failure while handling response.", ex);
          throw ex;
        }
        break;

      case RESPONSE_FAILURE:
        DrillPBError failure = DrillPBError.parseFrom(new ByteBufInputStream(msg.pBody, msg.pBody.readableBytes()));
        queue.updateFailedFuture(msg.coordinationId, failure);
        msg.release();
        if (RpcConstants.EXTRA_DEBUGGING) {
          logger.debug("Updated rpc future with coordinationId {} with failure ", msg.coordinationId, failure);
        }
        break;

      default:
        throw new UnsupportedOperationException();
      }
    }

  }

  public static <T> T get(ByteBuf pBody, Parser<T> parser) throws RpcException{
    try {
      ByteBufInputStream is = new ByteBufInputStream(pBody);
      return parser.parseFrom(is);
    } catch (InvalidProtocolBufferException e) {
      throw new RpcException(String.format("Failure while decoding message with parser of type. %s", parser.getClass().getCanonicalName()), e);
    }
  }

}
