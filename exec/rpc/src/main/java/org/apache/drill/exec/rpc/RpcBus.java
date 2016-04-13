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
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.util.concurrent.GenericFutureListener;

import java.io.Closeable;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.drill.common.SerializedExecutor;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.proto.GeneralRPCProtos.RpcMode;
import org.apache.drill.exec.proto.UserBitShared.DrillPBError;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
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

  private static final OutboundRpcMessage PONG = new OutboundRpcMessage(RpcMode.PONG, 0, 0, Acks.OK);
  private static final boolean ENABLE_SEPARATE_THREADS = "true".equals(System.getProperty("drill.enable_rpc_offload"));

  protected abstract MessageLite getResponseDefaultInstance(int rpcType) throws RpcException;

  protected void handle(C connection, int rpcType, ByteBuf pBody, ByteBuf dBody, ResponseSender sender) throws RpcException{
    sender.send(handle(connection, rpcType, pBody, dBody));
  }

  protected abstract Response handle(C connection, int rpcType, ByteBuf pBody, ByteBuf dBody) throws RpcException;

  protected final RpcConfig rpcConfig;

  protected volatile SocketAddress local;
  protected volatile SocketAddress remote;


  public RpcBus(RpcConfig rpcConfig) {
    this.rpcConfig = rpcConfig;
  }

  protected void setAddresses(SocketAddress remote, SocketAddress local){
    this.remote = remote;
    this.local = local;
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

    Preconditions
        .checkArgument(
            allowInEventLoop || !connection.inEventLoop(),
            "You attempted to send while inside the rpc event thread.  This isn't allowed because sending will block if the channel is backed up.");

    ByteBuf pBuffer = null;
    boolean completed = false;

    try {

      if (!allowInEventLoop && !connection.blockOnNotWritable(listener)) {
        // if we're in not in the event loop and we're interrupted while blocking, skip sending this message.
        return;
      }

      assert !Arrays.asList(dataBodies).contains(null);
      assert rpcConfig.checkSend(rpcType, protobufBody.getClass(), clazz);

      Preconditions.checkNotNull(protobufBody);
      ChannelListenerWithCoordinationId futureListener = connection.createNewRpcListener(listener, clazz);
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

  public abstract C initRemoteConnection(SocketChannel channel);

  public class ChannelClosedHandler implements GenericFutureListener<ChannelFuture> {

    final C clientConnection;
    private final Channel channel;

    public ChannelClosedHandler(C clientConnection, Channel channel) {
      this.channel = channel;
      this.clientConnection = clientConnection;
    }

    @Override
    public void operationComplete(ChannelFuture future) throws Exception {
      final String msg;

      if(local!=null) {
        msg = String.format("Channel closed %s <--> %s.", local, remote);
      }else{
        msg = String.format("Channel closed %s <--> %s.", future.channel().localAddress(), future.channel().remoteAddress());
      }

      final ChannelClosedException ex = future.cause() != null ? new ChannelClosedException(msg, future.cause()) : new ChannelClosedException(msg);
      clientConnection.channelClosed(ex);
    }

  }

  protected GenericFutureListener<ChannelFuture> getCloseHandler(SocketChannel channel, C clientConnection) {
    return new ChannelClosedHandler(clientConnection, channel);
  }


  private class ResponseSenderImpl implements ResponseSender {

    private RemoteConnection connection;
    private int coordinationId;
    private final AtomicBoolean sent = new AtomicBoolean(false);

    public ResponseSenderImpl() {
    }

    void set(RemoteConnection connection, int coordinationId){
      this.connection = connection;
      this.coordinationId = coordinationId;
      sent.set(false);
    }

    public void send(Response r) {
      assert rpcConfig.checkResponseSend(r.rpcType, r.pBody.getClass());
      sendOnce();
      OutboundRpcMessage outMessage = new OutboundRpcMessage(RpcMode.RESPONSE, r.rpcType, coordinationId,
          r.pBody, r.dBodies);
      if (RpcConstants.EXTRA_DEBUGGING) {
        logger.debug("Adding message to outbound buffer. {}", outMessage);
      }
      logger.debug("Sending response with Sender {}", System.identityHashCode(this));
      connection.getChannel().writeAndFlush(outMessage);
    }

    /**
     * Ensures that each sender is only used once.
     */
    private void sendOnce() {
      if (!sent.compareAndSet(false, true)) {
        throw new IllegalStateException("Attempted to utilize a sender multiple times.");
      }
    }

    void sendFailure(UserRpcException e){
      sendOnce();
      UserException uex = UserException.systemError(e)
          .addIdentity(e.getEndpoint())
          .build(logger);

      logger.error("Unexpected Error while handling request message", e);

      OutboundRpcMessage outMessage = new OutboundRpcMessage(
          RpcMode.RESPONSE_FAILURE,
          0,
          coordinationId,
          uex.getOrCreatePBError(false)
          );

      if (RpcConstants.EXTRA_DEBUGGING) {
        logger.debug("Adding message to outbound buffer. {}", outMessage);
      }
      connection.getChannel().writeAndFlush(outMessage);
    }

  }

  private class SameExecutor implements Executor {

    @Override
    public void execute(Runnable command) {
      command.run();
    }

  }

  protected class InboundHandler extends MessageToMessageDecoder<InboundRpcMessage> {

    private final Executor exec;
    private final C connection;

    public InboundHandler(C connection) {
      super();
      Preconditions.checkNotNull(connection);
      this.connection = connection;
      final Executor underlyingExecutor = ENABLE_SEPARATE_THREADS ? rpcConfig.getExecutor() : new SameExecutor();
      this.exec = new RpcEventHandler(underlyingExecutor);
    }

    @Override
    protected void decode(final ChannelHandlerContext ctx, final InboundRpcMessage msg, final List<Object> output) throws Exception {
      if (!ctx.channel().isOpen()) {
        return;
      }
      if (RpcConstants.EXTRA_DEBUGGING) {
        logger.debug("Received message {}", msg);
      }
      final Channel channel = connection.getChannel();
      final Stopwatch watch = Stopwatch.createStarted();

      try{

        switch (msg.mode) {
        case REQUEST:
          RequestEvent reqEvent = new RequestEvent(msg.coordinationId, connection, msg.rpcType, msg.pBody, msg.dBody);
          exec.execute(reqEvent);
          break;

        case RESPONSE:
          ResponseEvent respEvent = new ResponseEvent(connection, msg.rpcType, msg.coordinationId, msg.pBody, msg.dBody);
          exec.execute(respEvent);
          break;

        case RESPONSE_FAILURE:
          DrillPBError failure = DrillPBError.parseFrom(new ByteBufInputStream(msg.pBody, msg.pBody.readableBytes()));
          connection.recordRemoteFailure(msg.coordinationId, failure);
          if (RpcConstants.EXTRA_DEBUGGING) {
            logger.debug("Updated rpc future with coordinationId {} with failure ", msg.coordinationId, failure);
          }
          break;

        case PING:
          channel.writeAndFlush(PONG);
          break;

        case PONG:
          // noop.
          break;

        default:
          throw new UnsupportedOperationException();
        }
      } finally {
        long time = watch.elapsed(TimeUnit.MILLISECONDS);
        long delayThreshold = Integer.parseInt(System.getProperty("drill.exec.rpcDelayWarning", "500"));
        if (time > delayThreshold) {
          logger.warn(String.format(
              "Message of mode %s of rpc type %d took longer than %dms.  Actual duration was %dms.",
              msg.mode, msg.rpcType, delayThreshold, time));
        }
        msg.release();
      }
    }
  }

  public static <T> T get(ByteBuf pBody, Parser<T> parser) throws RpcException {
    try {
      ByteBufInputStream is = new ByteBufInputStream(pBody);
      return parser.parseFrom(is);
    } catch (InvalidProtocolBufferException e) {
      throw new RpcException(String.format("Failure while decoding message with parser of type. %s", parser.getClass().getCanonicalName()), e);
    }
  }

  class RpcEventHandler extends SerializedExecutor {

    public RpcEventHandler(Executor underlyingExecutor) {
      super(rpcConfig.getName() + "-rpc-event-queue", underlyingExecutor);
    }

    @Override
    protected void runException(Runnable command, Throwable t) {
      logger.error("Failure while running rpc command.", t);
    }

  }

  private class RequestEvent implements Runnable {
    private final ResponseSenderImpl sender;
    private final C connection;
    private final int rpcType;
    private final ByteBuf pBody;
    private final ByteBuf dBody;

    RequestEvent(int coordinationId, C connection, int rpcType, ByteBuf pBody, ByteBuf dBody) {
      sender = new ResponseSenderImpl();
      this.connection = connection;
      this.rpcType = rpcType;
      this.pBody = pBody;
      this.dBody = dBody;
      sender.set(connection, coordinationId);

      if(pBody != null){
        pBody.retain();
      }

      if(dBody != null){
        dBody.retain();
      }
    }

    @Override
    public void run() {
      try {
        handle(connection, rpcType, pBody, dBody, sender);
      } catch (UserRpcException e) {
        sender.sendFailure(e);
      } catch (Exception e) {
        logger.error("Failure while handling message.", e);
      }finally{
        if(pBody != null){
          pBody.release();
        }

        if(dBody != null){
          dBody.release();
        }
      }

    }


  }


  private class ResponseEvent implements Runnable {

    private final int rpcType;
    private final int coordinationId;
    private final ByteBuf pBody;
    private final ByteBuf dBody;
    private final C connection;

    public ResponseEvent(C connection, int rpcType, int coordinationId, ByteBuf pBody, ByteBuf dBody) {
      this.rpcType = rpcType;
      this.coordinationId = coordinationId;
      this.pBody = pBody;
      this.dBody = dBody;
      this.connection = connection;

      if(pBody != null){
        pBody.retain();
      }

      if(dBody != null){
        dBody.retain();
      }
    }

    public void run(){
      try {
        MessageLite m = getResponseDefaultInstance(rpcType);
        assert rpcConfig.checkReceive(rpcType, m.getClass());
        RpcOutcome<?> rpcFuture = connection.getAndRemoveRpcOutcome(rpcType, coordinationId, m.getClass());
        Parser<?> parser = m.getParserForType();
        Object value = parser.parseFrom(new ByteBufInputStream(pBody, pBody.readableBytes()));
        rpcFuture.set(value, dBody);
        if (RpcConstants.EXTRA_DEBUGGING) {
          logger.debug("Updated rpc future {} with value {}", rpcFuture, value);
        }
      } catch (Exception ex) {
        logger.error("Failure while handling response.", ex);
      }finally{
        if(pBody != null){
          pBody.release();
        }

        if(dBody != null){
          dBody.release();
        }

      }

    }

  }
}
