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
import io.netty.buffer.DrillBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.concurrent.GenericFutureListener;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.drill.exec.exception.FragmentSetupException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.BitData.BitClientHandshake;
import org.apache.drill.exec.proto.BitData.BitServerHandshake;
import org.apache.drill.exec.proto.BitData.FragmentRecordBatch;
import org.apache.drill.exec.proto.BitData.RpcType;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.UserBitShared.RpcChannel;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
import org.apache.drill.exec.rpc.Acks;
import org.apache.drill.exec.rpc.BasicServer;
import org.apache.drill.exec.rpc.OutOfMemoryHandler;
import org.apache.drill.exec.rpc.ProtobufLengthDecoder;
import org.apache.drill.exec.rpc.Response;
import org.apache.drill.exec.rpc.ResponseSender;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.control.WorkEventBus;
import org.apache.drill.exec.server.BootStrapContext;
import org.apache.drill.exec.work.WorkManager.WorkerBee;
import org.apache.drill.exec.work.fragment.FragmentManager;

import com.google.protobuf.MessageLite;

public class DataServer extends BasicServer<RpcType, BitServerConnection> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DataServer.class);

  private volatile ProxyCloseHandler proxyCloseHandler;
  private final BootStrapContext context;
  private final WorkEventBus workBus;
  private final WorkerBee bee;

  public DataServer(BootStrapContext context, BufferAllocator alloc, WorkEventBus workBus,
      WorkerBee bee) {
    super(
        DataRpcConfig.getMapping(context.getConfig(), context.getExecutor()),
        alloc.getAsByteBufAllocator(),
        context.getBitLoopGroup());
    this.context = context;
    this.workBus = workBus;
    this.bee = bee;
  }

  @Override
  public MessageLite getResponseDefaultInstance(int rpcType) throws RpcException {
    return DataDefaultInstanceHandler.getResponseDefaultInstanceServer(rpcType);
  }

  @Override
  protected GenericFutureListener<ChannelFuture> getCloseHandler(SocketChannel ch, BitServerConnection connection) {
    this.proxyCloseHandler = new ProxyCloseHandler(super.getCloseHandler(ch, connection));
    return proxyCloseHandler;
  }

  @Override
  public BitServerConnection initRemoteConnection(SocketChannel channel) {
    super.initRemoteConnection(channel);
    return new BitServerConnection(channel, context.getAllocator());
  }

  @Override
  protected ServerHandshakeHandler<BitClientHandshake> getHandshakeHandler(final BitServerConnection connection) {
    return new ServerHandshakeHandler<BitClientHandshake>(RpcType.HANDSHAKE, BitClientHandshake.PARSER) {

      @Override
      public MessageLite getHandshakeResponse(BitClientHandshake inbound) throws Exception {
        // logger.debug("Handling handshake from other bit. {}", inbound);
        if (inbound.getRpcVersion() != DataRpcConfig.RPC_VERSION) {
          throw new RpcException(String.format("Invalid rpc version.  Expected %d, actual %d.",
              inbound.getRpcVersion(), DataRpcConfig.RPC_VERSION));
        }
        if (inbound.getChannel() != RpcChannel.BIT_DATA) {
          throw new RpcException(String.format("Invalid NodeMode.  Expected BIT_DATA but received %s.",
              inbound.getChannel()));
        }

        return BitServerHandshake.newBuilder().setRpcVersion(DataRpcConfig.RPC_VERSION).build();
      }

    };
  }

  private static FragmentHandle getHandle(FragmentRecordBatch batch, int index) {
    return FragmentHandle.newBuilder()
        .setQueryId(batch.getQueryId())
        .setMajorFragmentId(batch.getReceivingMajorFragmentId())
        .setMinorFragmentId(batch.getReceivingMinorFragmentId(index))
        .build();
  }

  private void submit(IncomingDataBatch batch, int minorStart, int minorStopExclusive) throws FragmentSetupException,
      IOException {
    for (int minor = minorStart; minor < minorStopExclusive; minor++) {
      final FragmentManager manager = workBus.getFragmentManager(getHandle(batch.getHeader(), minor));
      if (manager == null) {
        // A missing manager means the query already terminated. We can simply drop this data.
        continue;
      }

      final boolean canRun = manager.handle(batch);
      if (canRun) {
        // logger.debug("Arriving batch means local batch can run, starting local batch.");
        /*
         * If we've reached the canRun threshold, we'll proceed. This expects manager.handle() to only return a single
         * true. This is guaranteed by the interface.
         */
        bee.startFragmentPendingRemote(manager);
      }
    }

  }

  @Override
  protected void handle(BitServerConnection connection, int rpcType, ByteBuf pBody, ByteBuf body, ResponseSender sender) throws RpcException {
    assert rpcType == RpcType.REQ_RECORD_BATCH_VALUE;

    final FragmentRecordBatch fragmentBatch = get(pBody, FragmentRecordBatch.PARSER);
    final AckSender ack = new AckSender(sender);


    // increment so we don't get false returns.
    ack.increment();

    try {

      final IncomingDataBatch batch = new IncomingDataBatch(fragmentBatch, (DrillBuf) body, ack);
      final int targetCount = fragmentBatch.getReceivingMinorFragmentIdCount();

      // randomize who gets first transfer (and thus ownership) so memory usage is balanced when we're sharing amongst
      // multiple fragments.
      final int firstOwner = ThreadLocalRandom.current().nextInt(targetCount);
      submit(batch, firstOwner, targetCount);
      submit(batch, 0, firstOwner);

    } catch (IOException | FragmentSetupException e) {
      logger.error("Failure while getting fragment manager. {}",
          QueryIdHelper.getQueryIdentifiers(fragmentBatch.getQueryId(),
              fragmentBatch.getReceivingMajorFragmentId(),
              fragmentBatch.getReceivingMinorFragmentIdList()), e);
      ack.clear();
      sender.send(new Response(RpcType.ACK, Acks.FAIL));
    } finally {

      // decrement the extra reference we grabbed at the top.
      ack.sendOk();
    }
  }


  private class ProxyCloseHandler implements GenericFutureListener<ChannelFuture> {

    private volatile GenericFutureListener<ChannelFuture> handler;

    public ProxyCloseHandler(GenericFutureListener<ChannelFuture> handler) {
      super();
      this.handler = handler;
    }

    @Override
    public void operationComplete(ChannelFuture future) throws Exception {
      handler.operationComplete(future);
    }

  }

  @Override
  public OutOfMemoryHandler getOutOfMemoryHandler() {
    return new OutOfMemoryHandler() {
      @Override
      public void handle() {
        logger.error("Out of memory in RPC layer.");
      }
    };
  }

  @Override
  public ProtobufLengthDecoder getDecoder(BufferAllocator allocator, OutOfMemoryHandler outOfMemoryHandler) {
    return new DataProtobufLengthDecoder.Server(allocator, outOfMemoryHandler);
  }

}
