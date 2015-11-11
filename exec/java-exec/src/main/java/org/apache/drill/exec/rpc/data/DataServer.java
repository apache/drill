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

import org.apache.drill.exec.exception.FragmentSetupException;
import org.apache.drill.exec.memory.AllocatorClosedException;
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
import org.apache.drill.exec.util.Pointer;
import org.apache.drill.exec.work.fragment.FragmentManager;

import com.google.protobuf.MessageLite;

public class DataServer extends BasicServer<RpcType, BitServerConnection> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DataServer.class);

  private volatile ProxyCloseHandler proxyCloseHandler;
  private final BootStrapContext context;
  private final WorkEventBus workBus;
  private final DataResponseHandler dataHandler;

  public DataServer(BootStrapContext context, WorkEventBus workBus, DataResponseHandler dataHandler) {
    super(
        DataRpcConfig.getMapping(context.getConfig(), context.getExecutor()),
        context.getAllocator().getUnderlyingAllocator(),
        context.getBitLoopGroup());
    this.context = context;
    this.workBus = workBus;
    this.dataHandler = dataHandler;
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

  private final static FragmentRecordBatch OOM_FRAGMENT = FragmentRecordBatch.newBuilder().setIsOutOfMemory(true).build();


  private static FragmentHandle getHandle(FragmentRecordBatch batch, int index) {
    return FragmentHandle.newBuilder()
        .setQueryId(batch.getQueryId())
        .setMajorFragmentId(batch.getReceivingMajorFragmentId())
        .setMinorFragmentId(batch.getReceivingMinorFragmentId(index))
        .build();
  }


  @Override
  protected void handle(BitServerConnection connection, int rpcType, ByteBuf pBody, ByteBuf body, ResponseSender sender) throws RpcException {
    assert rpcType == RpcType.REQ_RECORD_BATCH_VALUE;

    final FragmentRecordBatch fragmentBatch = get(pBody, FragmentRecordBatch.PARSER);
    final int targetCount = fragmentBatch.getReceivingMinorFragmentIdCount();

    AckSender ack = new AckSender(sender);
    // increment so we don't get false returns.
    ack.increment();
    try {

      if(body == null){

        for(int minor = 0; minor < targetCount; minor++){
          FragmentManager manager = workBus.getFragmentManager(getHandle(fragmentBatch, minor));
          if(manager != null){
            ack.increment();
            dataHandler.handle(manager, fragmentBatch, null, ack);
          }
        }

      }else{
        for (int minor = 0; minor < targetCount; minor++) {
          send(fragmentBatch, (DrillBuf) body, minor, ack);
        }
      }
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

  private void send(final FragmentRecordBatch fragmentBatch, final DrillBuf body, final int minor, final AckSender ack)
      throws FragmentSetupException, IOException {

    final FragmentManager manager = workBus.getFragmentManager(getHandle(fragmentBatch, minor));
    if (manager == null) {
      return;
    }

    final BufferAllocator allocator = manager.getFragmentContext().getAllocator();
    final Pointer<DrillBuf> out = new Pointer<>();

    final boolean withinMemoryEnvelope;

    try {
      withinMemoryEnvelope = allocator.shareOwnership((DrillBuf) body, out);
    } catch(final AllocatorClosedException e) {
      /*
       * It can happen that between the time we get the fragment manager and we
       * try to transfer this buffer to it, the fragment may have been cancelled
       * and closed. When that happens, the allocator will be closed when we
       * attempt this. That just means we can drop this data on the floor, since
       * the receiver no longer exists (and no longer needs it).
       *
       * Note that checking manager.isCancelled() before we attempt this isn't enough,
       * because of timing: it may still be cancelled between that check and
       * the attempt to do the memory transfer. To double check ourselves, we
       * do check manager.isCancelled() here, after the fact; it shouldn't
       * change again after its allocator has been closed.
       */
      assert manager.isCancelled();
      return;
    }

    if (!withinMemoryEnvelope) {
      // if we over reserved, we need to add poison pill before batch.
      dataHandler.handle(manager, OOM_FRAGMENT, null, null);
    }

    ack.increment();
    dataHandler.handle(manager, fragmentBatch, out.value, ack);

    // make sure to release the reference count we have to the new buffer.
    // dataHandler.handle should have taken any ownership it needed.
    out.value.release();
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
        dataHandler.informOutOfMemory();
      }
    };
  }

  @Override
  public ProtobufLengthDecoder getDecoder(BufferAllocator allocator, OutOfMemoryHandler outOfMemoryHandler) {
    return new DataProtobufLengthDecoder.Server(allocator, outOfMemoryHandler);
  }

}
