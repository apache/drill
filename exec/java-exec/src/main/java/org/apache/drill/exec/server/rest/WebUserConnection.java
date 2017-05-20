/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.server.rest;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.DrillBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelPromise;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.physical.impl.materialize.QueryWritableBatch;
import org.apache.drill.exec.proto.GeneralRPCProtos;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.rpc.AbstractUserClientConnectionWrapper;
import org.apache.drill.exec.rpc.Acks;
import org.apache.drill.exec.rpc.ChannelClosedException;
import org.apache.drill.exec.rpc.ConnectionThrottle;
import org.apache.drill.exec.rpc.RpcOutcomeListener;
import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.exec.vector.ValueVector;

import java.net.SocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class WebUserConnection extends AbstractUserClientConnectionWrapper implements ConnectionThrottle {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(WebUserConnection.class);

  /**
   * WebUserConnectionWrapper which represents the UserClientConnection for the WebUser submitting the query. It provides
   * access to the UserSession executing the query. There is no actual physical channel corresponding to this connection
   * wrapper.
   */

  protected BufferAllocator allocator;

  protected SocketAddress remoteAddress;

  protected UserSession webUserSession;

  protected final ChannelPromise closeFuture;

  public final List<Map<String, String>> results = Lists.newArrayList();

  public final Set<String> columns = Sets.newLinkedHashSet();

  WebUserConnection(BufferAllocator allocator, UserSession webUserSession,
                    SocketAddress remoteAddress, ChannelPromise closeFuture) {
    this.allocator = allocator;
    this.webUserSession = webUserSession;
    this.remoteAddress = remoteAddress;
    this.closeFuture = closeFuture;
  }

  @Override
  public UserSession getSession() {
    return webUserSession;
  }

  @Override
  public void sendData(RpcOutcomeListener<GeneralRPCProtos.Ack> listener, QueryWritableBatch result) {

    // Check if there is any data or not. There can be overflow here but DrillBuf doesn't support allocating with
    // bytes in long. Hence we are just preserving the earlier behavior and logging debug log for the case.
    final int dataByteCount = (int) result.getByteCount();

    if (dataByteCount <= 0) {
      if (logger.isDebugEnabled()) {
        logger.debug("Either no data received for this query or there is BufferOverflow in dataByteCount: {}",
            dataByteCount);
      }
      latch.countDown();
      listener.success(Acks.OK, null);
      return;
    }

    // If here that means there is some data for sure. Create a ByteBuf with all the data in it.
    final int rows = result.getHeader().getRowCount();
    final DrillBuf bufferWithData = allocator.buffer(dataByteCount);
    try {
      final ByteBuf[] resultDataBuffers = result.getBuffers();

      for (final ByteBuf buffer : resultDataBuffers) {
        bufferWithData.writeBytes(buffer);
        buffer.release();
      }

      final RecordBatchLoader loader = new RecordBatchLoader(allocator);
      try {
        loader.load(result.getHeader().getDef(), bufferWithData);
        // TODO:  Clean:  DRILL-2933:  That load(...) no longer throws
        // SchemaChangeException, so check/clean catch clause below.
        for (int i = 0; i < loader.getSchema().getFieldCount(); ++i) {
          columns.add(loader.getSchema().getColumn(i).getPath());
        }
        for (int i = 0; i < rows; ++i) {
          final Map<String, String> record = Maps.newHashMap();
          for (VectorWrapper<?> vw : loader) {
            final String field = vw.getValueVector().getMetadata().getNamePart().getName();
            final ValueVector.Accessor accessor = vw.getValueVector().getAccessor();
            final Object value = i < accessor.getValueCount() ? accessor.getObject(i) : null;
            final String display = value == null ? null : value.toString();
            record.put(field, display);
          }
          results.add(record);
        }
      } finally {
        loader.clear();
      }
    } catch (Exception e) { // not catching OOM here
      exception = UserException.systemError(e).build(logger);
      latch.countDown();
    } finally {
      bufferWithData.release();
    }

    // Notify the listener with ACK.OK because data is send successfully from Drillbit even though there is error found
    // during processing.
    listener.success(Acks.OK, null);
  }

  @Override
  public ChannelFuture getChannelClosureFuture() {
    return closeFuture;
  }

  @Override
  public SocketAddress getRemoteAddress() {
    return remoteAddress;
  }

  @Override
  public void setAutoRead(boolean enableAutoRead) {
    // no-op
  }

  /**
   * For anonymous WebUserSession it is called after each query request whereas for Authenticated WebUserSession
   * it is never called.
   */
  public void cleanupSession() {
    // no-op
  }

  public static class AnonWebUserConnection extends WebUserConnection {

    AnonWebUserConnection(BufferAllocator allocator, UserSession webUserSession,
                          SocketAddress remoteAddress, ChannelPromise closeFuture) {
      super(allocator, webUserSession, remoteAddress, closeFuture);
    }

    /**
     * For anonymous WebUserSession it is called after each query request whereas for Authenticated WebUserSession
     * it is never called.
     */
    @Override
    public void cleanupSession() {
      if (webUserSession != null) {
        webUserSession.close();
        webUserSession = null;
      }

      if(allocator != null) {
        allocator.close();
        allocator = null;
      }
    }
  }
}