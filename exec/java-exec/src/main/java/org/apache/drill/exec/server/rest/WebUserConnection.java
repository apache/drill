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
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.physical.impl.materialize.QueryWritableBatch;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.rpc.AbstractDisposableUserClientConnection;
import org.apache.drill.exec.rpc.Acks;
import org.apache.drill.exec.rpc.ConnectionThrottle;
import org.apache.drill.exec.rpc.RpcOutcomeListener;
import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.exec.vector.ValueVector.Accessor;

import java.net.SocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * WebUserConnectionWrapper which represents the UserClientConnection for the WebUser submitting the query. It provides
 * access to the UserSession executing the query. There is no actual physical channel corresponding to this connection
 * wrapper.
 */

public class WebUserConnection extends AbstractDisposableUserClientConnection implements ConnectionThrottle {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(WebUserConnection.class);

  protected WebSessionResources webSessionResources;

  public final List<Map<String, String>> results = Lists.newArrayList();

  public final Set<String> columns = Sets.newLinkedHashSet();

  WebUserConnection(WebSessionResources webSessionResources) {
    this.webSessionResources = webSessionResources;
  }

  @Override
  public UserSession getSession() {
    return webSessionResources.getSession();
  }

  @Override
  public void sendData(RpcOutcomeListener<Ack> listener, QueryWritableBatch result) {
    // Check if there is any data or not. There can be overflow here but DrillBuf doesn't support allocating with
    // bytes in long. Hence we are just preserving the earlier behavior and logging debug log for the case.
    final int dataByteCount = (int) result.getByteCount();

    if (dataByteCount <= 0) {
      if (logger.isDebugEnabled()) {
        logger.debug("Either no data received in this batch or there is BufferOverflow in dataByteCount: {}",
            dataByteCount);
      }
      listener.success(Acks.OK, null);
      return;
    }

    // If here that means there is some data for sure. Create a ByteBuf with all the data in it.
    final int rows = result.getHeader().getRowCount();
    final BufferAllocator allocator = webSessionResources.getAllocator();
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
            final Accessor accessor = vw.getValueVector().getAccessor();
            final Object value = i < accessor.getValueCount() ? accessor.getObject(i) : null;
            final String display = value == null ? null : value.toString();
            record.put(field, display);
          }
          results.add(record);
        }
      } finally {
        loader.clear();
      }
    } catch (Exception e) {
      exception = UserException.systemError(e).build(logger);
    } finally {
      // Notify the listener with ACK.OK both in error/success case because data was send successfully from Drillbit.
      bufferWithData.release();
      listener.success(Acks.OK, null);
    }
  }

  @Override
  public ChannelFuture getChannelClosureFuture() {
    return webSessionResources.getCloseFuture();
  }

  @Override
  public SocketAddress getRemoteAddress() {
    return webSessionResources.getRemoteAddress();
  }

  @Override
  public void setAutoRead(boolean enableAutoRead) {
    // no-op
  }

  /**
   * For authenticated WebUser no cleanup of {@link WebSessionResources} is done since it's re-used
   * for all the queries until lifetime of the web session.
   */
  public void cleanupSession() {
    // no-op
  }

  public static class AnonWebUserConnection extends WebUserConnection {

    AnonWebUserConnection(WebSessionResources webSessionResources) {
      super(webSessionResources);
    }

    /**
     * For anonymous WebUser after each query request is completed the {@link WebSessionResources} is cleaned up.
     */
    @Override
    public void cleanupSession() {
      webSessionResources.close();
    }
  }
}