/*
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

package org.apache.drill.exec.server.rest;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.DrillBuf;
import io.netty.channel.ChannelFuture;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.physical.impl.materialize.QueryWritableBatch;
import org.apache.drill.exec.proto.GeneralRPCProtos;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.QueryType;
import org.apache.drill.exec.proto.UserProtos;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.rpc.AbstractUserClientConnectionWrapper;
import org.apache.drill.exec.rpc.Acks;
import org.apache.drill.exec.rpc.ConnectionThrottle;
import org.apache.drill.exec.rpc.RpcOutcomeListener;
import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.exec.server.rest.auth.DrillUserPrincipal;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.work.WorkManager;
import org.eclipse.jetty.server.ServerConnector;

import javax.xml.bind.annotation.XmlRootElement;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

@XmlRootElement
public class QueryWrapper {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(QueryWrapper.class);

  private final String query;

  private final String queryType;

  @JsonCreator
  public QueryWrapper(@JsonProperty("query") String query, @JsonProperty("queryType") String queryType) {
    this.query = query;
    this.queryType = queryType;
  }

  public String getQuery() {
    return query;
  }

  public String getQueryType() {
    return queryType;
  }

  public QueryType getType() {
    QueryType type = QueryType.SQL;
    switch (queryType) {
      case "SQL":
        type = QueryType.SQL;
        break;
      case "LOGICAL":
        type = QueryType.LOGICAL;
        break;
      case "PHYSICAL":
        type = QueryType.PHYSICAL;
        break;
    }
    return type;
  }

  public QueryResult run(final WorkManager workManager, final DrillUserPrincipal principal,
                         final ServerConnector webServerConnector) throws Exception {

    final BufferAllocator allocator = workManager.getContext().getAllocator();

    // Create a WebUserConnection wrapper for this query.
    final WebUserConnectionWrapper userConnection = new WebUserConnectionWrapper(allocator,
        principal, webServerConnector);
    final UserProtos.RunQuery runQuery = UserProtos.RunQuery.getDefaultInstance().newBuilderForType()
        .setType(getType())
        .setPlan(getQuery())
        .setResultsMode(UserProtos.QueryResultsMode.STREAM_FULL)
        .build();

    // Submit user query to Drillbit work queue.
    final QueryId queryId = workManager.getUserWorker().submitWork(userConnection, runQuery);

    // Wait until the query execution is complete or there is error submitting the query
    userConnection.await();

    logger.trace("Query {} is completed ", queryId);

    if (userConnection.results.isEmpty()) {
      userConnection.results.add(Maps.<String, String>newHashMap());
    }

    // Return the QueryResult.
    return new QueryResult(userConnection.columns, userConnection.results);
  }

  public static class QueryResult {
    public final Collection<String> columns;

    public final List<Map<String, String>> rows;

    public QueryResult(Collection<String> columns, List<Map<String, String>> rows) {
      this.columns = columns;
      this.rows = rows;
    }
  }

  @Override
  public String toString() {
    return "QueryRequest [queryType=" + queryType + ", query=" + query + "]";
  }

  /**
   * WebUserConnectionWrapper which represents the UserClientConnection for the WebUser submitting the query. It provides
   * access to the UserSession executing the query. There is no actual physical channel corresponding to this connection
   * wrapper.
   */
  public class WebUserConnectionWrapper extends AbstractUserClientConnectionWrapper implements ConnectionThrottle {

    private final BufferAllocator allocator;

    private final DrillUserPrincipal principal;

    private final ServerConnector webServerConnector;

    public final List<Map<String, String>> results = Lists.newArrayList();

    public final Set<String> columns = Sets.newLinkedHashSet();

    WebUserConnectionWrapper(BufferAllocator allocator, DrillUserPrincipal principal,
                             ServerConnector webServerConnector) {
      this.allocator = allocator;
      this.principal = principal;
      this.webServerConnector = webServerConnector;
    }

    @Override
    public UserSession getSession() {
      return principal.getWebUserSession();
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
      } catch (SchemaChangeException e) { // not catching OOM here
        throw new RuntimeException(e);
      } finally {
        bufferWithData.release();
      }

      // Notify the listener with ACK
      listener.success(Acks.OK, null);
    }

    @Override
    public ChannelFuture getChannelClosureFuture() {
      return principal.getSessionCloseFuture();
    }

    @Override
    public SocketAddress getRemoteAddress() {
      SocketAddress addr;
      try {
        addr = ((ServerSocketChannel) webServerConnector.getTransport()).getLocalAddress();
      } catch (Exception e) {
        logger.error("Failed to get local address");
        addr = new InetSocketAddress(webServerConnector.getHost(), webServerConnector.getPort());
      }
      return addr;
    }

    @Override
    public void setAutoRead(boolean enableAutoRead) {
      // no-op
    }
  }
}
