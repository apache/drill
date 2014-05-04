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
package org.apache.drill.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.TimeZone;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import net.hydromatic.avatica.AvaticaPrepareResult;
import net.hydromatic.avatica.AvaticaResultSet;
import net.hydromatic.avatica.AvaticaStatement;

import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserProtos.QueryType;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.user.ConnectionThrottle;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.rpc.user.UserResultsListener;

import com.google.common.collect.Queues;

public class DrillResultSet extends AvaticaResultSet {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillResultSet.class);

  SchemaChangeListener changeListener;
  final Listener listener = new Listener();
  private volatile QueryId queryId;
  private final DrillClient client;
  final RecordBatchLoader currentBatch;
  final DrillCursor cursor; 
  
  public DrillResultSet(AvaticaStatement statement, AvaticaPrepareResult prepareResult,
      ResultSetMetaData resultSetMetaData, TimeZone timeZone) {
    super(statement, prepareResult, resultSetMetaData, timeZone);
    DrillConnection c = (DrillConnection) statement.getConnection();
    DrillClient client = c.getClient();
    // DrillClient client, DrillStatement statement) {
    currentBatch = new RecordBatchLoader(client.getAllocator());
    this.client = client;
    cursor = new DrillCursor(this);
  }
  
  @Override
  protected void cancel() {
    cleanup();
    close();
  }

  synchronized void cleanup(){
    if (queryId != null && !listener.completed){
      client.cancelQuery(queryId);
    }
    listener.close();
  }
  
  @Override protected DrillResultSet execute() throws SQLException{
    // Call driver's callback. It is permitted to throw a RuntimeException.
    DrillConnectionImpl connection = (DrillConnectionImpl) statement.getConnection();

    connection.getClient().runQuery(QueryType.SQL, this.prepareResult.getSql(), listener);
    connection.getDriver().handler.onStatementExecute(statement, null);

    super.execute();

    // don't return with metadata until we've achieved at least one return message.
    try {
      listener.latch.await();
      cursor.next();
    } catch (InterruptedException e) {
    }
    
    return this;
  }
  
  class Listener implements UserResultsListener {
    private static final int MAX = 100;
    private volatile RpcException ex;
    volatile boolean completed = false;
    private volatile boolean autoread = true;
    private volatile ConnectionThrottle throttle;
    private volatile boolean closed = false;
    private CountDownLatch latch = new CountDownLatch(1);
    private AtomicBoolean receivedMessage = new AtomicBoolean(false);
    
    
    
    final LinkedBlockingDeque<QueryResultBatch> queue = Queues.newLinkedBlockingDeque();

    private boolean releaseIfFirst(){
      if(receivedMessage.compareAndSet(false, true)){
        latch.countDown();
        return true;
      }
      
      return false;
    }
    
    @Override
    public void submissionFailed(RpcException ex) {
      releaseIfFirst();
      this.ex = ex;
      completed = true;
      close();
      System.out.println("Query failed: " + ex);
    }

    @Override
    public void resultArrived(QueryResultBatch result, ConnectionThrottle throttle) {
      logger.debug("Result arrived {}", result);

      // if we're in a closed state, just release the message.
      if (closed) {
        result.release();
        completed = true;
        return;
      }

      // we're active, let's add to the queue.
      queue.add(result);
      if (queue.size() >= MAX - 1) {
        throttle.setAutoRead(false);
        this.throttle = throttle;
        autoread = false;
      }

      if (result.getHeader().getIsLastChunk()) {
        completed = true;
      }

      if (result.getHeader().getErrorCount() > 0) {
        submissionFailed(new RpcException(String.format("%s", result.getHeader().getErrorList())));
      }
      
      releaseIfFirst();

    }

    public QueryResultBatch getNext() throws RpcException, InterruptedException {
      while (true) {
        if (ex != null)
          throw ex;
        if (completed && queue.isEmpty()) {
          return null;
        } else {
          QueryResultBatch q = queue.poll(50, TimeUnit.MILLISECONDS);
          if (q != null) {
            if (!autoread && queue.size() < MAX / 2) {
              autoread = true;
              throttle.setAutoRead(true);
              throttle = null;
            }
            return q;
          }

        }

      }
    }

    void close() {
      closed = true;
      while (!queue.isEmpty()) {
        QueryResultBatch qrb = queue.poll();
        if(qrb != null && qrb.getData() != null) qrb.getData().release();
      }
      completed = true;
    }

    @Override
    public void queryIdArrived(QueryId queryId) {
      DrillResultSet.this.queryId = queryId;
    }
  }

}
