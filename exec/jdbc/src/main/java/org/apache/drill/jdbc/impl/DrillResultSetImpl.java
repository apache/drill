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
package org.apache.drill.jdbc.impl;

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

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.QueryResult.QueryState;
import org.apache.drill.exec.proto.UserBitShared.QueryType;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.rpc.user.ConnectionThrottle;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.rpc.user.UserResultsListener;
import org.apache.drill.jdbc.AlreadyClosedSqlException;
import org.apache.drill.jdbc.DrillConnection;
import org.apache.drill.jdbc.DrillConnectionImpl;
import org.apache.drill.jdbc.DrillCursor;
import org.apache.drill.jdbc.DrillResultSet;
import org.apache.drill.jdbc.SchemaChangeListener;

import com.google.common.collect.Queues;


//???? Split this into interface org.apache.drill.jdbc.DrillResultSet for published
// interface and a class probably named org.apache.drill.jdbc.impl.DrillResultSetImpl.
// Add any needed documentation of Drill-specific behavior of JDBC-defined
// ResultSet methods to new DrillResultSet. ...

public class DrillResultSetImpl extends AvaticaResultSet implements DrillResultSet {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillResultSetImpl.class);

  // (Public until JDBC impl. classes moved out of published-intf. package. (DRILL-2089).)
  public SchemaChangeListener changeListener;
  // (Public until JDBC impl. classes moved out of published-intf. package. (DRILL-2089).)
  public final ResultsListener resultslistener = new ResultsListener();
  private volatile QueryId queryId;
  private final DrillClient client;
  // (Public until JDBC impl. classes moved out of published-intf. package. (DRILL-2089).)
  public final RecordBatchLoader currentBatch;
  // (Public until JDBC impl. classes moved out of published-intf. package. (DRILL-2089).)
  public final DrillCursor cursor;

  public DrillResultSetImpl(AvaticaStatement statement, AvaticaPrepareResult prepareResult,
      ResultSetMetaData resultSetMetaData, TimeZone timeZone) {
    super(statement, prepareResult, resultSetMetaData, timeZone);
    DrillConnection c = (DrillConnection) statement.getConnection();
    DrillClient client = c.getClient();
    // DrillClient client, DrillStatement statement) {
    currentBatch = new RecordBatchLoader(client.getAllocator());
    this.client = client;
    cursor = new DrillCursor(this);
  }

  /**
   * Throws AlreadyClosedSqlException if this ResultSet is closed.
   *
   * @throws AlreadyClosedSqlException if ResultSet is closed
   * @throws SQLException if error in calling {@link #isClosed()}
   */
  private void checkNotClosed() throws SQLException {
    if ( isClosed() ) {
      throw new AlreadyClosedSqlException( "ResultSet is already closed." );
    }
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    checkNotClosed();
    return super.getMetaData();
  }


  @Override
  protected void cancel() {
    cleanup();
    close();
  }

  // (Public until JDBC impl. classes moved out of published-intf. package. (DRILL-2089).)
  public synchronized void cleanup() {
    if (queryId != null && ! resultslistener.completed) {
      client.cancelQuery(queryId);
    }
    resultslistener.close();
    currentBatch.clear();
  }

  @Override
  public boolean next() throws SQLException {
    checkNotClosed();
    // Next may be called after close has been called (for example after a user cancel) which in turn
    // sets the cursor to null. So we must check before we call next.
    // TODO: handle next() after close is called in the Avatica code.
    if (super.cursor != null) {
      return super.next();
    } else {
      return false;
    }
  }

  @Override
  protected DrillResultSetImpl execute() throws SQLException{
    checkNotClosed();
    // Call driver's callback. It is permitted to throw a RuntimeException.
    DrillConnectionImpl connection = (DrillConnectionImpl) statement.getConnection();

    connection.getClient().runQuery(QueryType.SQL, this.prepareResult.getSql(),
                                    resultslistener);
    connection.getDriver().handler.onStatementExecute(statement, null);

    super.execute();

    // don't return with metadata until we've achieved at least one return message.
    try {
      resultslistener.latch.await();
      cursor.next();
    } catch (InterruptedException e) {
     // TODO:  Check:  Should this call Thread.currentThread.interrupt()?   If
     // not, at least document why this is empty.
    }

    return this;
  }

  public String getQueryId() {
    if (queryId != null) {
      return QueryIdHelper.getQueryId(queryId);
    } else {
      return null;
    }
  }

  // (Public until JDBC impl. classes moved out of published-intf. package. (DRILL-2089).)
  public class ResultsListener implements UserResultsListener {
    private static final int MAX = 100;
    private volatile UserException ex;
    volatile boolean completed = false;
    private volatile boolean autoread = true;
    private volatile ConnectionThrottle throttle;
    private volatile boolean closed = false;
    private CountDownLatch latch = new CountDownLatch(1);
    private AtomicBoolean receivedMessage = new AtomicBoolean(false);



    final LinkedBlockingDeque<QueryDataBatch> queue = Queues.newLinkedBlockingDeque();

    // TODO:  Doc.:  Release what if what is first relative to what?
    private boolean releaseIfFirst() {
      if (receivedMessage.compareAndSet(false, true)) {
        latch.countDown();
        return true;
      }

      return false;
    }

    @Override
    public void submissionFailed(UserException ex) {
      this.ex = ex;
      completed = true;
      close();
      System.out.println("Query failed: " + ex.getMessage());
    }

    @Override
    public void queryCompleted(QueryState state) {
      releaseIfFirst();
      completed = true;
    }

    @Override
    public void dataArrived(QueryDataBatch result, ConnectionThrottle throttle) {
      logger.debug("Result arrived {}", result);

      // If we're in a closed state, just release the message.
      if (closed) {
        result.release();
        completed = true;
        return;
      }

      // We're active; let's add to the queue.
      queue.add(result);
      if (queue.size() >= MAX - 1) {
        throttle.setAutoRead(false);
        this.throttle = throttle;
        autoread = false;
      }

      releaseIfFirst();
    }

    // TODO:  Doc.:  Specify whether result can be null and what that means.
    public QueryDataBatch getNext() throws Exception {
      while (true) {
        if (ex != null) {
          throw ex;
        }
        if (completed && queue.isEmpty()) {
          return null;
        } else {
          QueryDataBatch q = queue.poll(50, TimeUnit.MILLISECONDS);
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
        QueryDataBatch qrb = queue.poll();
        if (qrb != null && qrb.getData() != null) {
          qrb.getData().release();
        }
      }
      // close may be called before the first result is received and the main thread is blocked waiting
      // for the result. In that case we want to unblock the main thread.
      latch.countDown();
      completed = true;
    }

    @Override
    public void queryIdArrived(QueryId queryId) {
      DrillResultSetImpl.this.queryId = queryId;
    }
  }

}
