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
import org.apache.drill.jdbc.ExecutionCanceledSqlException;
import org.apache.drill.jdbc.SchemaChangeListener;

import static org.slf4j.LoggerFactory.getLogger;
import org.slf4j.Logger;

import com.google.common.collect.Queues;


public class DrillResultSetImpl extends AvaticaResultSet implements DrillResultSet {
  @SuppressWarnings("unused")
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillResultSetImpl.class);

  // (Public until JDBC impl. classes moved out of published-intf. package. (DRILL-2089).)
  public SchemaChangeListener changeListener;
  // (Public until JDBC impl. classes moved out of published-intf. package. (DRILL-2089).)
  public final ResultsListener resultsListener = new ResultsListener();
  private final DrillClient client;
  // (Public until JDBC impl. classes moved out of published-intf. package. (DRILL-2089).)
  // TODO:  Resolve:  Since is barely manipulated here in DrillResultSetImpl,
  //  move down into DrillCursor and have this.clean() have cursor clean it.
  public final RecordBatchLoader currentBatch;
  // (Public until JDBC impl. classes moved out of published-intf. package. (DRILL-2089).)
  public final DrillCursor cursor;
  public boolean hasPendingCancelationNotification;

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
   * Throws AlreadyClosedSqlException or QueryCanceledSqlException if this
   * ResultSet is closed.
   *
   * @throws  ExecutionCanceledSqlException  if ResultSet is closed because of
   *          cancelation and no QueryCanceledSqlException had been thrown yet
   *          for this ResultSet
   * @throws  AlreadyClosedSqlException  if ResultSet is closed
   * @throws SQLException if error in calling {@link #isClosed()}
   */
  private void checkNotClosed() throws SQLException {
    if ( isClosed() ) {
      if ( hasPendingCancelationNotification ) {
        hasPendingCancelationNotification = false;
        throw new ExecutionCanceledSqlException(
            "SQL statement execution canceled; resultSet closed." );
      }
      else {
        throw new AlreadyClosedSqlException( "ResultSet is already closed." );
      }
    }
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    checkNotClosed();
    return super.getMetaData();
  }


  @Override
  protected void cancel() {
    hasPendingCancelationNotification = true;
    cleanup();
    close();
  }

  // (Public until JDBC impl. classes moved out of published-intf. package. (DRILL-2089).)
  public synchronized void cleanup() {
    if (resultsListener.getQueryId() != null && ! resultsListener.completed) {
      client.cancelQuery(resultsListener.getQueryId());
    }
    resultsListener.close();
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
    DrillConnectionImpl connection = (DrillConnectionImpl) statement.getConnection();

    connection.getClient().runQuery(QueryType.SQL, this.prepareResult.getSql(),
                                    resultsListener);
    connection.getDriver().handler.onStatementExecute(statement, null);

    super.execute();

    // don't return with metadata until we've achieved at least one return message.
    try {
      // TODO:  Revisit:  Why reaching directly into ResultsListener rather than
      // calling some wait method?
      resultsListener.latch.await();
    } catch ( InterruptedException e ) {
      // Not normally expected--Drill doesn't interrupt in this area (right?)--
      // but JDBC client certainly could.
      throw new SQLException( "Interrupted", e );
    }
    cursor.next();

    return this;
  }

  public String getQueryId() {
    if (resultsListener.queryId != null) {
      return QueryIdHelper.getQueryId(resultsListener.queryId);
    } else {
      return null;
    }
  }

  // (Public until JDBC impl. classes moved out of published-intf. package. (DRILL-2089).)
  public static class ResultsListener implements UserResultsListener {
    private static Logger logger = getLogger( ResultsListener.class );

    private static final int THROTTLING_QUEUE_SIZE_THRESHOLD = 100;

    private volatile QueryId queryId;

    private volatile UserException executionFailureException;
    volatile boolean completed = false;
    private volatile boolean autoread = true;
    private volatile ConnectionThrottle throttle;
    private volatile boolean closed = false;
    // TODO:  Rename.  It's obvious it's a latch--but what condition or action
    // does it represent or control?
    private CountDownLatch latch = new CountDownLatch(1);
    private AtomicBoolean receivedMessage = new AtomicBoolean(false);

    final LinkedBlockingDeque<QueryDataBatch> batchQueue =
        Queues.newLinkedBlockingDeque();


    ResultsListener() {
      logger.debug( "Query listener created." );
    }

    // TODO:  Doc.:  Release what if what is first relative to what?
    private boolean releaseIfFirst() {
      if (receivedMessage.compareAndSet(false, true)) {
        latch.countDown();
        return true;
      }

      return false;
    }

    @Override
    public void queryIdArrived(QueryId queryId) {
      logger.debug( "Received query ID: {}.", queryId );
      this.queryId = queryId;
    }

    @Override
    public void submissionFailed(UserException ex) {
      logger.debug( "Received query failure.", ex );
      this.executionFailureException = ex;
      completed = true;
      close();
      logger.info( "Query failed: ", ex );
    }

    @Override
    public void dataArrived(QueryDataBatch result, ConnectionThrottle throttle) {
      logger.debug( "Received query data: {}.", result );

      // If we're in a closed state, just release the message.
      if (closed) {
        result.release();
        completed = true;
        return;
      }

      // We're active; let's add to the queue.
      batchQueue.add(result);
      if (batchQueue.size() >= THROTTLING_QUEUE_SIZE_THRESHOLD - 1) {
        throttle.setAutoRead(false);
        this.throttle = throttle;
        autoread = false;
      }

      releaseIfFirst();
    }

    @Override
    public void queryCompleted(QueryState state) {
      logger.debug( "Query completion arrived: {}.", state );
      releaseIfFirst();
      completed = true;
    }

    public QueryId getQueryId() {
      return queryId;
    }


    /**
     * Gets the next batch of query results from the queue.
     * @return  the next batch, or {@code null} after last batch has been returned
     * @throws UserException
     *         if the query failed
     * @throws InterruptedException
     *         if waiting on the queue was interrupted
     */
    public QueryDataBatch getNext() throws UserException,
                                           InterruptedException {
      while (true) {
        if (executionFailureException != null) {
          logger.debug( "Dequeued query failure exception: {}.", executionFailureException );
          throw executionFailureException;
        }
        if (completed && batchQueue.isEmpty()) {
          return null;
        } else {
          QueryDataBatch q = batchQueue.poll(50, TimeUnit.MILLISECONDS);
          if (q != null) {
            if (!autoread && batchQueue.size() < THROTTLING_QUEUE_SIZE_THRESHOLD / 2) {
              autoread = true;
              throttle.setAutoRead(true);
              throttle = null;
            }
            logger.debug( "Dequeued query data: {}.", q );
            return q;
          }
        }
      }
    }

    void close() {
      closed = true;
      while (!batchQueue.isEmpty()) {
        QueryDataBatch qrb = batchQueue.poll();
        if (qrb != null && qrb.getData() != null) {
          qrb.getData().release();
        }
      }
      // close may be called before the first result is received and the main thread is blocked waiting
      // for the result. In that case we want to unblock the main thread.
      latch.countDown(); // TODO:  Why not call releaseIfFirst as used elsewhere?
      completed = true;
    }

  }

}
