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
package org.apache.drill.exec.rpc.user;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.DrillBuf;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.QueryResult;
import org.apache.drill.exec.proto.UserBitShared.QueryData;
import org.apache.drill.exec.proto.UserBitShared.QueryResult.QueryState;
import org.apache.drill.exec.rpc.BaseRpcOutcomeListener;
import org.apache.drill.exec.rpc.RpcBus;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.RpcOutcomeListener;

import com.google.common.collect.Maps;
import com.google.common.collect.Queues;

/**
 * Encapsulates the future management of query submissions.  This entails a
 * potential race condition.  Normal ordering is:
 * <ul>
 *   <li>1.  Submit query to be executed. </li>
 *   <li>2.  Receive QueryHandle for buffer management. </li>
 *   <li>3.  Start receiving results batches for query. </li>
 * </ul>
 * However, 3 could potentially occur before 2.   Because of that, we need to
 * handle this case and then do a switcheroo.
 */
public class QueryResultHandler {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(QueryResultHandler.class);

  /**
   * Current listener for results, for each active query.
   * <p>
   *   Concurrency:  Access by SubmissionLister for query-ID message vs.
   *   access by batchArrived is not otherwise synchronized.
   * </p>
   */
  private final ConcurrentMap<QueryId, UserResultsListener> queryIdToResultsListenersMap =
      Maps.newConcurrentMap();

  public RpcOutcomeListener<QueryId> getWrappedListener(UserResultsListener resultsListener) {
    return new SubmissionListener(resultsListener);
  }

  /**
   * Maps internal low-level API protocol to {@link UserResultsListener}-level API protocol.
   * handles data result messages
   */
  public void resultArrived( ByteBuf pBody ) throws RpcException {
    final QueryResult queryResult = RpcBus.get( pBody, QueryResult.PARSER );

    final QueryId queryId = queryResult.getQueryId();
    final QueryState queryState = queryResult.getQueryState();

    logger.debug( "resultArrived: queryState: {}, queryId = {}", queryState, queryId );

    assert queryResult.hasQueryState() : "received query result without QueryState";

    final boolean isFailureResult    = QueryState.FAILED    == queryState;
    // CANCELED queries are handled the same way as COMPLETED
    final boolean isTerminalResult;
    switch ( queryState ) {
      case PENDING:
        isTerminalResult = false;
        break;
      case FAILED:
      case CANCELED:
      case COMPLETED:
        isTerminalResult = true;
        break;
      default:
        logger.error( "Unexpected/unhandled QueryState " + queryState
          + " (for query " + queryId +  ")" );
        isTerminalResult = false;
        break;
    }

    assert isFailureResult || queryResult.getErrorCount() == 0
      : "Error count for the query batch is non-zero but QueryState != FAILED";

    UserResultsListener resultsListener = newUserResultsListener(queryId);

    try {
      if (isFailureResult) {
        // Failure case--pass on via submissionFailed(...).

        String message = buildErrorMessage(queryResult);
        resultsListener.submissionFailed(new RpcException(message));
        // Note: Listener is removed in finally below.
      } else if (isTerminalResult) {
        // A successful completion/canceled case--pass on via resultArrived

        try {
          resultsListener.queryCompleted();
        } catch ( Exception e ) {
          resultsListener.submissionFailed(new RpcException(e));
        }
      } else {
        logger.warn("queryState {} was ignored", queryState);
      }
    } finally {
      if ( isTerminalResult ) {
        // TODO:  What exactly are we checking for?  How should we really check
        // for it?
        if ( (! ( resultsListener instanceof BufferingResultsListener )
          || ((BufferingResultsListener) resultsListener).output != null ) ) {
          queryIdToResultsListenersMap.remove( queryId, resultsListener );
        }
      }
    }
  }

  /**
   * Maps internal low-level API protocol to {@link UserResultsListener}-level API protocol.
   * handles query data messages
   */
  public void batchArrived( ConnectionThrottle throttle,
                            ByteBuf pBody, ByteBuf dBody ) throws RpcException {
    final QueryData queryData = RpcBus.get( pBody, QueryData.PARSER );
    // Current batch coming in.
    final QueryDataBatch batch = new QueryDataBatch( queryData, (DrillBuf) dBody );

    final QueryId queryId = queryData.getQueryId();

    logger.debug( "batchArrived: queryId = {}", queryId );
    logger.trace( "batchArrived: batch = {}", batch );

    UserResultsListener resultsListener = newUserResultsListener(queryId);

    // A data case--pass on via dataArrived

    try {
      resultsListener.dataArrived(batch, throttle);
      // That releases batch if successful.
    } catch ( Exception e ) {
      batch.release();
      resultsListener.submissionFailed(new RpcException(e));
    }
  }

  /**
   * Return {@link UserResultsListener} associated with queryId. Will create a new {@link BufferingResultsListener}
   * if no listener found.
   * @param queryId queryId we are getting the listener for
   * @return {@link UserResultsListener} associated with queryId
   */
  private UserResultsListener newUserResultsListener(QueryId queryId) {
    UserResultsListener resultsListener = queryIdToResultsListenersMap.get( queryId );
    logger.trace( "For QueryId [{}], retrieved results listener {}", queryId, resultsListener );
    if ( null == resultsListener ) {
      // WHO?? didn't get query ID response and set submission listener yet,
      // so install a buffering listener for now

      BufferingResultsListener bl = new BufferingResultsListener();
      resultsListener = queryIdToResultsListenersMap.putIfAbsent( queryId, bl );
      // If we had a successful insertion, use that reference.  Otherwise, just
      // throw away the new buffering listener.
      if ( null == resultsListener ) {
        resultsListener = bl;
      }
      // TODO:  Is there a more direct way to detect a Query ID in whatever state this string comparison detects?
      if ( queryId.toString().isEmpty() ) {
        failAll();
      }
    }
    return resultsListener;
  }

  protected String buildErrorMessage(QueryResult result) {
    StringBuilder sb = new StringBuilder();
    for (UserBitShared.DrillPBError error : result.getErrorList()) {
      sb.append(error.getMessage());
      sb.append("\n");
    }
    return sb.toString();
  }

  private void failAll() {
    for (UserResultsListener l : queryIdToResultsListenersMap.values()) {
      l.submissionFailed(new RpcException("Received result without QueryId"));
    }
  }

  private static class BufferingResultsListener implements UserResultsListener {

    private ConcurrentLinkedQueue<QueryDataBatch> results = Queues.newConcurrentLinkedQueue();
    private volatile boolean finished = false;
    private volatile RpcException ex;
    private volatile UserResultsListener output;
    private volatile ConnectionThrottle throttle;

    public boolean transferTo(UserResultsListener l) {
      synchronized (this) {
        output = l;
        for (QueryDataBatch r : results) {
          l.dataArrived(r, throttle);
        }
        if (ex != null) {
          l.submissionFailed(ex);
          return true;
        } else if (finished) {
          l.queryCompleted();
        }

        return finished;
      }
    }

    @Override
    public void queryCompleted() {
      finished = true;
      synchronized (this) {
        if (output != null) {
          output.queryCompleted();
        }
      }
    }

    @Override
    public void dataArrived(QueryDataBatch result, ConnectionThrottle throttle) {
      this.throttle = throttle;

      synchronized (this) {
        if (output == null) {
          this.results.add(result);
        } else {
          output.dataArrived(result, throttle);
        }
      }
    }

    @Override
    public void submissionFailed(RpcException ex) {
      finished = true;
      synchronized (this) {
        if (output == null) {
          this.ex = ex;
        } else{
          output.submissionFailed(ex);
        }
      }
    }

    public boolean isFinished() {
      return finished;
    }

    @Override
    public void queryIdArrived(QueryId queryId) {
    }

  }

  private class SubmissionListener extends BaseRpcOutcomeListener<QueryId> {
    private UserResultsListener resultsListener;

    public SubmissionListener(UserResultsListener resultsListener) {
      super();
      this.resultsListener = resultsListener;
    }

    @Override
    public void failed(RpcException ex) {
      resultsListener.submissionFailed(ex);
    }

    @Override
    public void success(QueryId queryId, ByteBuf buf) {
      resultsListener.queryIdArrived(queryId);
      logger.debug("Received QueryId {} successfully.  Adding results listener {}.",
                   queryId, resultsListener);
      UserResultsListener oldListener =
          queryIdToResultsListenersMap.putIfAbsent(queryId, resultsListener);

      // We need to deal with the situation where we already received results by
      // the time we got the query id back.  In that case, we'll need to
      // transfer the buffering listener over, grabbing a lock against reception
      // of additional results during the transition.
      if (oldListener != null) {
        logger.debug("Unable to place user results listener, buffering listener was already in place.");
        if (oldListener instanceof BufferingResultsListener) {
          boolean all = ((BufferingResultsListener) oldListener).transferTo(this.resultsListener);
          // simply remove the buffering listener if we already have the last response.
          if (all) {
            queryIdToResultsListenersMap.remove(queryId);
          } else {
            boolean replaced = queryIdToResultsListenersMap.replace(queryId, oldListener, resultsListener);
            if (!replaced) {
              throw new IllegalStateException(); // TODO: Say what the problem is!
            }
          }
        } else {
          throw new IllegalStateException("Trying to replace a non-buffering User Results listener.");
        }
      }

    }

  }

}
