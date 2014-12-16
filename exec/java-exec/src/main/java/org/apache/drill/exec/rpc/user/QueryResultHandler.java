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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.Nullable;

import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.QueryResult;
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

  /**
   * Any is-last-chunk batch being deferred until the next batch
   * (normally one with COMPLETED) arrives, per active query.
   * <ul>
   *   <li>Last-chunk batch is added (and not passed on) when it arrives.</li>
   *   <li>Last-chunk batch is removed (and passed on) when next batch arrives
   *       and has state {@link QueryState.COMPLETED}.</li>
   *   <li>Last-chunk batch is removed (and not passed on) when next batch
   *       arrives and has state {@link QueryState.CANCELED} or
   *       {@link QueryState.FAILED}.</li>
   * </ul>
   */
  private final Map<QueryId, QueryResultBatch> queryIdToDeferredLastChunkBatchesMap =
      new ConcurrentHashMap<>();


  public RpcOutcomeListener<QueryId> getWrappedListener(UserResultsListener resultsListener) {
    return new SubmissionListener(resultsListener);
  }

  /**
   * Maps internal low-level API protocol to {@link UserResultsListener}-level
   * API protocol, deferring sending is-last-chunk batches until (internal)
   * COMPLETED batch.
   */
  public void batchArrived( ConnectionThrottle throttle,
                            ByteBuf pBody, ByteBuf dBody ) throws RpcException {
    final QueryResult queryResult = RpcBus.get( pBody, QueryResult.PARSER );
    // Current batch coming in.  (Not necessarily passed along now or ever.)
    final QueryResultBatch inputBatch = new QueryResultBatch( queryResult,
                                                              (DrillBuf) dBody );

    final QueryId queryId = queryResult.getQueryId();
    final QueryState queryState = inputBatch.getHeader().getQueryState();

    logger.debug( "batchArrived: isLastChunk: {}, queryState: {}, queryId = {}",
                  inputBatch.getHeader().getIsLastChunk(), queryState, queryId );
    logger.trace( "batchArrived: currentBatch = {}", inputBatch );

    final boolean isFailureBatch    = QueryState.FAILED    == queryState;
    final boolean isCompletionBatch = QueryState.COMPLETED == queryState;
    final boolean isLastChunkBatchToDelay =
        inputBatch.getHeader().getIsLastChunk() && QueryState.PENDING == queryState;
    final boolean isTerminalBatch;
    switch ( queryState ) {
      case PENDING:
         isTerminalBatch = false;
         break;
      case FAILED:
      case CANCELED:
      case COMPLETED:
        isTerminalBatch = true;
        break;
      default:
        logger.error( "Unexpected/unhandled QueryState " + queryState
                      + " (for query " + queryId +  ")" );
        isTerminalBatch = false;
        break;
    }
    assert isFailureBatch || inputBatch.getHeader().getErrorCount() == 0
        : "Error count for the query batch is non-zero but QueryState != FAILED";

    UserResultsListener resultsListener = queryIdToResultsListenersMap.get( queryId );
    logger.trace( "For QueryId [{}], retrieved results listener {}", queryId,
                  resultsListener );
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
      // TODO:  Is there a more direct way to detect a Query ID in whatever
      // state this string comparison detects?
      if ( queryId.toString().equals( "" ) ) {
        failAll();
      }
    }

    try {
      if (isFailureBatch) {
        // Failure case--pass on via submissionFailed(...).

        try {
          String message = buildErrorMessage(inputBatch);
          resultsListener.submissionFailed(new RpcException(message));
        }
        finally {
          inputBatch.release();
        }
        // Note: Listener and any delayed batch are removed in finally below.
      } else {
        // A successful (data, completion, or cancelation) case--pass on via
        // resultArrived, delaying any last-chunk batches until following
        // COMPLETED batch and omitting COMPLETED batch.

        // If is last-chunk batch, save until next batch for query (normally a
        // COMPLETED batch) comes in:
        if ( isLastChunkBatchToDelay ) {
          // We have a (non-failure) is-last-chunk batch--defer it until we get
          // the query's COMPLETED batch.

          QueryResultBatch expectNone;
          assert null == ( expectNone =
                           queryIdToDeferredLastChunkBatchesMap.get( queryId ) )
              : "Already have pending last-batch QueryResultBatch " + expectNone
                + " (at receiving last-batch QueryResultBatch " + inputBatch
                + ") for query " + queryId;
          queryIdToDeferredLastChunkBatchesMap.put( queryId, inputBatch );
          // Can't release batch now; will release at terminal batch in
          // finally below.
        } else {
          // We have a batch triggering sending out a batch (maybe same one,
          // maybe deferred one.

          // Batch to send out in response to current batch.
          final QueryResultBatch outputBatch;
          if ( isCompletionBatch ) {
            // We have a COMPLETED batch--we should have a saved is-last-chunk
            // batch, and we must pass that on now (that we've seen COMPLETED).

            outputBatch = queryIdToDeferredLastChunkBatchesMap.get( queryId );
            assert null != outputBatch
                : "No pending last-batch QueryResultsBatch saved, at COMPLETED"
                + " QueryResultsBatch " + inputBatch + " for query " + queryId;
          } else {
            // We have a non--last-chunk PENDING batch or a CANCELED
            // batch--pass it on.
            outputBatch = inputBatch;
          }
          // Note to release input batch if it's not the batch we're sending out.
          final boolean releaseInputBatch = outputBatch != inputBatch;

          try {
            resultsListener.resultArrived( outputBatch, throttle );
            // That releases outputBatch if successful.
          } catch ( Exception e ) {
            outputBatch.release();
            resultsListener.submissionFailed(new RpcException(e));
          }
          finally {
            if ( releaseInputBatch ) {
              inputBatch.release();
            }
          }
        }
      }
    } finally {
      if ( isTerminalBatch ) {
        // Remove and release any deferred is-last-chunk batch:
        QueryResultBatch anyUnsentLastChunkBatch =
             queryIdToDeferredLastChunkBatchesMap.remove( queryId );
        if ( null != anyUnsentLastChunkBatch ) {
          anyUnsentLastChunkBatch.release();
        }

       // TODO:  What exactly are we checking for?  How should we really check
        // for it?
        if ( (! ( resultsListener instanceof BufferingResultsListener )
             || ((BufferingResultsListener) resultsListener).output != null ) ) {
          queryIdToResultsListenersMap.remove( queryId, resultsListener );
        }
      }
    }
  }

  protected String buildErrorMessage(QueryResultBatch batch) {
    StringBuilder sb = new StringBuilder();
    for (UserBitShared.DrillPBError error : batch.getHeader().getErrorList()) {
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

    private ConcurrentLinkedQueue<QueryResultBatch> results = Queues.newConcurrentLinkedQueue();
    private volatile boolean finished = false;
    private volatile RpcException ex;
    private volatile UserResultsListener output;
    private volatile ConnectionThrottle throttle;

    public boolean transferTo(UserResultsListener l) {
      synchronized (this) {
        output = l;
        boolean last = false;
        for (QueryResultBatch r : results) {
          l.resultArrived(r, throttle);
          last = r.getHeader().getIsLastChunk();
        }
        if (ex != null) {
          l.submissionFailed(ex);
          return true;
        }
        return last;
      }
    }

    @Override
    public void resultArrived(QueryResultBatch result, ConnectionThrottle throttle) {
      this.throttle = throttle;
      if (result.getHeader().getIsLastChunk()) {
        finished = true;
      }

      synchronized (this) {
        if (output == null) {
          this.results.add(result);
        } else {
          output.resultArrived(result, throttle);
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
          queryIdToResultsListenersMap.remove(oldListener);
          boolean all = ((BufferingResultsListener) oldListener).transferTo(this.resultsListener);
          // simply remove the buffering listener if we already have the last response.
          if (all) {
            queryIdToResultsListenersMap.remove(oldListener);
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
