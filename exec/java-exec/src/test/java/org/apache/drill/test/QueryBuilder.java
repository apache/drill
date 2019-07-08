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
package org.apache.drill.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.drill.PlanTestBase;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.exceptions.UserRemoteException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.client.LoggingResultsListener;
import org.apache.drill.exec.client.QuerySubmitter.Format;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.QueryResult.QueryState;
import org.apache.drill.exec.proto.UserBitShared.QueryType;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.rpc.ConnectionThrottle;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.user.AwaitableUserResultsListener;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.rpc.user.UserResultsListener;
import org.apache.drill.exec.util.VectorUtil;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.ScalarReader;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.test.BufferingQueryEventListener.QueryEvent;
import org.apache.drill.test.ClientFixture.StatementParser;
import org.apache.drill.test.rowSet.DirectRowSet;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSetReader;
import org.joda.time.Period;

/**
 * Builder for a Drill query. Provides all types of query formats,
 * and a variety of ways to run the query.
 */

public class QueryBuilder {

  /**
   * Listener used to retrieve the query summary (only) asynchronously
   * using a {@link QuerySummaryFuture}.
   */

  public class SummaryOnlyQueryEventListener implements UserResultsListener {

    /**
     * The future to be notified. Created here and returned by the
     * query builder.
     */

    private final QuerySummaryFuture future;
    private QueryId queryId;
    private int recordCount;
    private int batchCount;
    private long startTime;

    public SummaryOnlyQueryEventListener(QuerySummaryFuture future) {
      this.future = future;
      startTime = System.currentTimeMillis();
    }

    @Override
    public void queryIdArrived(QueryId queryId) {
      this.queryId = queryId;
    }

    @Override
    public void submissionFailed(UserException ex) {
      future.completed(
          new QuerySummary(queryId, recordCount, batchCount,
                           System.currentTimeMillis() - startTime, ex));
    }

    @Override
    public void dataArrived(QueryDataBatch result, ConnectionThrottle throttle) {
      batchCount++;
      recordCount += result.getHeader().getRowCount();
      result.release();
    }

    @Override
    public void queryCompleted(QueryState state) {
      future.completed(
          new QuerySummary(queryId, recordCount, batchCount,
                           System.currentTimeMillis() - startTime, state));
    }
  }

  /**
   * The future used to wait for the completion of an async query. Returns
   * just the summary of the query.
   */

  public class QuerySummaryFuture implements Future<QuerySummary> {

    /**
     * Synchronizes the listener thread and the test thread that
     * launched the query.
     */

    private CountDownLatch lock = new CountDownLatch(1);
    private QuerySummary summary;

    /**
     * Unsupported at present.
     */

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      throw new UnsupportedOperationException();
    }

    /**
     * Always returns false.
     */

    @Override
    public boolean isCancelled() { return false; }

    @Override
    public boolean isDone() { return summary != null; }

    @Override
    public QuerySummary get() throws InterruptedException, ExecutionException {
      lock.await();
      return summary;
    }

    /**
     * Not supported at present, just does a non-timeout get.
     */

    @Override
    public QuerySummary get(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      return get();
    }

    protected void completed(QuerySummary querySummary) {
      summary = querySummary;
      lock.countDown();
    }
  }

  /**
   * Summary results of a query: records, batches, run time.
   */

  public static class QuerySummary {
    private final QueryId queryId;
    private final int records;
    private final int batches;
    private final long ms;
    private final QueryState finalState;
    private final Exception error;

    public QuerySummary(QueryId queryId, int recordCount, int batchCount, long elapsed, QueryState state) {
      this.queryId = queryId;
      records = recordCount;
      batches = batchCount;
      ms = elapsed;
      finalState = state;
      error = null;
    }

    public QuerySummary(QueryId queryId, int recordCount, int batchCount, long elapsed, Exception ex) {
      this.queryId = queryId;
      records = recordCount;
      batches = batchCount;
      ms = elapsed;
      finalState = null;
      error = ex;
    }

    public boolean failed() { return error != null; }
    public boolean succeeded() { return error == null; }
    public long recordCount() { return records; }
    public int batchCount() { return batches; }
    public long runTimeMs() { return ms; }
    public QueryId queryId() { return queryId; }
    public String queryIdString() { return QueryIdHelper.getQueryId(queryId); }
    public Exception error() { return error; }
    public QueryState finalState() { return finalState; }
  }

  /**
   * Scalar reader function interface for a set of reader methods
   * @param <T> - reader returned type
   */
  private interface SingletonScalarReader<T> {
    T read(ScalarReader reader);
  }

  /**
   * VectorQueryReader function interface
   * @param <V> - vector class
   * @param <T> - result type
   */
  public interface VectorQueryReader<T, V> {
    T read(int recordsCount, V vector);
  }

  private final ClientFixture client;
  private QueryType queryType;
  private String queryText;
  private List<PlanFragment> planFragments;

  QueryBuilder(ClientFixture client) {
    this.client = client;
  }

  public QueryBuilder query(QueryType type, String text) {
    queryType = type;
    queryText = text;
    return this;
  }

  public QueryBuilder sql(String sql) {
    return query(QueryType.SQL, sql);
  }

  public QueryBuilder sql(String query, Object... args) {
    return sql(String.format(query, args));
  }

  /**
   * Run a physical plan presented as a list of fragments.
   *
   * @param planFragments fragments that make up the plan
   * @return this builder
   */

  public QueryBuilder plan(List<PlanFragment> planFragments) {
    queryType = QueryType.EXECUTION;
    this.planFragments = planFragments;
    return this;
  }

  /**
   * Parse a single SQL statement (with optional ending semi-colon) from
   * the file provided.
   * @param file the file containing exactly one SQL statement, with
   * optional ending semi-colon
   * @return this builder
   */

  public QueryBuilder sql(File file) throws FileNotFoundException, IOException {
    try (BufferedReader in = new BufferedReader(new FileReader(file))) {
      StatementParser parser = new StatementParser(in);
      String sql = parser.parseNext();
      if (sql == null) {
        throw new IllegalArgumentException("No query found");
      }
      return sql(sql);
    }
  }

  public QueryBuilder physical(String plan) {
    return query(QueryType.PHYSICAL, plan);
  }

  /**
   * Run a query contained in a resource file.
   *
   * @param resource Name of the resource
   * @return this builder
   */

  public QueryBuilder sqlResource(String resource) {
    sql(ClusterFixture.loadResource(resource));
    return this;
  }

  public QueryBuilder sqlResource(String resource, Object... args) {
    sql(ClusterFixture.loadResource(resource), args);
    return this;
  }

  public QueryBuilder physicalResource(String resource) {
    physical(ClusterFixture.loadResource(resource));
    return this;
  }

  /**
   * Run the query returning just a summary of the results: record count,
   * batch count and run time. Handy when doing performance tests when the
   * validity of the results is verified in some other test.
   *
   * @return the query summary
   * @throws Exception if anything goes wrong anywhere in the execution
   */

  public QuerySummary run() throws Exception {
    return produceSummary(withEventListener());
  }

  /**
   * Run the query and return a list of the result batches. Use
   * if the batch count is small and you want to work with them.
   * @return a list of batches resulting from the query
   * @throws RpcException
   */

  public List<QueryDataBatch> results() throws RpcException {
    Preconditions.checkNotNull(queryType, "Query not provided.");
    Preconditions.checkNotNull(queryText, "Query not provided.");
    return client.client().runQuery(queryType, queryText);
  }

  /**
   * Run the query and return the first non-empty batch as a
   * {@link DirectRowSet} object that can be inspected directly
   * by the code using a {@link RowSetReader}.
   * <p>
   *
   * @see {@link #rowSetIterator()} for a version that reads a series of
   * batches as row sets.
   * @return a row set that represents the first non-empty batch returned from
   * the query
   * @throws RpcException if anything goes wrong
   */

  public DirectRowSet rowSet() throws RpcException {

    // Ignore all but the first non-empty batch.
    // Always return the last batch, which may be empty.

    QueryDataBatch resultBatch = null;
    for (QueryDataBatch batch : results()) {
      if (resultBatch == null) {
        resultBatch = batch;
      } else if (resultBatch.getHeader().getRowCount() == 0) {
        resultBatch.release();
        resultBatch = batch;
      } else if (batch.getHeader().getRowCount() > 0) {
        throw new IllegalStateException("rowSet() returns a single batch, but this query returned multiple batches. Consider rowSetIterator() instead.");
      } else {
        batch.release();
      }
    }

    // No results?

    if (resultBatch == null) {
      return null;
    }

    // Unload the batch and convert to a row set.

    final RecordBatchLoader loader = new RecordBatchLoader(client.allocator());
    try {
      loader.load(resultBatch.getHeader().getDef(), resultBatch.getData());
      resultBatch.release();
      VectorContainer container = loader.getContainer();
      container.setRecordCount(loader.getRecordCount());

      // Null results? Drill will return a single batch with no rows
      // and no columns even if the scan (or other) operator returns
      // no batches at all. For ease of testing, simply map this null
      // result set to a null output row set that says "nothing at all
      // was returned." Note that this is different than an empty result
      // set which has a schema, but no rows.

      if (container.getRecordCount() == 0 && container.getNumberOfColumns() == 0) {
        container.clear();
        return null;
      }

      return DirectRowSet.fromContainer(container);
    } catch (SchemaChangeException e) {
      throw new IllegalStateException(e);
    }
  }

  public QueryRowSetIterator rowSetIterator() {
    return new QueryRowSetIterator(client.allocator(), withEventListener());
  }

  /**
   * Run the query which expect to return vector {@code V} representation
   * of type {@code T} for the column {@code columnName}.
   * <p>
   * <pre>
   * Example:
   *
   *  Set<String> results = queryBuilder()
   *      .sql(query)
   *      .vectorValue(
   *        "columnName",
   *        SomeVector.class,
   *        (resultRecordCount, vector) -> {
   *          Set<String> r = new HashSet<>();
   *          for (int i = 0; i < resultRecordCount; i++) {
   *            r.add(vector.getAccessor().getAsStringBuilder(i).toString());
   *          }
   *          return r;
   *        }
   *      );
   * </pre>
   * @param columnName name of the column to read
   * @param vectorClass returned by the query vector class
   * @param reader lambda to read the vector value representation
   * @param <V> vector class
   * @param <T> return type
   * @return result produced by {@code reader} lambda or {@code null} if no records returned from the query
   *
   */
  @SuppressWarnings("unchecked")
  public <T, V> T vectorValue(String columnName, Class<V> vectorClass, VectorQueryReader<T, V> reader)
      throws RpcException, SchemaChangeException {

    List<QueryDataBatch> result = results();
    RecordBatchLoader loader = new RecordBatchLoader(client.allocator());
    QueryDataBatch queryDataBatch = null;

    try {
      queryDataBatch = result.get(0);
      loader.load(queryDataBatch.getHeader().getDef(), queryDataBatch.getData());

      V vector = (V) loader.getValueAccessorById(
          vectorClass,
          loader.getValueVectorId(SchemaPath.getCompoundPath(columnName)).getFieldIds())
          .getValueVector();

      return (loader.getRecordCount() > 0) ? reader.read(loader.getRecordCount(), vector) : null;
    } finally {
      if (queryDataBatch != null) {
        queryDataBatch.release();
      }
      loader.clear();
    }
  }

  /**
   * Run the query that is expected to return (at least) one row
   * with the only (or first) column returning a {@link T} value.
   * The {@link T} value cannot be null.
   *
   * @return the value of the first column of the first row
   * @throws RpcException if anything goes wrong
   */
  private <T> T singletonGeneric(SingletonScalarReader<T> scalarReader) throws RpcException {
    RowSet rowSet = rowSet();
    if (rowSet == null) {
      throw new IllegalStateException("No rows returned");
    }
    try {
      RowSetReader reader = rowSet.reader();
      reader.next();
      return scalarReader.read(reader.scalar(0));
    } finally {
      rowSet.clear();
    }
  }

  /**
   * Run the query that is expected to return (at least) one row
   * with the only (or first) column returning a long value.
   * The long value cannot be null.
   *
   * @return the value of the first column of the first row
   * @throws RpcException if anything goes wrong
   */
  public long singletonLong() throws RpcException {
    return singletonGeneric(ScalarReader::getLong);
  }

  /**
   * Run the query that is expected to return (at least) one row
   * with the only (or first) column returning a double value.
   * The double value cannot be null.
   *
   * @return the value of the first column of the first row
   * @throws RpcException if anything goes wrong
   */
  public double singletonDouble() throws RpcException {
    return singletonGeneric(ScalarReader::getDouble);
  }

  /**
   * Run the query that is expected to return (at least) one row
   * with the only (or first) column returning a int value.
   * The int value cannot be null.
   *
   * @return the value of the first column of the first row
   * @throws RpcException if anything goes wrong
   */
  public int singletonInt() throws RpcException {
    return singletonGeneric(ScalarReader::getInt);
  }

  /**
   * Run the query that is expected to return (at least) one row
   * with the only (or first) column returning a {@link Period} value.
   * The {@link Period} value cannot be null.
   *
   * @return the value of the first column of the first row
   * @throws RpcException if anything goes wrong
   */
  public Period singletonPeriod() throws RpcException {
    return singletonGeneric(ScalarReader::getPeriod);
  }

  /**
   * Run the query that is expected to return (at least) one row
   * with the only (or first) column returning a string value.
   * The value may be null, in which case a null string is returned.
   *
   * @return the value of the first column of the first row
   * @throws RpcException if anything goes wrong
   */

  public String singletonString() throws RpcException {
    return singletonGeneric(ScalarReader::getString);
  }

  /**
   * Run the query with the listener provided. Use when the result
   * count will be large, or you don't need the results.
   *
   * @param listener the Drill listener
   */

  public void withListener(UserResultsListener listener) {
    Preconditions.checkNotNull(queryType, "Query not provided.");
    if (planFragments != null) {
      try {
        client.client().runQuery(QueryType.EXECUTION, planFragments, listener);
      } catch(RpcException e) {
        throw new IllegalStateException(e);
      }
    } else {
      Preconditions.checkNotNull(queryText, "Query not provided.");
      client.client().runQuery(queryType, queryText, listener);
    }
  }

  /**
   * Run the query, return an easy-to-use event listener to process
   * the query results. Use when the result set is large. The listener
   * allows the caller to iterate over results in the test thread.
   * (The listener implements a producer-consumer model to hide the
   * details of Drill listeners.)
   *
   * @return the query event listener
   */

  public BufferingQueryEventListener withEventListener() {
    BufferingQueryEventListener listener = new BufferingQueryEventListener();
    withListener(listener);
    return listener;
  }

  public long logCsv() {
    return log(Format.CSV);
  }

  public long log(Format format) {
    return log(format,20);
  }

  public long log(Format format, int colWidth) {
    return runAndWait(new LoggingResultsListener(client.cluster().config(), format, colWidth));
  }

  /**
   * <p>
   *   Run a query and logs the output in TSV format.
   *   Similar to {@link QueryTestUtil#testRunAndLog} with one query.
   * </p>
   *
   * @return The number of rows returned.
   * @throws Exception If anything goes wrong with query execution.
   */
  public long log() throws Exception {
    return log(Format.TSV, VectorUtil.DEFAULT_COLUMN_WIDTH);
  }

  public long printCsv() {
    return print(Format.CSV);
  }

  public long print(Format format) {
    return print(format,20);
  }

  public long print(Format format, int colWidth) {
    return runAndWait(new PrintingResultsListener(client.cluster().config(), format, colWidth));
  }

  /**
   * <p>
   *   Runs a query and prints the output to stdout in TSV format.
   *   Similar to {@link QueryTestUtil#testRunAndLog} with one query.
   * </p>
   *
   * @return The number of rows returned.
   * @throws Exception If anything goes wrong with query execution.
   */
  public long print() throws Exception {
    return print(Format.TSV, VectorUtil.DEFAULT_COLUMN_WIDTH);
  }

  /**
   * Run the query asynchronously, returning a future to be used
   * to check for query completion, wait for completion, and obtain
   * the result summary.
   */

  public QuerySummaryFuture futureSummary() {
    QuerySummaryFuture future = new QuerySummaryFuture();
    withListener(new SummaryOnlyQueryEventListener(future));
    return future;
  }

  public long runAndWait(UserResultsListener listener) {
    AwaitableUserResultsListener resultListener =
        new AwaitableUserResultsListener(listener);
    withListener(resultListener);
    try {
      return resultListener.await();
    } catch (UserRemoteException e) {
      throw e;
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Submit an "EXPLAIN" statement, and return text form of the
   * plan.
   * @throws Exception if the query fails
   */

  public String explainText() throws Exception {
    return explain(ClusterFixture.EXPLAIN_PLAN_TEXT);
  }

  /**
   * Submit an "EXPLAIN" statement, and return the JSON form of the
   * plan.
   * @throws Exception if the query fails
   */

  public String explainJson() throws Exception {
    return explain(ClusterFixture.EXPLAIN_PLAN_JSON);
  }

  public String explain(String format) throws Exception {
    queryText = "EXPLAIN PLAN FOR " + queryText;
    return queryPlan(format);
  }

  private QuerySummary produceSummary(BufferingQueryEventListener listener) throws Exception {
    long start = System.currentTimeMillis();
    int recordCount = 0;
    int batchCount = 0;
    QueryId queryId = null;
    QueryState state = null;
    loop:
    for (;;) {
      QueryEvent event = listener.get();
      switch (event.type)
      {
      case BATCH:
        batchCount++;
        recordCount += event.batch.getHeader().getRowCount();
        event.batch.release();
        break;
      case EOF:
        state = event.state;
        break loop;
      case ERROR:
        throw event.error;
      case QUERY_ID:
        queryId = event.queryId;
        break;
      default:
        throw new IllegalStateException("Unexpected event: " + event.type);
      }
    }
    long end = System.currentTimeMillis();
    long elapsed = end - start;
    return new QuerySummary(queryId, recordCount, batchCount, elapsed, state);
  }

  public QueryResultSet resultSet() {
    BufferingQueryEventListener listener = withEventListener();
    return new QueryResultSet(listener, client.allocator());
  }

  /**
   * Submit an "EXPLAIN" statement, and return the column value which
   * contains the plan's string.
   * <p>
   * Cribbed from {@link PlanTestBase#getPlanInString(String, String)}
   * @throws Exception if anything goes wrogn in the query
   */

  protected String queryPlan(String columnName) throws Exception {
    Preconditions.checkArgument(queryType == QueryType.SQL, "Can only explain an SQL query.");
    final List<QueryDataBatch> results = results();
    final RecordBatchLoader loader = new RecordBatchLoader(client.allocator());
    final StringBuilder builder = new StringBuilder();

    for (final QueryDataBatch b : results) {
      if (!b.hasData()) {
        continue;
      }

      loader.load(b.getHeader().getDef(), b.getData());

      final VectorWrapper<?> vw;
      try {
          vw = loader.getValueAccessorById(
              NullableVarCharVector.class,
              loader.getValueVectorId(SchemaPath.getSimplePath(columnName)).getFieldIds());
      } catch (Throwable t) {
        throw new IllegalStateException("Looks like you did not provide an explain plan query, please add EXPLAIN PLAN FOR to the beginning of your query.");
      }

      final ValueVector vv = vw.getValueVector();
      for (int i = 0; i < vv.getAccessor().getValueCount(); i++) {
        final Object o = vv.getAccessor().getObject(i);
        builder.append(o);
      }
      loader.clear();
      b.release();
    }

    return builder.toString();
  }
}
