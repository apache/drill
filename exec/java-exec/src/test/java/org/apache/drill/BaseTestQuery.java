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
package org.apache.drill;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.util.TestTools;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ExecTest;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.client.PrintingResultsListener;
import org.apache.drill.exec.client.QuerySubmitter;
import org.apache.drill.exec.client.QuerySubmitter.Format;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.TopLevelAllocator;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.QueryType;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.user.ConnectionThrottle;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.rpc.user.UserResultsListener;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.apache.drill.exec.util.VectorUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;

public class BaseTestQuery extends ExecTest{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BaseTestQuery.class);

  private int[] columnWidths = new int[] { 8 };

  private static final String ENABLE_FULL_CACHE = "drill.exec.test.use-full-cache";

  @SuppressWarnings("serial")
  private static final Properties TEST_CONFIGURATIONS = new Properties() {
    {
      put(ExecConstants.SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE, "false");
      put(ExecConstants.HTTP_ENABLE, "false");
    }
  };

  public final TestRule resetWatcher = new TestWatcher() {
    @Override
    protected void failed(Throwable e, Description description) {
      try {
        resetClientAndBit();
      } catch (Exception e1) {
        throw new RuntimeException("Failure while resetting client.", e1);
      }
    }
  };

  protected static DrillClient client;
  protected static Drillbit bit;
  protected static RemoteServiceSet serviceSet;
  protected static DrillConfig config;
  protected static QuerySubmitter submitter = new QuerySubmitter();
  protected static BufferAllocator allocator;

  static void resetClientAndBit() throws Exception{
    closeClient();
    openClient();
  }

  @BeforeClass
  public static void openClient() throws Exception{
    config = DrillConfig.create(TEST_CONFIGURATIONS);
    allocator = new TopLevelAllocator(config);
    if (config.hasPath(ENABLE_FULL_CACHE) && config.getBoolean(ENABLE_FULL_CACHE)) {
      serviceSet = RemoteServiceSet.getServiceSetWithFullCache(config, allocator);
    } else {
      serviceSet = RemoteServiceSet.getLocalServiceSet();
    }
    bit = new Drillbit(config, serviceSet);
    bit.run();
    client = new DrillClient(config, serviceSet.getCoordinator());
    client.connect();
    List<QueryResultBatch> results = client.runQuery(QueryType.SQL, String.format("alter session set `%s` = 2", ExecConstants.MAX_WIDTH_PER_NODE_KEY));
    for (QueryResultBatch b : results) {
      b.release();
    }
  }

  protected BufferAllocator getAllocator() {
    return allocator;
  }

  public TestBuilder newTest() {
    return testBuilder();
  }

  public TestBuilder testBuilder() {
    return new TestBuilder(allocator);
  }

  @AfterClass
  public static void closeClient() throws IOException{
    if (client != null) {
      client.close();
    }
    if (bit != null) {
      bit.close();
    }
    if(serviceSet != null) {
      serviceSet.close();
    }
    if (allocator != null) {
      allocator.close();
    }
  }

  protected void runSQL(String sql) throws Exception {
    SilentListener listener = new SilentListener();
    testWithListener(QueryType.SQL, sql, listener);
    listener.waitForCompletion();
  }

  protected List<QueryResultBatch> testSqlWithResults(String sql) throws Exception{
    return testRunAndReturn(QueryType.SQL, sql);
  }

  protected List<QueryResultBatch> testLogicalWithResults(String logical) throws Exception{
    return testRunAndReturn(QueryType.LOGICAL, logical);
  }

  protected List<QueryResultBatch> testPhysicalWithResults(String physical) throws Exception{
    return testRunAndReturn(QueryType.PHYSICAL, physical);
  }

  public static List<QueryResultBatch>  testRunAndReturn(QueryType type, String query) throws Exception{
    query = normalizeQuery(query);
    return client.runQuery(type, query);
  }

  public static int testRunAndPrint(QueryType type, String query) throws Exception{
    query = normalizeQuery(query);
    PrintingResultsListener resultListener = new PrintingResultsListener(client.getConfig(), Format.TSV, VectorUtil.DEFAULT_COLUMN_WIDTH);
    client.runQuery(type, query, resultListener);
    return resultListener.await();
  }

  protected void testWithListener(QueryType type, String query, UserResultsListener resultListener) {
    query = normalizeQuery(query);
    client.runQuery(type, query, resultListener);
  }

  protected void testNoResult(String query, Object... args) throws Exception {
    testNoResult(1, query, args);
  }

  protected void testNoResult(int interation, String query, Object... args) throws Exception {
    query = String.format(query, args);
    logger.debug("Running query:\n--------------\n"+query);
    for (int i = 0; i < interation; i++) {
      List<QueryResultBatch> results = client.runQuery(QueryType.SQL, query);
      for (QueryResultBatch queryResultBatch : results) {
        queryResultBatch.release();
      }
    }
  }

  public static void test(String query, Object... args) throws Exception {
    test(String.format(query, args));
  }

  public static void test(String query) throws Exception{
    query = normalizeQuery(query);
    String[] queries = query.split(";");
    for (String q : queries) {
      if (q.trim().isEmpty()) {
        continue;
      }
      testRunAndPrint(QueryType.SQL, q);
    }
  }

  public static String normalizeQuery(String query) {
    if (query.contains("${WORKING_PATH}")) {
      return query.replaceAll(Pattern.quote("${WORKING_PATH}"), Matcher.quoteReplacement(TestTools.getWorkingPath()));
    } else if (query.contains("[WORKING_PATH]")) {
      return query.replaceAll(Pattern.quote("[WORKING_PATH]"), Matcher.quoteReplacement(TestTools.getWorkingPath()));
    }
    return query;
  }

  protected int testLogical(String query) throws Exception{
    return testRunAndPrint(QueryType.LOGICAL, query);
  }

  protected int testPhysical(String query) throws Exception{
    return testRunAndPrint(QueryType.PHYSICAL, query);
  }

  protected int testSql(String query) throws Exception{
    return testRunAndPrint(QueryType.SQL, query);
  }

  protected void testPhysicalFromFile(String file) throws Exception{
    testPhysical(getFile(file));
  }

  protected List<QueryResultBatch> testPhysicalFromFileWithResults(String file) throws Exception {
    return testRunAndReturn(QueryType.PHYSICAL, getFile(file));
  }

  protected void testLogicalFromFile(String file) throws Exception{
    testLogical(getFile(file));
  }

  protected void testSqlFromFile(String file) throws Exception{
    test(getFile(file));
  }

  public static String getFile(String resource) throws IOException{
    URL url = Resources.getResource(resource);
    if (url == null) {
      throw new IOException(String.format("Unable to find path %s.", resource));
    }
    return Resources.toString(url, Charsets.UTF_8);
  }

  private static class SilentListener implements UserResultsListener {
    private volatile Exception exception;
    private AtomicInteger count = new AtomicInteger();
    private CountDownLatch latch = new CountDownLatch(1);

    @Override
    public void submissionFailed(RpcException ex) {
      exception = ex;
      System.out.println("Query failed: " + ex.getMessage());
      latch.countDown();
    }

    @Override
    public void resultArrived(QueryResultBatch result, ConnectionThrottle throttle) {
      int rows = result.getHeader().getRowCount();
      if (result.getData() != null) {
        count.addAndGet(rows);
      }
      result.release();
      if (result.getHeader().getIsLastChunk()) {
        System.out.println("Query completed successfully with row count: " + count.get());
        latch.countDown();
      }
    }

    @Override
    public void queryIdArrived(QueryId queryId) {}

    public int waitForCompletion() throws Exception {
      latch.await();
      if (exception != null) {
        throw exception;
      }
      return count.get();
    }
  }

  protected void setColumnWidth(int columnWidth) {
    this.columnWidths = new int[] { columnWidth };
  }

  protected void setColumnWidths(int[] columnWidths) {
    this.columnWidths = columnWidths;
  }

  protected int printResult(List<QueryResultBatch> results) throws SchemaChangeException {
    int rowCount = 0;
    RecordBatchLoader loader = new RecordBatchLoader(getAllocator());
    for(QueryResultBatch result : results) {
      rowCount += result.getHeader().getRowCount();
      loader.load(result.getHeader().getDef(), result.getData());
      if (loader.getRecordCount() <= 0) {
        continue;
      }
      VectorUtil.showVectorAccessibleContent(loader, columnWidths);
      loader.clear();
      result.release();
    }
    System.out.println("Total record count: " + rowCount);
    return rowCount;
  }

  protected String getResultString(List<QueryResultBatch> results, String delimiter) throws SchemaChangeException {
    StringBuilder formattedResults = new StringBuilder();
    boolean includeHeader = true;
    RecordBatchLoader loader = new RecordBatchLoader(getAllocator());
    for(QueryResultBatch result : results) {
      loader.load(result.getHeader().getDef(), result.getData());
      if (loader.getRecordCount() <= 0) {
        continue;
      }
      VectorUtil.appendVectorAccessibleContent(loader, formattedResults, delimiter, includeHeader);
      if (!includeHeader) {
        includeHeader = false;
      }
      loader.clear();
      result.release();
    }

    return formattedResults.toString();
  }
}
