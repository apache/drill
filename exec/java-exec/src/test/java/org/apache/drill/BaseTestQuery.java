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

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.io.Files;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.scanner.ClassPathScanner;
import org.apache.drill.common.scanner.persistence.ScanResult;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ExecTest;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.RootAllocatorFactory;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.QueryResult.QueryState;
import org.apache.drill.exec.proto.UserBitShared.QueryType;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.rpc.user.ConnectionThrottle;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.rpc.user.UserResultsListener;
import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.util.TestUtilities;
import org.apache.drill.exec.util.VectorUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.io.Resources;

import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class BaseTestQuery extends ExecTest {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BaseTestQuery.class);

  protected static final String TEMP_SCHEMA = "dfs_test.tmp";

  private static final String ENABLE_FULL_CACHE = "drill.exec.test.use-full-cache";
  private static final int MAX_WIDTH_PER_NODE = 2;

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
  protected static Drillbit[] bits;
  protected static RemoteServiceSet serviceSet;
  protected static DrillConfig config;
  protected static BufferAllocator allocator;

  /**
   * Number of Drillbits in test cluster. Default is 1.
   *
   * Tests can update the cluster size through {@link #updateTestCluster(int, DrillConfig)}
   */
  private static int drillbitCount = 1;

  /**
   * Location of the dfs_test.tmp schema on local filesystem.
   */
  private static String dfsTestTmpSchemaLocation;

  private int[] columnWidths = new int[] { 8 };

  private static ScanResult classpathScan;

  @BeforeClass
  public static void setupDefaultTestCluster() throws Exception {
    config = DrillConfig.create(TEST_CONFIGURATIONS);
    classpathScan = ClassPathScanner.fromPrescan(config);
    openClient();
  }

  protected static void updateTestCluster(int newDrillbitCount, DrillConfig newConfig) {
    Preconditions.checkArgument(newDrillbitCount > 0, "Number of Drillbits must be at least one");
    if (drillbitCount != newDrillbitCount || config != null) {
      // TODO: Currently we have to shutdown the existing Drillbit cluster before starting a new one with the given
      // Drillbit count. Revisit later to avoid stopping the cluster.
      try {
        closeClient();
        drillbitCount = newDrillbitCount;
        if (newConfig != null) {
          // For next test class, updated DrillConfig will be replaced by default DrillConfig in BaseTestQuery as part
          // of the @BeforeClass method of test class.
          config = newConfig;
        }
        openClient();
      } catch(Exception e) {
        throw new RuntimeException("Failure while updating the test Drillbit cluster.", e);
      }
    }
  }

  /**
   * Useful for tests that require a DrillbitContext to get/add storage plugins, options etc.
   *
   * @return DrillbitContext of first Drillbit in the cluster.
   */
  protected static DrillbitContext getDrillbitContext() {
    Preconditions.checkState(bits != null && bits[0] != null, "Drillbits are not setup.");
    return bits[0].getContext();
  }

  protected static Properties cloneDefaultTestConfigProperties() {
    final Properties props = new Properties();
    for(String propName : TEST_CONFIGURATIONS.stringPropertyNames()) {
      props.put(propName, TEST_CONFIGURATIONS.getProperty(propName));
    }

    return props;
  }

  protected static String getDfsTestTmpSchemaLocation() {
    return dfsTestTmpSchemaLocation;
  }

  private static void resetClientAndBit() throws Exception{
    closeClient();
    openClient();
  }

  private static void openClient() throws Exception {
    allocator = RootAllocatorFactory.newRoot(config);
    if (config.hasPath(ENABLE_FULL_CACHE) && config.getBoolean(ENABLE_FULL_CACHE)) {
      serviceSet = RemoteServiceSet.getServiceSetWithFullCache(config, allocator);
    } else {
      serviceSet = RemoteServiceSet.getLocalServiceSet();
    }

    dfsTestTmpSchemaLocation = TestUtilities.createTempDir();

    bits = new Drillbit[drillbitCount];
    for(int i = 0; i < drillbitCount; i++) {
      bits[i] = new Drillbit(config, serviceSet, classpathScan);
      bits[i].run();

      final StoragePluginRegistry pluginRegistry = bits[i].getContext().getStorage();
      TestUtilities.updateDfsTestTmpSchemaLocation(pluginRegistry, dfsTestTmpSchemaLocation);
      TestUtilities.makeDfsTmpSchemaImmutable(pluginRegistry);
    }

    client = QueryTestUtil.createClient(config,  serviceSet, MAX_WIDTH_PER_NODE, null);
  }

  /**
   * Close the current <i>client</i> and open a new client using the given <i>properties</i>. All tests executed
   * after this method call use the new <i>client</i>.
   *
   * @param properties
   */
  public static void updateClient(Properties properties) throws Exception {
    Preconditions.checkState(bits != null && bits[0] != null, "Drillbits are not setup.");
    if (client != null) {
      client.close();
      client = null;
    }

    client = QueryTestUtil.createClient(config, serviceSet, MAX_WIDTH_PER_NODE, properties);
  }

  /*
   * Close the current <i>client</i> and open a new client for the given user. All tests executed
   * after this method call use the new <i>client</i>.
   * @param user
   */
  public static void updateClient(String user) throws Exception {
    updateClient(user, null);
  }

  /*
   * Close the current <i>client</i> and open a new client for the given user and password credentials. Tests
   * executed after this method call use the new <i>client</i>.
   * @param user
   */
  public static void updateClient(final String user, final String password) throws Exception {
    final Properties props = new Properties();
    props.setProperty(UserSession.USER, user);
    if (password != null) {
      props.setProperty(UserSession.PASSWORD, password);
    }
    updateClient(props);
  }

  protected static BufferAllocator getAllocator() {
    return allocator;
  }

  public static TestBuilder newTest() {
    return testBuilder();
  }

  public static TestBuilder testBuilder() {
    return new TestBuilder(allocator);
  }

  @AfterClass
  public static void closeClient() throws IOException {
    if (client != null) {
      client.close();
    }

    if (bits != null) {
      for(final Drillbit bit : bits) {
        if (bit != null) {
          bit.close();
        }
      }
    }

    if(serviceSet != null) {
      serviceSet.close();
    }
    if (allocator != null) {
      allocator.close();
    }
  }

  @AfterClass
  public static void resetDrillbitCount() {
    // some test classes assume this value to be 1 and will fail if run along other tests that increase it
    drillbitCount = 1;
  }

  protected static void runSQL(String sql) throws Exception {
    final SilentListener listener = new SilentListener();
    testWithListener(QueryType.SQL, sql, listener);
    listener.waitForCompletion();
  }

  protected static List<QueryDataBatch> testSqlWithResults(String sql) throws Exception{
    return testRunAndReturn(QueryType.SQL, sql);
  }

  protected static List<QueryDataBatch> testLogicalWithResults(String logical) throws Exception{
    return testRunAndReturn(QueryType.LOGICAL, logical);
  }

  protected static List<QueryDataBatch> testPhysicalWithResults(String physical) throws Exception{
    return testRunAndReturn(QueryType.PHYSICAL, physical);
  }

  public static List<QueryDataBatch>  testRunAndReturn(QueryType type, String query) throws Exception{
    query = QueryTestUtil.normalizeQuery(query);
    return client.runQuery(type, query);
  }

  public static int testRunAndPrint(final QueryType type, final String query) throws Exception {
    return QueryTestUtil.testRunAndPrint(client, type, query);
  }

  protected static void testWithListener(QueryType type, String query, UserResultsListener resultListener) {
    QueryTestUtil.testWithListener(client, type, query, resultListener);
  }

  public static void testNoResult(String query, Object... args) throws Exception {
    testNoResult(1, query, args);
  }

  protected static void testNoResult(int interation, String query, Object... args) throws Exception {
    query = String.format(query, args);
    logger.debug("Running query:\n--------------\n" + query);
    for (int i = 0; i < interation; i++) {
      final List<QueryDataBatch> results = client.runQuery(QueryType.SQL, query);
      for (final QueryDataBatch queryDataBatch : results) {
        queryDataBatch.release();
      }
    }
  }

  public static void test(String query, Object... args) throws Exception {
    QueryTestUtil.test(client, String.format(query, args));
  }

  public static void test(final String query) throws Exception {
    QueryTestUtil.test(client, query);
  }

  protected static int testLogical(String query) throws Exception{
    return testRunAndPrint(QueryType.LOGICAL, query);
  }

  protected static int testPhysical(String query) throws Exception{
    return testRunAndPrint(QueryType.PHYSICAL, query);
  }

  protected static int testSql(String query) throws Exception{
    return testRunAndPrint(QueryType.SQL, query);
  }

  protected static void testPhysicalFromFile(String file) throws Exception{
    testPhysical(getFile(file));
  }

  protected static List<QueryDataBatch> testPhysicalFromFileWithResults(String file) throws Exception {
    return testRunAndReturn(QueryType.PHYSICAL, getFile(file));
  }

  protected static void testLogicalFromFile(String file) throws Exception{
    testLogical(getFile(file));
  }

  protected static void testSqlFromFile(String file) throws Exception{
    test(getFile(file));
  }

  /**
   * Utility method which tests given query produces a {@link UserException} and the exception message contains
   * the given message.
   * @param testSqlQuery Test query
   * @param expectedErrorMsg Expected error message.
   */
  protected static void errorMsgTestHelper(final String testSqlQuery, final String expectedErrorMsg) throws Exception {
    UserException expException = null;
    try {
      test(testSqlQuery);
    } catch (final UserException ex) {
      expException = ex;
    }

    assertNotNull("Expected a UserException", expException);
    assertThat(expException.getMessage(), containsString(expectedErrorMsg));
  }

  /**
   * Utility method which tests given query produces a {@link UserException}
   * with {@link org.apache.drill.exec.proto.UserBitShared.DrillPBError.ErrorType} being DrillPBError.ErrorType.PARSE
   * the given message.
   * @param testSqlQuery Test query
   */
  protected static void parseErrorHelper(final String testSqlQuery) throws Exception {
    errorMsgTestHelper(testSqlQuery, UserBitShared.DrillPBError.ErrorType.PARSE.name());
  }

  public static String getFile(String resource) throws IOException{
    final URL url = Resources.getResource(resource);
    if (url == null) {
      throw new IOException(String.format("Unable to find path %s.", resource));
    }
    return Resources.toString(url, Charsets.UTF_8);
  }

  /**
   * Copy the resource (ex. file on classpath) to a physical file on FileSystem.
   * @param resource
   * @return the file path
   * @throws IOException
   */
  public static String getPhysicalFileFromResource(final String resource) throws IOException {
    final File file = File.createTempFile("tempfile", ".txt");
    file.deleteOnExit();
    final PrintWriter printWriter = new PrintWriter(file);
    printWriter.write(BaseTestQuery.getFile(resource));
    printWriter.close();

    return file.getPath();
  }

  /**
   * Create a temp directory to store the given <i>dirName</i>
   * @param dirName
   * @return Full path including temp parent directory and given directory name.
   */
  public static String getTempDir(final String dirName) {
    final File dir = Files.createTempDir();
    dir.deleteOnExit();

    return dir.getAbsolutePath() + File.separator + dirName;
  }


  protected static void setSessionOption(final String option, final String value) {
    try {
      runSQL(String.format("alter session set `%s` = %s", option, value));
    } catch(final Exception e) {
      fail(String.format("Failed to set session option `%s` = %s, Error: %s", option, value, e.toString()));
    }
  }

  public static class SilentListener implements UserResultsListener {
    private volatile UserException exception;
    private final AtomicInteger count = new AtomicInteger();
    private final CountDownLatch latch = new CountDownLatch(1);

    @Override
    public void submissionFailed(UserException ex) {
      exception = ex;
      System.out.println("Query failed: " + ex.getMessage());
      latch.countDown();
    }

    @Override
    public void queryCompleted(QueryState state) {
      System.out.println("Query completed successfully with row count: " + count.get());
      latch.countDown();
    }

    @Override
    public void dataArrived(QueryDataBatch result, ConnectionThrottle throttle) {
      final int rows = result.getHeader().getRowCount();
      if (result.getData() != null) {
        count.addAndGet(rows);
      }
      result.release();
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

  protected int printResult(List<QueryDataBatch> results) throws SchemaChangeException {
    int rowCount = 0;
    final RecordBatchLoader loader = new RecordBatchLoader(getAllocator());
    for(final QueryDataBatch result : results) {
      rowCount += result.getHeader().getRowCount();
      loader.load(result.getHeader().getDef(), result.getData());
      // TODO:  Clean:  DRILL-2933:  That load(...) no longer throws
      // SchemaChangeException, so check/clean throw clause above.
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

  protected static String getResultString(List<QueryDataBatch> results, String delimiter)
      throws SchemaChangeException {
    final StringBuilder formattedResults = new StringBuilder();
    boolean includeHeader = true;
    final RecordBatchLoader loader = new RecordBatchLoader(getAllocator());
    for(final QueryDataBatch result : results) {
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
