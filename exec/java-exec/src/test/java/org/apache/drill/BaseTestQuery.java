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
package org.apache.drill;

import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.drill.DrillTestWrapper.TestServices;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.config.DrillProperties;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.scanner.ClassPathScanner;
import org.apache.drill.common.scanner.persistence.ScanResult;
import org.apache.drill.common.util.TestTools;
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
import org.apache.drill.exec.proto.UserProtos.PreparedStatementHandle;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.rpc.ConnectionThrottle;
import org.apache.drill.exec.rpc.user.AwaitableUserResultsListener;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.rpc.user.UserResultsListener;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.util.TestUtilities;
import org.apache.drill.exec.util.VectorUtil;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.io.Resources;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.ValueVector;

public class BaseTestQuery extends ExecTest {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BaseTestQuery.class);

  public static final String TEST_SCHEMA = "dfs_test";
  public static final String TEMP_SCHEMA = TEST_SCHEMA + ".tmp";

  private static final int MAX_WIDTH_PER_NODE = 2;

  @SuppressWarnings("serial")
  private static final Properties TEST_CONFIGURATIONS = new Properties() {
    {
      put(ExecConstants.SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE, "false");
      put(ExecConstants.HTTP_ENABLE, "false");
      // Increasing retry attempts for testing
      put(ExecConstants.UDF_RETRY_ATTEMPTS, "10");
      put(ExecConstants.SSL_USE_HADOOP_CONF, "false");
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

  private static FileSystem fs;

  @BeforeClass
  public static void setupDefaultTestCluster() throws Exception {
    config = DrillConfig.create(TEST_CONFIGURATIONS);
    classpathScan = ClassPathScanner.fromPrescan(config);
    openClient();
    // turns on the verbose errors in tests
    // sever side stacktraces are added to the message before sending back to the client
    test("ALTER SESSION SET `exec.errors.verbose` = true");
    fs = getLocalFileSystem();
  }

  protected static void updateTestCluster(int newDrillbitCount, DrillConfig newConfig) {
    updateTestCluster(newDrillbitCount, newConfig, null);
  }

  protected static void updateTestCluster(int newDrillbitCount, DrillConfig newConfig, Properties properties) {
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
        openClient(properties);
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
    openClient(null);
  }

  private static void openClient(Properties properties) throws Exception {
    if (properties == null) {
      properties = new Properties();
    }

    allocator = RootAllocatorFactory.newRoot(config);
    serviceSet = RemoteServiceSet.getLocalServiceSet();

    dfsTestTmpSchemaLocation = TestUtilities.createTempDir();

    bits = new Drillbit[drillbitCount];
    for(int i = 0; i < drillbitCount; i++) {
      bits[i] = new Drillbit(config, serviceSet, classpathScan);
      bits[i].run();

      final StoragePluginRegistry pluginRegistry = bits[i].getContext().getStorage();
      TestUtilities.updateDfsTestTmpSchemaLocation(pluginRegistry, dfsTestTmpSchemaLocation);
      TestUtilities.makeDfsTmpSchemaImmutable(pluginRegistry);
    }

    if (!properties.containsKey(DrillProperties.DRILLBIT_CONNECTION)) {
      properties.setProperty(DrillProperties.DRILLBIT_CONNECTION,
          String.format("localhost:%s", bits[0].getUserPort()));
    }

    DrillConfig clientConfig = DrillConfig.forClient();
    client = QueryTestUtil.createClient(clientConfig,  serviceSet, MAX_WIDTH_PER_NODE, properties);
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

    DrillConfig clientConfig = DrillConfig.forClient();
    client = QueryTestUtil.createClient(clientConfig, serviceSet, MAX_WIDTH_PER_NODE, properties);
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
    props.setProperty(DrillProperties.USER, user);
    if (password != null) {
      props.setProperty(DrillProperties.PASSWORD, password);
    }
    updateClient(props);
  }

  protected static BufferAllocator getAllocator() {
    return allocator;
  }

  public static int getUserPort() {
    return bits[0].getUserPort();
  }

  public static TestBuilder newTest() {
    return testBuilder();
  }


  public static class ClassicTestServices implements TestServices {
    @Override
    public BufferAllocator allocator() {
      return allocator;
    }

    @Override
    public void test(String query) throws Exception {
      BaseTestQuery.test(query);
    }

    @Override
    public List<QueryDataBatch> testRunAndReturn(final QueryType type, final Object query) throws Exception {
      return BaseTestQuery.testRunAndReturn(type, query);
    }
  }

  public static TestBuilder testBuilder() {
    return new TestBuilder(new ClassicTestServices());
  }

  @AfterClass
  public static void closeClient() throws Exception {
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
    final AwaitableUserResultsListener listener = new AwaitableUserResultsListener(new SilentListener());
    testWithListener(QueryType.SQL, sql, listener);
    listener.await();
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

  public static List<QueryDataBatch>  testRunAndReturn(QueryType type, Object query) throws Exception{
    if (type == QueryType.PREPARED_STATEMENT) {
      Preconditions.checkArgument(query instanceof PreparedStatementHandle,
          "Expected an instance of PreparedStatement as input query");
      return testPreparedStatement((PreparedStatementHandle)query);
    } else {
      Preconditions.checkArgument(query instanceof String, "Expected a string as input query");
      query = QueryTestUtil.normalizeQuery((String)query);
      return client.runQuery(type, (String)query);
    }
  }

  public static List<QueryDataBatch> testPreparedStatement(PreparedStatementHandle handle) throws Exception {
    return client.executePreparedStatement(handle);
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
    try {
      test(testSqlQuery);
      fail("Expected a UserException when running " + testSqlQuery);
    } catch (final UserException actualException) {
      try {
        assertThat("message of UserException when running " + testSqlQuery, actualException.getMessage(), containsString(expectedErrorMsg));
      } catch (AssertionError e) {
        e.addSuppressed(actualException);
        throw e;
    }
  }
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

  protected static void setSessionOption(final String option, final boolean value) {
    setSessionOption(option, Boolean.toString(value));
  }

  protected static void setSessionOption(final String option, final long value) {
    setSessionOption(option, Long.toString(value));
  }

  protected static void setSessionOption(final String option, final double value) {
    setSessionOption(option, Double.toString(value));
  }

  protected static void setSessionOption(final String option, final String value) {
    try {
      runSQL(String.format("alter session set `%s` = %s", option, value));
    } catch(final Exception e) {
      fail(String.format("Failed to set session option `%s` = %s, Error: %s", option, value, e.toString()));
    }
  }

  public static class SilentListener implements UserResultsListener {
    private final AtomicInteger count = new AtomicInteger();

    @Override
    public void submissionFailed(UserException ex) {
      logger.debug("Query failed: " + ex.getMessage());
    }

    @Override
    public void queryCompleted(QueryState state) {
      logger.debug("Query completed successfully with row count: " + count.get());
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


  public class TestResultSet {

    private final List<List<String>> rows;

    public TestResultSet() {
      rows = new ArrayList<>();
    }

    public TestResultSet(List<QueryDataBatch> batches) throws SchemaChangeException {
      rows = new ArrayList<>();
      convert(batches);
    }

    public void addRow(String... cells) {
      List<String> newRow = Arrays.asList(cells);
      rows.add(newRow);
    }

    public int size() {
      return rows.size();
    }

    @Override public boolean equals(Object o) {
      boolean result = false;

      if (this == o) {
        result = true;
      } else if (o instanceof TestResultSet) {
        TestResultSet that = (TestResultSet) o;
        assertEquals(this.size(), that.size());
        for (int i = 0; i < this.rows.size(); i++) {
          assertEquals(this.rows.get(i).size(), that.rows.get(i).size());
          for (int j = 0; j < this.rows.get(i).size(); ++j) {
            assertEquals(this.rows.get(i).get(j), that.rows.get(i).get(j));
          }
        }
        result = true;
      }

      return result;
    }

    private void convert(List<QueryDataBatch> batches) throws SchemaChangeException {
      RecordBatchLoader loader = new RecordBatchLoader(getAllocator());
      for (QueryDataBatch batch : batches) {
        int rc = batch.getHeader().getRowCount();
        if (batch.getData() != null) {
          loader.load(batch.getHeader().getDef(), batch.getData());
          for (int i = 0; i < rc; ++i) {
            List<String> newRow = new ArrayList<>();
            rows.add(newRow);
            for (VectorWrapper<?> vw : loader) {
              ValueVector.Accessor accessor = vw.getValueVector().getAccessor();
              Object o = accessor.getObject(i);
              newRow.add(o == null ? null : o.toString());
            }
          }
        }
        loader.clear();
        batch.release();
      }
    }
  }

  private static String replaceWorkingPathInString(String orig) {
    return orig.replaceAll(Pattern.quote("[WORKING_PATH]"), Matcher.quoteReplacement(TestTools.getWorkingPath()));
  }

  protected static void copyDirectoryIntoTempSpace(String resourcesDir) throws IOException {
    copyDirectoryIntoTempSpace(resourcesDir, null);
  }

  protected static void copyDirectoryIntoTempSpace(String resourcesDir, String destinationSubDir) throws IOException {
    Path destination = destinationSubDir != null ? new Path(getDfsTestTmpSchemaLocation(), destinationSubDir)
        : new Path(getDfsTestTmpSchemaLocation());
    fs.copyFromLocalFile(
        new Path(replaceWorkingPathInString(resourcesDir)),
        destination);
  }

  protected static void copyMetaDataCacheToTempReplacingInternalPaths(String srcFileOnClassPath, String destFolderInTmp,
      String metaFileName) throws IOException {
    copyMetaDataCacheToTempWithReplacements(srcFileOnClassPath, destFolderInTmp, metaFileName, null);
  }

  protected static void copyMetaDataCacheToTempReplacingInternalPaths(Path srcFileOnClassPath, String destFolderInTmp,
                                                                      String metaFileName) throws IOException {
    copyMetaDataCacheToTempReplacingInternalPaths(srcFileOnClassPath.toUri().getPath(), destFolderInTmp, metaFileName);
  }

  /**
   * Old metadata cache files include full paths to the files that have been scanned.
   * <p>
   * There is no way to generate a metadata cache file with absolute paths that
   * will be guaranteed to be available on an arbitrary test machine.
   * <p>
   * To enable testing older metadata cache files, they were generated manually
   * using older drill versions, and the absolute path up to the folder where
   * the metadata cache file appeared was manually replaced with the string
   * REPLACED_IN_TEST. Here the file is re-written into the given temporary
   * location after the REPLACED_IN_TEST string has been replaced by the actual
   * location generated during this run of the tests.
   *
   * @param srcFileOnClassPath the source path of metadata cache file, which should be replaced
   * @param destFolderInTmp  the parent folder name of the metadata cache file
   * @param metaFileName the name of metadata cache file depending on the type of the metadata
   * @param customStringReplacement custom string to replace the "CUSTOM_REPLACED" target string in metadata file
   * @throws IOException if a create or write errors occur
   */
  protected static void copyMetaDataCacheToTempWithReplacements(String srcFileOnClassPath,
      String destFolderInTmp, String metaFileName, String customStringReplacement) throws IOException {
    String metadataFileContents = getFile(srcFileOnClassPath);
    Path rootMeta = new Path(dfsTestTmpSchemaLocation, destFolderInTmp);
    Path newMetaCache = new Path(rootMeta, metaFileName);
    try (FSDataOutputStream outSteam = fs.create(newMetaCache)) {
      if (customStringReplacement != null) {
        metadataFileContents = metadataFileContents.replace("CUSTOM_STRING_REPLACEMENT", customStringReplacement);
      }
      outSteam.writeBytes(metadataFileContents.replace("REPLACED_IN_TEST", dfsTestTmpSchemaLocation));
    }
  }

 }
