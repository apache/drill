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
package org.apache.drill.exec;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import com.google.common.io.Resources;

import org.apache.drill.QueryTestUtil;
import org.apache.drill.TestBuilder;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.TopLevelAllocator;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.rpc.user.ConnectionThrottle;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.rpc.user.UserResultsListener;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.util.TestUtilities;
import org.apache.drill.exec.util.VectorUtil;
import org.apache.drill.test.DrillTestBase;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.annotation.Annotation;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.net.URL;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Abstract base class for integration tests.
 */
public abstract class DrillIntegrationTestBase extends DrillTestBase {

    private static final Logger logger = LoggerFactory.getLogger(DrillIntegrationTestBase.class);

    protected static final String TEMP_SCHEMA = "dfs_test.tmp";

    private static final String ENABLE_FULL_CACHE = "drill.exec.test.use-full-cache";
    private static final int MAX_WIDTH_PER_NODE = 2;

    protected static DrillClient client;
    protected static Drillbit[] bits;
    protected static RemoteServiceSet serviceSet;
    protected static DrillConfig config;
    protected static BufferAllocator allocator;

    @SuppressWarnings("serial")
    private static final Properties TEST_CONFIGURATIONS = new Properties() {
        {
            put(ExecConstants.SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE, "false");
            put(ExecConstants.HTTP_ENABLE, "false");
        }
    };

    /* *** Cluster Scope Settings *** */

    private static final int DEFAULT_MIN_NUM_DRILLBITS = 1;
    private static final int DEFAULT_MAX_NUM_DRILLBITS = 3;

    @Retention(RetentionPolicy.RUNTIME)
    @Target({ElementType.TYPE})
    public @interface ClusterScope {

        /**
         * Returns the scope. {@link Scope#SUITE} is default.
         */
        Scope scope() default Scope.SUITE;

        /**
         * Returns the number of drillbits in the cluster. Default is <tt>-1</tt> which means
         * a random number of drillbits is used, where the minimum and maximum number of nodes
         * are either the specified ones or the default ones if not specified.
         */
        int numDrillbits() default -1;

        /**
         * Returns the minimum number of drillbits in the cluster. Default is <tt>-1</tt>.
         * Ignored when {@link ClusterScope#numDrillbits()} is set.
         */
        int minNumDrillbits() default -1;

        /**
         * Returns the maximum number of drillbits in the cluster. Default is <tt>-1</tt>.
         * Ignored when {@link ClusterScope#numDrillbits()} is set.
         */
        int maxNumDrillbits() default -1;
    }

    /**
     * The scope of a test cluster used together with
     * {@link ClusterScope} annotations on {@link DrillIntegrationTestBase} subclasses.
     */
    public enum Scope {
        /** A cluster shared across all methods in a single test suite */
        SUITE,
        /** A cluster that is exclusive to an individual test */
        TEST
    }

    private Scope getCurrentClusterScope() {
        return getCurrentClusterScope(this.getClass());
    }

    private static Scope getCurrentClusterScope(Class<?> clazz) {
        ClusterScope annotation = getAnnotation(clazz, ClusterScope.class);
        return annotation == null ? Scope.SUITE : annotation.scope();
    }

    private int getNumDrillbits() {
        ClusterScope annotation = getAnnotation(this.getClass(), ClusterScope.class);
        return annotation == null ? -1 : annotation.numDrillbits();
    }

    private int getMinNumDrillbits() {
        ClusterScope annotation = getAnnotation(this.getClass(), ClusterScope.class);
        return annotation == null || annotation.minNumDrillbits() == -1 ? DEFAULT_MIN_NUM_DRILLBITS : annotation.minNumDrillbits();
    }

    private int getMaxNumDrillbits() {
        ClusterScope annotation = getAnnotation(this.getClass(), ClusterScope.class);
        return annotation == null || annotation.maxNumDrillbits() == -1 ? DEFAULT_MAX_NUM_DRILLBITS : annotation.maxNumDrillbits();
    }

    private static <A extends Annotation> A getAnnotation(Class<?> clazz, Class<A> annotationClass) {
        if (clazz == Object.class || clazz == DrillIntegrationTestBase.class) {
            return null;
        }
        A annotation = clazz.getAnnotation(annotationClass);
        if (annotation != null) {
            return annotation;
        }
        return getAnnotation(clazz.getSuperclass(), annotationClass);
    }

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

    @BeforeClass
    public static void setupDefaultTestCluster() throws Exception {
        config = DrillConfig.create(TEST_CONFIGURATIONS);
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
        allocator = new TopLevelAllocator(config);
        if (config.hasPath(ENABLE_FULL_CACHE) && config.getBoolean(ENABLE_FULL_CACHE)) {
            serviceSet = RemoteServiceSet.getServiceSetWithFullCache(config, allocator);
        } else {
            serviceSet = RemoteServiceSet.getLocalServiceSet();
        }

        dfsTestTmpSchemaLocation = TestUtilities.createTempDir();

        bits = new Drillbit[drillbitCount];
        for(int i = 0; i < drillbitCount; i++) {
            bits[i] = new Drillbit(config, serviceSet);
            bits[i].run();

            final StoragePluginRegistry pluginRegistry = bits[i].getContext().getStorage();
            TestUtilities.updateDfsTestTmpSchemaLocation(pluginRegistry, dfsTestTmpSchemaLocation);
            TestUtilities.makeDfsTmpSchemaImmutable(pluginRegistry);
        }

        client = QueryTestUtil.createClient(config, serviceSet, MAX_WIDTH_PER_NODE, null);
    }

    /**
     * Close the current <i>client</i> and open a new client using the given <i>properties</i>. All tests executed
     * after this method call use the new <i>client</i>.
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
        final Properties props = new Properties();
        props.setProperty("user", user);
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
    public static void closeClient() throws IOException, InterruptedException {
        if (client != null) {
            client.close();
        }

        if (bits != null) {
            for(Drillbit bit : bits) {
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
        SilentListener listener = new SilentListener();
        testWithListener(UserBitShared.QueryType.SQL, sql, listener);
        listener.waitForCompletion();
    }

    protected static List<QueryDataBatch> testSqlWithResults(String sql) throws Exception{
        return testRunAndReturn(UserBitShared.QueryType.SQL, sql);
    }

    protected static List<QueryDataBatch> testLogicalWithResults(String logical) throws Exception{
        return testRunAndReturn(UserBitShared.QueryType.LOGICAL, logical);
    }

    protected static List<QueryDataBatch> testPhysicalWithResults(String physical) throws Exception{
        return testRunAndReturn(UserBitShared.QueryType.PHYSICAL, physical);
    }

    public static List<QueryDataBatch>  testRunAndReturn(UserBitShared.QueryType type, String query) throws Exception{
        query = QueryTestUtil.normalizeQuery(query);
        return client.runQuery(type, query);
    }

    public static int testRunAndPrint(final UserBitShared.QueryType type, final String query) throws Exception {
        return QueryTestUtil.testRunAndPrint(client, type, query);
    }

    protected static void testWithListener(UserBitShared.QueryType type, String query, UserResultsListener resultListener) {
        QueryTestUtil.testWithListener(client, type, query, resultListener);
    }

    protected static void testNoResult(String query, Object... args) throws Exception {
        testNoResult(1, query, args);
    }

    protected static void testNoResult(int interation, String query, Object... args) throws Exception {
        query = String.format(query, args);
        logger.debug("Running query:\n--------------\n" + query);
        for (int i = 0; i < interation; i++) {
            List<QueryDataBatch> results = client.runQuery(UserBitShared.QueryType.SQL, query);
            for (QueryDataBatch queryDataBatch : results) {
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
        return testRunAndPrint(UserBitShared.QueryType.LOGICAL, query);
    }

    protected static int testPhysical(String query) throws Exception{
        return testRunAndPrint(UserBitShared.QueryType.PHYSICAL, query);
    }

    protected static int testSql(String query) throws Exception{
        return testRunAndPrint(UserBitShared.QueryType.SQL, query);
    }

    protected static void testPhysicalFromFile(String file) throws Exception{
        testPhysical(getFile(file));
    }

    protected static List<QueryDataBatch> testPhysicalFromFileWithResults(String file) throws Exception {
        return testRunAndReturn(UserBitShared.QueryType.PHYSICAL, getFile(file));
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
        URL url = Resources.getResource(resource);
        if (url == null) {
            throw new IOException(String.format("Unable to find path %s.", resource));
        }
        return Resources.toString(url, Charsets.UTF_8);
    }

    /**
     * Copy the resource (ex. file on classpath) to a physical file on FileSystem.
     * @throws IOException
     */
    public static String getPhysicalFileFromResource(final String resource) throws IOException {
        final File file = File.createTempFile("tempfile", ".txt");
        file.deleteOnExit();
        PrintWriter printWriter = new PrintWriter(file);
        printWriter.write(DrillIntegrationTestBase.getFile(resource));
        printWriter.close();

        return file.getPath();
    }

    /**
     * Create a temp directory to store the given <i>dirName</i>
     * @return Full path including temp parent directory and given directory name.
     */
    public static String getTempDir(final String dirName) {
        File dir = Files.createTempDir();
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

    private static class SilentListener implements UserResultsListener {
        private volatile UserException exception;
        private AtomicInteger count = new AtomicInteger();
        private CountDownLatch latch = new CountDownLatch(1);

        @Override
        public void submissionFailed(UserException ex) {
            exception = ex;
            System.out.println("Query failed: " + ex.getMessage());
            latch.countDown();
        }

        @Override
        public void queryCompleted(UserBitShared.QueryResult.QueryState state) {
            System.out.println("Query completed successfully with row count: " + count.get());
            latch.countDown();
        }

        @Override
        public void dataArrived(QueryDataBatch result, ConnectionThrottle throttle) {
            int rows = result.getHeader().getRowCount();
            if (result.getData() != null) {
                count.addAndGet(rows);
            }
            result.release();
        }

        @Override
        public void queryIdArrived(UserBitShared.QueryId queryId) {}

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
        RecordBatchLoader loader = new RecordBatchLoader(getAllocator());
        for(QueryDataBatch result : results) {
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
        StringBuilder formattedResults = new StringBuilder();
        boolean includeHeader = true;
        RecordBatchLoader loader = new RecordBatchLoader(getAllocator());
        for(QueryDataBatch result : results) {
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
