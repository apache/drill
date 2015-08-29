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
import com.google.common.io.Files;
import com.google.common.io.Resources;

import org.apache.drill.QueryTestUtil;
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

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
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

    protected static BufferAllocator allocator;

    private static Scope scope;
    private static DrillTestCluster cluster;

    private int[] columnWidths = new int[] { 8 };

    @SuppressWarnings("serial")
    private static final Properties TEST_CONFIGURATIONS = new Properties() {
        {
            put(ExecConstants.SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE, "false");
            put(ExecConstants.HTTP_ENABLE, "false");
        }
    };

    /* *** Cluster Scope Settings *** */

    public static final int MIN_NUM_DRILLBITS = 1;
    public static final int MAX_NUM_DRILLBITS = 5;

    public static final int MIN_PARALLELIZATION_WIDTH = 1;
    public static final int MAX_PARALLELIZATION_WIDTH = 5;

    @Retention(RetentionPolicy.RUNTIME)
    @Target({ElementType.TYPE})
    public @interface ClusterScope {

        /**
         * Returns the scope. {@link Scope#GLOBAL} is default.
         */
        Scope scope() default Scope.GLOBAL;

        /**
         * Sets the number of drillbits to create in the test cluster.
         *
         * Default is <tt>-1</tt>, which indicates that the test framework should choose a
         * random value in the inclusive range [{@link DrillIntegrationTestBase#MIN_NUM_DRILLBITS},
         * {@link DrillIntegrationTestBase#MAX_NUM_DRILLBITS}].
         */
        int bits() default -1;

        /**
         * Sets the value of {@link ExecConstants#MAX_WIDTH_PER_NODE_KEY}, which determines
         * the level of parallelization per drillbit.
         *
         * Default is <tt>-1</tt>, which indicates that the test framework should choose a
         * random value in the inclusive range [{@link DrillIntegrationTestBase#MIN_PARALLELIZATION_WIDTH},
         * {@link DrillIntegrationTestBase#MAX_PARALLELIZATION_WIDTH}].
         */
        int width() default -1;
    }

    /**
     * The scope of a test cluster used together with
     * {@link ClusterScope} annotations on {@link DrillIntegrationTestBase} subclasses.
     */
    public enum Scope {
        /**
         * A cluster that is shared across all test suites
         */
        GLOBAL,
        /**
         * A cluster shared across all methods in a single test suite, but not shared across test suites
         */
        SUITE
    }

    private static Scope getCurrentClusterScope(Class<?> clazz) {
        ClusterScope annotation = getAnnotation(clazz, ClusterScope.class);
        return annotation == null ? Scope.GLOBAL : annotation.scope();
    }

    private static int getNumDrillbits() {
        ClusterScope annotation = getAnnotation(getTestClass(), ClusterScope.class);
        return annotation == null ? -1 : annotation.bits();
    }

    private static int getParallelizationWidth() {
        ClusterScope annotation = getAnnotation(getTestClass(), ClusterScope.class);
        return annotation == null ? -1 : annotation.width();
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

    protected static BufferAllocator getAllocator() {
        return allocator;
    }

    protected static DrillTestCluster getCluster() {
        return cluster;
    }

    @BeforeClass
    public static void beforeClass() throws Exception {

        logger.debug("Initializing test framework for class: {}", getTestClass());
        allocator = new TopLevelAllocator(DrillConfig.create(TEST_CONFIGURATIONS));

        if (cluster != null) {
            logger.info("Re-using existing test cluster of {} drillbits", cluster.bits().length);
            return;
        }

        scope = getCurrentClusterScope(getTestClass());

        int size = getNumDrillbits();
        if (size <= 0 || size > MAX_NUM_DRILLBITS) {
            size = randomIntBetween(MIN_NUM_DRILLBITS, MAX_NUM_DRILLBITS);
        }

        int width = getParallelizationWidth();
        if (width <= 0 || width > MAX_PARALLELIZATION_WIDTH) {
            width = randomIntBetween(MIN_PARALLELIZATION_WIDTH, MAX_PARALLELIZATION_WIDTH);
        }

        switch (scope) {
            case GLOBAL:
            case SUITE:
                cluster = new DrillTestCluster(size, width, DrillConfig.create(TEST_CONFIGURATIONS));
                break;
            default:
                fail("Unsupported cluster scope: " + scope);
        }
    }

    @Before
    public void beforeDrillIntegrationTest() throws Exception {
        ;
    }

    @AfterClass
    public static void afterClass() throws IOException, InterruptedException {
        if (cluster != null) {
           cluster.close();
        }
    }

    @After
    public void afterDrillIntegrationTest() {
        logger.info("Executing post-test cleanup: [{}]", getTestClass());
        if (allocator != null) {
            allocator.close();
            allocator = null;
        }
    }

    protected static final class DrillTestCluster implements Closeable {

        private final Drillbit[]       bits;
        private final DrillClient      client;
        private final DrillConfig      config;
        private final BufferAllocator  allocator;
        private final RemoteServiceSet serviceSet;
        private final String           tmpSchemaLocation;

        public DrillTestCluster(int size, int width, DrillConfig config) throws Exception {

            logger.info("Initializing test cluster of {} drillbits (parallelization width: {})", size, width);

            this.config = config;
            this.allocator = new TopLevelAllocator(config);
            this.tmpSchemaLocation = TestUtilities.createTempDir();
            this.serviceSet = RemoteServiceSet.getLocalServiceSet();

            bits = new Drillbit[size];
            for (int i = 0; i < bits.length; i++) {
                logger.debug("Initializing drillbit #{}", i);
                bits[i] = new Drillbit(config, serviceSet);
                bits[i].run();
                StoragePluginRegistry registry = bits[i].getContext().getStorage();
                TestUtilities.updateDfsTestTmpSchemaLocation(registry, tmpSchemaLocation);
                TestUtilities.makeDfsTmpSchemaImmutable(registry);
            }

            client = QueryTestUtil.createClient(config, serviceSet, width, null);
        }

        public DrillClient client() {
            return client;
        }

        public Drillbit[] bits() {
            return bits;
        }

        public DrillConfig config() {
            return config;
        }

        public BufferAllocator allocator() {
            return allocator;
        }

        public Drillbit randomBit() {
            return randomFrom(bits);
        }

        public DrillbitContext randomDrillBitContext() {
            return randomBit().getContext();
        }

        public RemoteServiceSet serviceSet() {
            return serviceSet;
        }

        @Override
        public void close() throws IOException {
            cluster.client().close();
            for (Drillbit bit : cluster.bits()) {
                bit.close();
            }
            cluster.serviceSet().close();
            cluster.allocator().close();
        }
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
        return cluster.client().runQuery(type, query);
    }

    public static int testRunAndPrint(final UserBitShared.QueryType type, final String query) throws Exception {
        return QueryTestUtil.testRunAndPrint(cluster.client(), type, query);
    }

    protected static void testWithListener(UserBitShared.QueryType type, String query, UserResultsListener resultListener) {
        QueryTestUtil.testWithListener(cluster.client(), type, query, resultListener);
    }

    protected static void testNoResult(String query, Object... args) throws Exception {
        testNoResult(1, query, args);
    }

    protected static void testNoResult(int iteration, String query, Object... args) throws Exception {
        query = String.format(query, args);
        logger.debug("Running query:\n--------------\n" + query);
        for (int i = 0; i < iteration; i++) {
            List<QueryDataBatch> results = cluster.client().runQuery(UserBitShared.QueryType.SQL, query);
            for (QueryDataBatch queryDataBatch : results) {
                queryDataBatch.release();
            }
        }
    }

    public static void test(String query, Object... args) throws Exception {
        QueryTestUtil.test(cluster.client(), String.format(query, args));
    }

    public static void test(final String query) throws Exception {
        QueryTestUtil.test(cluster.client(), query);
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
}
