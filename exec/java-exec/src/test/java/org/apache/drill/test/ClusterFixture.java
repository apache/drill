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

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;
import org.apache.commons.io.FileUtils;
import org.apache.drill.BaseTestQuery;
import org.apache.drill.DrillTestWrapper.TestServices;
import org.apache.drill.QueryTestUtil;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ZookeeperHelper;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.RootAllocatorFactory;
import org.apache.drill.exec.proto.UserBitShared.QueryType;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.StoragePluginRegistryImpl;
import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.drill.exec.store.dfs.FileSystemPlugin;
import org.apache.drill.exec.store.dfs.WorkspaceConfig;
import org.apache.drill.exec.store.mock.MockStorageEngine;
import org.apache.drill.exec.store.mock.MockStorageEngineConfig;
import org.apache.drill.exec.store.sys.store.provider.ZookeeperPersistentStoreProvider;
import org.apache.drill.exec.util.TestUtilities;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import com.google.common.io.Resources;

/**
 * Test fixture to start a Drillbit with provide options, create a client, and
 * execute queries. Can be used in JUnit tests, or in ad-hoc programs. Provides
 * a builder to set the necessary embedded Drillbit and client options, then
 * creates the requested Drillbit and client.
 */

public class ClusterFixture implements AutoCloseable {
  // private static final org.slf4j.Logger logger =
  // org.slf4j.LoggerFactory.getLogger(ClientFixture.class);
  public static final String ENABLE_FULL_CACHE = "drill.exec.test.use-full-cache";
  public static final int MAX_WIDTH_PER_NODE = 2;

  @SuppressWarnings("serial")
  public static final Properties TEST_CONFIGURATIONS = new Properties() {
    {
      // Properties here mimic those in drill-root/pom.xml, Surefire plugin
      // configuration. They allow tests to run successfully in Eclipse.

      put(ExecConstants.SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE, false);

      // The CTTAS function requires that the default temporary workspace be
      // writable. By default, the default temporary workspace points to
      // dfs.tmp. But, the test setup marks dfs.tmp as read-only. To work
      // around this, tests are supposed to use dfs_test. So, we need to
      // set the default temporary workspace to dfs_test.tmp.

      put(ExecConstants.DEFAULT_TEMPORARY_WORKSPACE, BaseTestQuery.TEMP_SCHEMA);
      put(ExecConstants.HTTP_ENABLE, false);
      put(QueryTestUtil.TEST_QUERY_PRINTING_SILENT, true);
      put("drill.catastrophic_to_standard_out", true);

      // Verbose errors.

      put(ExecConstants.ENABLE_VERBOSE_ERRORS_KEY, true);

      // See Drillbit.close. The Drillbit normally waits a specified amount
      // of time for ZK registration to drop. But, embedded Drillbits normally
      // don't use ZK, so no need to wait.

      put(ExecConstants.ZK_REFRESH, 0);

      // This is just a test, no need to be heavy-duty on threads.
      // This is the number of server and client RPC threads. The
      // production default is DEFAULT_SERVER_RPC_THREADS.

      put(ExecConstants.BIT_SERVER_RPC_THREADS, 2);

      // No need for many scanners except when explicitly testing that
      // behavior. Production default is DEFAULT_SCAN_THREADS

      put(ExecConstants.SCAN_THREADPOOL_SIZE, 4);

      // Define a useful root location for the ZK persistent
      // storage. Profiles will go here when running in distributed
      // mode.

      put(ZookeeperPersistentStoreProvider.DRILL_EXEC_SYS_STORE_PROVIDER_ZK_BLOBROOT, "/tmp/drill/log");
    }
  };

  public static final String DEFAULT_BIT_NAME = "drillbit";

  private DrillConfig config;
  private Map<String, Drillbit> bits = new HashMap<>();
  private Drillbit defaultDrillbit;
  private BufferAllocator allocator;
  private boolean ownsZK;
  private ZookeeperHelper zkHelper;
  private RemoteServiceSet serviceSet;
  private File dfsTestTempDir;
  protected List<ClientFixture> clients = new ArrayList<>();
  private boolean usesZk;
  private boolean preserveLocalFiles;
  private boolean isLocal;
  private Properties clientProps;

  /**
   * Temporary directories created for this test cluster.
   * Each is removed when closing the cluster.
   */

  private List<File> tempDirs = new ArrayList<>();

  ClusterFixture(FixtureBuilder builder) {

    setClientProps(builder);
    configureZk(builder);
    try {
      createConfig(builder);
      allocator = RootAllocatorFactory.newRoot(config);
      startDrillbits(builder);
      applyOptions(builder);
    } catch (Exception e) {
      // Translate exceptions to unchecked to avoid cluttering
      // tests. Failures will simply fail the test itself.

      throw new IllegalStateException( "Cluster fixture setup failed", e );
    }
  }

  /**
   * Set the client properties to be used by client fixture.
   * @param builder {@link FixtureBuilder#clientProps}
   */
  private void setClientProps(FixtureBuilder builder) {
      clientProps = builder.clientProps;
  }

  public Properties getClientProps() {
    return clientProps;
  }

  private void configureZk(FixtureBuilder builder) {

    // Start ZK if requested.

    String zkConnect = null;
    if (builder.zkHelper != null) {
      // Case where the test itself started ZK and we're only using it.

      zkHelper = builder.zkHelper;
      ownsZK = false;
    } else if (builder.localZkCount > 0) {
      // Case where we need a local ZK just for this test cluster.

      zkHelper = new ZookeeperHelper("dummy");
      zkHelper.startZookeeper(builder.localZkCount);
      ownsZK = true;
    }
    if (zkHelper != null) {
      zkConnect = zkHelper.getConnectionString();

      // When using ZK, we need to pass in the connection property as
      // a config property. But, we can only do that if we are passing
      // in config properties defined at run time. Drill does not allow
      // combining locally-set properties and a config file: it is one
      // or the other.

      if (builder.configProps == null) {
        throw new IllegalArgumentException("Cannot specify a local ZK while using an external config file.");
      }
      builder.configProperty(ExecConstants.ZK_CONNECTION, zkConnect);

      // Forced to disable this, because currently we leak memory which is a known issue for query cancellations.
      // Setting this causes unit tests to fail.
      builder.configProperty(ExecConstants.RETURN_ERROR_FOR_FAILURE_IN_CANCELLED_FRAGMENTS, true);
    }
  }

  private void createConfig(FixtureBuilder builder) throws Exception {

    // Create a config
    // Because of the way DrillConfig works, we can set the ZK
    // connection string only if a property set is provided.

    if (builder.configResource != null) {
      config = DrillConfig.create(builder.configResource);
    } else if (builder.configProps != null) {
      config = configProperties(builder.configProps);
    } else {
      throw new IllegalStateException("Configuration was not provided.");
    }

    if (builder.usingZk) {
      // Distribute drillbit using ZK (in-process or external)

      serviceSet = null;
      usesZk = true;
      isLocal = false;
    } else {
      // Embedded Drillbit.

      serviceSet = RemoteServiceSet.getLocalServiceSet();
      isLocal = true;
    }
  }

  private void startDrillbits(FixtureBuilder builder) throws Exception {
//    // Ensure that Drill uses the log directory determined here rather than
//    // it's hard-coded defaults. WIP: seems to be needed some times but
//    // not others.
//
//    String logDir = null;
//    if (builder.tempDir != null) {
//      logDir = builder.tempDir.getAbsolutePath();
//    }
//    if (logDir == null) {
//      logDir = config.getString(ExecConstants.DRILL_TMP_DIR);
//      if (logDir != null) {
//        logDir += "/drill/log";
//      }
//    }
//    if (logDir == null) {
//      logDir = "/tmp/drill";
//    }
//    new File(logDir).mkdirs();
//    System.setProperty("drill.log-dir", logDir);

    dfsTestTempDir = makeTempDir("dfs-test");

    // Clean up any files that may have been left from the
    // last run.

    preserveLocalFiles = builder.preserveLocalFiles;
    removeLocalFiles();

    // Start the Drillbits.

    Preconditions.checkArgument(builder.bitCount > 0);
    int bitCount = builder.bitCount;
    for (int i = 0; i < bitCount; i++) {
      Drillbit bit = new Drillbit(config, serviceSet);
      bit.run();

      // Bit name and registration.

      String name;
      if (builder.bitNames != null && i < builder.bitNames.length) {
        name = builder.bitNames[i];
      } else {

        // Name the Drillbit by default. Most tests use one Drillbit,
        // so make the name simple: "drillbit." Only add a numeric suffix
        // when the test creates multiple bits.

        if (bitCount == 1) {
          name = DEFAULT_BIT_NAME;
        } else {
          name = DEFAULT_BIT_NAME + Integer.toString(i + 1);
        }
      }
      bits.put(name, bit);

      // Remember the first Drillbit, this is the default one returned from
      // drillbit().

      if (i == 0) {
        defaultDrillbit = bit;
      }
      configureStoragePlugins(bit);
    }
  }

  private void configureStoragePlugins(Drillbit bit) throws Exception {
    // Create the dfs_test name space

    @SuppressWarnings("resource")
    final StoragePluginRegistry pluginRegistry = bit.getContext().getStorage();
    TestUtilities.updateDfsTestTmpSchemaLocation(pluginRegistry, dfsTestTempDir.getAbsolutePath());
    TestUtilities.makeDfsTmpSchemaImmutable(pluginRegistry);

    // Create the mock data plugin

    MockStorageEngineConfig config = MockStorageEngineConfig.INSTANCE;
    @SuppressWarnings("resource")
    MockStorageEngine plugin = new MockStorageEngine(
        MockStorageEngineConfig.INSTANCE, bit.getContext(),
        MockStorageEngineConfig.NAME);
    ((StoragePluginRegistryImpl) pluginRegistry).definePlugin(MockStorageEngineConfig.NAME, config, plugin);
  }

  private void applyOptions(FixtureBuilder builder) throws Exception {

    // Apply system options

    if (builder.systemOptions != null) {
      for (FixtureBuilder.RuntimeOption option : builder.systemOptions) {
        clientFixture().alterSystem(option.key, option.value);
      }
    }

    // Apply session options.

    if (builder.sessionOptions != null) {
      for (FixtureBuilder.RuntimeOption option : builder.sessionOptions) {
        clientFixture().alterSession(option.key, option.value);
      }
    }
  }

  private DrillConfig configProperties(Properties configProps) {
    Properties stringProps = new Properties();
    Properties collectionProps = new Properties();

    // Filter out the collection type configs and other configs which can be converted to string.
    for(Entry<Object, Object> entry : configProps.entrySet()) {
      if(entry.getValue() instanceof Collection<?>) {
        collectionProps.put(entry.getKey(), entry.getValue());
      } else {
        stringProps.setProperty(entry.getKey().toString(), entry.getValue().toString());
      }
    }

    // First create a DrillConfig based on string properties.
    Config drillConfig = DrillConfig.create(stringProps);

    // Then add the collection properties inside the DrillConfig. Below call to withValue returns
    // a new reference. Considering mostly properties will be of string type, doing this
    // later will be less expensive as compared to doing it for all the properties.
    for(Entry<Object, Object> entry : collectionProps.entrySet()) {
      drillConfig = drillConfig.withValue(entry.getKey().toString(),
        ConfigValueFactory.fromAnyRef(entry.getValue()));
    }

    return new DrillConfig(drillConfig, true);
  }

  public Drillbit drillbit() { return defaultDrillbit; }
  public Drillbit drillbit(String name) { return bits.get(name); }
  public Collection<Drillbit> drillbits() { return bits.values(); }
  public RemoteServiceSet serviceSet() { return serviceSet; }
  public BufferAllocator allocator() { return allocator; }
  public DrillConfig config() { return config; }
  public File getDfsTestTmpDir() { return dfsTestTempDir; }

  public ClientFixture.ClientBuilder clientBuilder() {
    return new ClientFixture.ClientBuilder(this);
  }

  public ClientFixture clientFixture() {
    if (clients.isEmpty()) {
      clientBuilder().build();
    }
    return clients.get(0);
  }

  public DrillClient client() {
    return clientFixture().client();
  }

  /**
   * Close the clients, Drillbits, allocator and
   * Zookeeper. Checks for exceptions. If an exception occurs,
   * continues closing, suppresses subsequent exceptions, and
   * throws the first exception at completion of close. This allows
   * the test code to detect any state corruption which only shows
   * itself when shutting down resources (memory leaks, for example.)
   */

  @Override
  public void close() throws Exception {
    Exception ex = null;

    // Close clients. Clients remove themselves from the client
    // list.

    while (!clients.isEmpty()) {
      ex = safeClose(clients.get(0), ex);
    }

    for (Drillbit bit : drillbits()) {
      ex = safeClose(bit, ex);
    }
    bits.clear();
    ex = safeClose(serviceSet, ex);
    serviceSet = null;
    ex = safeClose(allocator, ex);
    allocator = null;
    if (zkHelper != null && ownsZK) {
      try {
        zkHelper.stopZookeeper();
      } catch (Exception e) {
        ex = ex == null ? e : ex;
      }
    }
    zkHelper = null;

    // Delete any local files, if we wrote to the local
    // persistent store. But, leave the files if the user wants
    // to review them, for debugging, say. Note that, even if the
    // files are preserved here, they will be removed when the
    // next cluster fixture starts, else the CTTAS initialization
    // will fail.

    if (! preserveLocalFiles) {
      try {
        removeLocalFiles();
      } catch (Exception e) {
        ex = ex == null ? e : ex;
      }
    }

    // Remove temporary directories created for this cluster session.

    try {
      removeTempDirs();
    } catch (Exception e) {
      ex = ex == null ? e : ex;
    }
    if (ex != null) {
      throw ex;
    }
  }

  /**
   * Removes files stored locally in the "local store provider."
   * Required because CTTAS setup fails if these files are left from one
   * run to the next.
   *
   * @throws IOException if a directory cannot be deleted
   */

  private void removeLocalFiles() throws IOException {

    // Don't delete if this is not a local Drillbit.

    if (! isLocal) {
      return;
    }

    // Remove the local files if they exist.

    String localStoreLocation = config.getString(ExecConstants.SYS_STORE_PROVIDER_LOCAL_PATH);
    removeDir(new File(localStoreLocation));
  }

  private void removeTempDirs() throws IOException {
    IOException ex = null;
    for (File dir : tempDirs) {
      try {
        removeDir(dir);
      } catch (IOException e) {
        ex = ex == null ? e : ex;
      }
    }
    if (ex != null) {
      throw ex;
    }
  }

  public void removeDir(File dir) throws IOException {
    if (dir.exists()) {
      FileUtils.deleteDirectory(dir);
    }
  }

  /**
   * Close a resource, suppressing the exception, and keeping
   * only the first exception that may occur. We assume that only
   * the first is useful, any others are probably down-stream effects
   * of that first one.
   *
   * @param item Item to be closed
   * @param ex exception to be returned if none thrown here
   * @return the first exception found
   */
  private Exception safeClose(AutoCloseable item, Exception ex) {
    try {
      if (item != null) {
        item.close();
      }
    } catch (Exception e) {
      ex = ex == null ? e : ex;
    }
    return ex;
  }

  /**
   * Define a workspace within an existing storage plugin. Useful for
   * pointing to local file system files outside the Drill source tree.
   *
   * @param pluginName name of the plugin like "dfs" or "dfs_test".
   * @param schemaName name of the new schema
   * @param path directory location (usually local)
   * @param defaultFormat default format for files in the schema
   */

  public void defineWorkspace(String pluginName, String schemaName, String path,
      String defaultFormat) {
    for (Drillbit bit : drillbits()) {
      try {
        defineWorkspace(bit, pluginName, schemaName, path, defaultFormat);
      } catch (ExecutionSetupException e) {
        // This functionality is supposed to work in tests. Change
        // exception to unchecked to make test code simpler.

        throw new IllegalStateException(e);
      }
    }
  }

  public static void defineWorkspace(Drillbit drillbit, String pluginName,
      String schemaName, String path, String defaultFormat)
      throws ExecutionSetupException {
    @SuppressWarnings("resource")
    final StoragePluginRegistry pluginRegistry = drillbit.getContext().getStorage();
    @SuppressWarnings("resource")
    final FileSystemPlugin plugin = (FileSystemPlugin) pluginRegistry.getPlugin(pluginName);
    final FileSystemConfig pluginConfig = (FileSystemConfig) plugin.getConfig();
    final WorkspaceConfig newTmpWSConfig = new WorkspaceConfig(path, true, defaultFormat);

    pluginConfig.workspaces.remove(schemaName);
    pluginConfig.workspaces.put(schemaName, newTmpWSConfig);

    pluginRegistry.createOrUpdate(pluginName, pluginConfig, true);
  }

  public static final String EXPLAIN_PLAN_TEXT = "text";
  public static final String EXPLAIN_PLAN_JSON = "json";

  public static FixtureBuilder builder() {
     return new FixtureBuilder()
         .configProps(FixtureBuilder.defaultProps())
         .sessionOption(ExecConstants.MAX_WIDTH_PER_NODE_KEY, MAX_WIDTH_PER_NODE)
         ;
  }

  /**
   * Return a cluster builder without any of the usual defaults. Use
   * this only for special cases. Your code is responsible for all the
   * odd bits that must be set to get the setup right. See
   * {@link ClusterFixture#TEST_CONFIGURATIONS} for details. Note that
   * you are often better off using the defaults, then replacing selected
   * properties with the values you prefer.
   *
   * @return a fixture builder with no default properties set
   */

  public static FixtureBuilder bareBuilder() {
    return new FixtureBuilder();
  }

  /**
   * Shim class to allow the {@link TestBuilder} class to work with the
   * cluster fixture.
   */

  public static class FixtureTestServices implements TestServices {

    private ClientFixture client;

    public FixtureTestServices(ClientFixture client) {
      this.client = client;
    }

    @Override
    public BufferAllocator allocator() {
      return client.allocator();
    }

    @Override
    public void test(String query) throws Exception {
      client.runQueries(query);
    }

    @Override
    public List<QueryDataBatch> testRunAndReturn(QueryType type, Object query)
        throws Exception {
      return client.queryBuilder().query(type, (String) query).results();
    }
  }

  /**
   * Return a cluster fixture built with standard options. This is a short-cut
   * for simple tests that don't need special setup.
   *
   * @return a cluster fixture with standard options
   * @throws Exception if something goes wrong
   */
  public static ClusterFixture standardCluster() {
    return builder().build();
  }

  /**
   * Convert a Java object (typically a boxed scalar) to a string
   * for use in SQL. Quotes strings but just converts others to
   * string format.
   *
   * @param value the value to encode
   * @return the SQL-acceptable string equivalent
   */

  public static String stringify(Object value) {
    if (value instanceof String) {
      return "'" + (String) value + "'";
    } else {
      return value.toString();
    }
  }

  public static String getResource(String resource) throws IOException {
    // Unlike the Java routines, Guava does not like a leading slash.

    final URL url = Resources.getResource(trimSlash(resource));
    if (url == null) {
      throw new IOException(
          String.format("Unable to find resource %s.", resource));
    }
    return Resources.toString(url, Charsets.UTF_8);
  }

  /**
   * Load a resource file, returning the resource as a string.
   * "Hides" the checked exception as unchecked, which is fine
   * in a test as the unchecked exception will fail the test
   * without unnecessary error fiddling.
   *
   * @param resource path to the resource
   * @return the resource contents as a string
   */

  public static String loadResource(String resource) {
    try {
      return getResource(resource);
    } catch (IOException e) {
      throw new IllegalStateException("Resource not found: " + resource, e);
    }
  }

  /**
   * Guava likes paths to resources without an initial slash, the JDK
   * needs a slash. Normalize the path when needed.
   *
   * @param path resource path with optional leading slash
   * @return same path without the leading slash
   */

  public static String trimSlash(String path) {
    if (path == null) {
      return path;
    } else if (path.startsWith("/")) {
      return path.substring(1);
    } else {
      return path;
    }
  }

  /**
   * Create a temp directory to store the given <i>dirName</i>. Directory will
   * be deleted on exit. Directory is created if it does not exist.
   *
   * @param dirName directory name
   * @return Full path including temp parent directory and given directory name.
   */

  public static File getTempDir(final String dirName) {
    final File dir = Files.createTempDir();
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        FileUtils.deleteQuietly(dir);
      }
    });
    File tempDir = new File(dir, dirName);
    tempDir.mkdirs();
    return tempDir;
  }

  /**
   * Create a temporary directory which will be removed when the
   * cluster closes.
   *
   * @param dirName the name of the leaf directory
   * @return the path to the temporary directory which is usually
   * under the temporary directory structure for this machine
   */

  public File makeTempDir(final String dirName) {
    File dir = getTempDir(dirName);
    tempDirs.add(dir);
    return dir;
  }

  /**
   * Create a temporary data directory which will be removed when the
   * cluster closes, and register it as a "dfs" name space.
   *
   * @param key the name to use for the directory and the name space.
   * Access the directory as "dfs.<key>".
   * @param defaultFormat default storage format for the workspace
   * @return location of the directory which can be used to create
   * temporary input files
   */

  public File makeDataDir(String key, String defaultFormat) {
    File dir = makeTempDir(key);
    defineWorkspace("dfs", key, dir.getAbsolutePath(), defaultFormat);
    return dir;
  }

  public File getDrillTempDir() {
    return new File(config.getString(ExecConstants.SYS_STORE_PROVIDER_LOCAL_PATH));
  }

  public boolean usesZK() {
    return usesZk;
  }

  /**
   * Returns the directory that holds query profiles. Valid only for an
   * embedded Drillbit with local cluster coordinator &ndash; the normal
   * case for unit tests.
   *
   * @return query profile directory
   */

  public File getProfileDir() {
    File baseDir;
    if (usesZk) {
      baseDir = new File(config.getString(ZookeeperPersistentStoreProvider.DRILL_EXEC_SYS_STORE_PROVIDER_ZK_BLOBROOT));
    } else {
      baseDir = getDrillTempDir();
    }
    return new File(baseDir, "profiles");
  }
}
