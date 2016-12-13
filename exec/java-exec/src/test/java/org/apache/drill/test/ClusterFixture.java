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

import org.apache.commons.io.FileUtils;
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
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ClientFixture.class);
  public static final String ENABLE_FULL_CACHE = "drill.exec.test.use-full-cache";
  public static final int MAX_WIDTH_PER_NODE = 2;

  @SuppressWarnings("serial")
  public static final Properties TEST_CONFIGURATIONS = new Properties() {
    {
      // Properties here mimic those in drill-root/pom.xml, Surefire plugin
      // configuration. They allow tests to run successfully in Eclipse.

      put(ExecConstants.SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE, false);
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
    }
  };

  public static final String DEFAULT_BIT_NAME = "drillbit";

  private DrillConfig config;
  private Map<String,Drillbit> bits = new HashMap<>();
  private Drillbit defaultDrillbit;
  private BufferAllocator allocator;
  private boolean ownsZK;
  private ZookeeperHelper zkHelper;
  private RemoteServiceSet serviceSet;
  private String dfsTestTmpSchemaLocation;
  protected List<ClientFixture> clients = new ArrayList<>();

  protected ClusterFixture(FixtureBuilder  builder) throws Exception {

    // Start ZK if requested.

    if (builder.zkHelper != null) {
      zkHelper = builder.zkHelper;
      ownsZK = false;
    } else if (builder.zkCount > 0) {
      zkHelper = new ZookeeperHelper(true);
      zkHelper.startZookeeper(builder.zkCount);
      ownsZK = true;
    }

    // Create a config
    // Because of the way DrillConfig works, we can set the ZK
    // connection string only if a property set is provided.

    if (builder.configResource != null) {
      config = DrillConfig.create(builder.configResource);
    } else if (builder.configProps != null) {
      config = DrillConfig.create(configProperties(builder.configProps));
    } else {
      config = DrillConfig.create(configProperties(TEST_CONFIGURATIONS));
    }

    // Not quite sure what this is, but some tests seem to use it.

    if (builder.enableFullCache ||
        (config.hasPath(ENABLE_FULL_CACHE) && config.getBoolean(ENABLE_FULL_CACHE))) {
      serviceSet = RemoteServiceSet.getServiceSetWithFullCache(config, allocator);
    } else {
      serviceSet = RemoteServiceSet.getLocalServiceSet();
    }

    dfsTestTmpSchemaLocation = TestUtilities.createTempDir();

    Preconditions.checkArgument(builder.bitCount > 0);
    int bitCount = builder.bitCount;
    for (int i = 0; i < bitCount; i++) {
      @SuppressWarnings("resource")
      Drillbit bit = new Drillbit(config, serviceSet);
      bit.run();

      // Create the dfs_test name space

      @SuppressWarnings("resource")
      final StoragePluginRegistry pluginRegistry = bit.getContext().getStorage();
      TestUtilities.updateDfsTestTmpSchemaLocation(pluginRegistry, dfsTestTmpSchemaLocation);
      TestUtilities.makeDfsTmpSchemaImmutable(pluginRegistry);

      // Create the mock data plugin
      // (Disabled until DRILL-5152 is committed.)

      MockStorageEngineConfig config = MockStorageEngineConfig.INSTANCE;
      @SuppressWarnings("resource")
      MockStorageEngine plugin = new MockStorageEngine(
          MockStorageEngineConfig.INSTANCE, bit.getContext(),
          MockStorageEngineConfig.NAME);
      ((StoragePluginRegistryImpl) pluginRegistry)
          .definePlugin(MockStorageEngineConfig.NAME, config, plugin);

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
          name = DEFAULT_BIT_NAME + Integer.toString(i+1);
        }
      }
      bits.put(name, bit);

      // Remember the first Drillbit, this is the default one returned from
      // drillbit().

      if (i == 0) {
        defaultDrillbit = bit;
      }
    }

    // Some operations need an allocator.

    allocator = RootAllocatorFactory.newRoot(config);

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

  private Properties configProperties(Properties configProps) {
    Properties effectiveProps = new Properties();
    for (Entry<Object, Object> entry : configProps.entrySet()) {
      effectiveProps.put(entry.getKey(), entry.getValue().toString());
    }
    if (zkHelper != null) {
      effectiveProps.put(ExecConstants.ZK_CONNECTION, zkHelper.getConfig().getString(ExecConstants.ZK_CONNECTION));
    }
    return effectiveProps;
  }

  public Drillbit drillbit() { return defaultDrillbit; }
  public Drillbit drillbit(String name) { return bits.get(name); }
  public Collection<Drillbit> drillbits() { return bits.values(); }
  public RemoteServiceSet serviceSet() { return serviceSet; }
  public BufferAllocator allocator() { return allocator; }
  public DrillConfig config() { return config; }

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
   * Close the clients, drillbits, allocator and
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
    if (ex != null) {
      throw ex;
    }
  }

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

  public void defineWorkspace(String pluginName, String schemaName, String path,
      String defaultFormat) throws ExecutionSetupException {
    for (Drillbit bit : drillbits()) {
      defineWorkspace(bit, pluginName, schemaName, path, defaultFormat);
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

  public static FixtureBuilder bareBuilder() {
    return new FixtureBuilder();
  }

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

  public static ClusterFixture standardCluster() throws Exception {
    return builder().build();
  }

  static String stringify(Object value) {
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
      throw new IOException(String.format("Unable to find resource %s.", resource));
    }
    return Resources.toString(url, Charsets.UTF_8);
  }

  public static String loadResource(String resource) {
    try {
      return getResource(resource);
    } catch (IOException e) {
      throw new IllegalStateException("Resource not found: " + resource, e);
    }
  }

  static String trimSlash(String path) {
    if (path == null) {
      return path;
    } else if (path.startsWith("/")) {
      return path.substring(1);
    } else {
      return path;
    }
  }

  /**
   * Create a temp directory to store the given <i>dirName</i>.
   * Directory will be deleted on exit. Directory is created if it does
   * not exist.
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
}
