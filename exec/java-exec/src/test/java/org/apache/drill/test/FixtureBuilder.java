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
 ******************************************************************************/
package org.apache.drill.test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ZookeeperHelper;

/**
 * Build a Drillbit and client with the options provided. The simplest
 * builder starts an embedded Drillbit, with the "dfs_test" name space,
 * a max width (parallelization) of 2.
 */

public class FixtureBuilder {

  public static class RuntimeOption {
    public String key;
    public Object value;

    public RuntimeOption(String key, Object value) {
      this.key = key;
      this.value = value;
    }
  }

  // Values in the drill-module.conf file for values that are customized
  // in the defaults.

  public static final int DEFAULT_ZK_REFRESH = 500; // ms
  public static final int DEFAULT_SERVER_RPC_THREADS = 10;
  public static final int DEFAULT_SCAN_THREADS = 8;

  protected ConfigBuilder configBuilder = new ConfigBuilder();
  protected List<RuntimeOption> sessionOptions;
  protected List<RuntimeOption> systemOptions;
  protected int bitCount = 1;
  protected String bitNames[];
  protected int localZkCount;
  protected ZookeeperHelper zkHelper;
  protected boolean usingZk;
  protected File tempDir;
  protected boolean preserveLocalFiles;
  protected Properties clientProps;

  /**
   * The configuration builder which this fixture builder uses.
   * @return the configuration builder for use in setting "advanced"
   * configuration options.
   */

  public ConfigBuilder configBuilder() { return configBuilder; }

  /**
   * Use the given configuration file, stored as a resource, to start the
   * embedded Drillbit. Note that the resource file should have the two
   * following settings to work as a test:
   * <pre><code>
   * drill.exec.sys.store.provider.local.write : false,
   * drill.exec.http.enabled : false
   * </code></pre>
   * It may be more convenient to add your settings to the default
   * config settings with {@link #configProperty(String, Object)}.
   * @param configResource path to the file that contains the
   * config file to be read
   * @return this builder
   * @see {@link #configProperty(String, Object)}
   */

  public FixtureBuilder configResource(String configResource) {

    // TypeSafe gets unhappy about a leading slash, but other functions
    // require it. Silently discard the leading slash if given to
    // preserve the test writer's sanity.

    configBuilder.resource(ClusterFixture.trimSlash(configResource));
    return this;
  }

  /**
   * Add an additional boot-time property for the embedded Drillbit.
   * @param key config property name
   * @param value property value
   * @return this builder
   */

  public FixtureBuilder configProperty(String key, Object value) {
    configBuilder.put(key, value.toString());
    return this;
  }

  /**
   * Add an additional property for the client connection URL. Convert all the values into
   * String type.
   * @param key config property name
   * @param value property value
   * @return this builder
   */
  public FixtureBuilder configClientProperty(String key, Object value) {
    if (clientProps == null) {
      clientProps = new Properties();
    }
    clientProps.put(key, value.toString());
    return this;
  }

   /**
   * Provide a session option to be set once the Drillbit
   * is started.
   *
   * @param key the name of the session option
   * @param value the value of the session option
   * @return this builder
   * @see {@link ClusterFixture#alterSession(String, Object)}
   */

  public FixtureBuilder sessionOption(String key, Object value) {
    if (sessionOptions == null) {
      sessionOptions = new ArrayList<>();
    }
    sessionOptions.add(new RuntimeOption(key, value));
    return this;
  }

  /**
   * Provide a system option to be set once the Drillbit
   * is started.
   *
   * @param key the name of the system option
   * @param value the value of the system option
   * @return this builder
   * @see {@link ClusterFixture#alterSystem(String, Object)}
   */

  public FixtureBuilder systemOption(String key, Object value) {
    if (systemOptions == null) {
      systemOptions = new ArrayList<>();
    }
    systemOptions.add(new RuntimeOption(key, value));
    return this;
  }

  /**
   * Set the maximum parallelization (max width per node). Defaults
   * to 2.
   *
   * @param n the "max width per node" parallelization option.
   * @return this builder
   */
  public FixtureBuilder maxParallelization(int n) {
    return sessionOption(ExecConstants.MAX_WIDTH_PER_NODE_KEY, n);
  }

  /**
   * The number of Drillbits to start in the cluster.
   *
   * @param n the desired cluster size
   * @return this builder
   */
  public FixtureBuilder clusterSize(int n) {
    bitCount = n;
    bitNames = null;
    return this;
  }

  /**
   * Define a cluster by providing names to the Drillbits.
   * The cluster size is the same as the number of names provided.
   *
   * @param bitNames array of (unique) Drillbit names
   * @return this builder
   */
  public FixtureBuilder withBits(String bitNames[]) {
    this.bitNames = bitNames;
    bitCount = bitNames.length;
    return this;
  }

  /**
   * By default the embedded Drillbits use an in-memory cluster coordinator.
   * Use this option to start an in-memory ZK instance to coordinate the
   * Drillbits.
   * @return this builder
   */
  public FixtureBuilder withLocalZk() {
    return withLocalZk(1);
  }

  public FixtureBuilder withLocalZk(int count) {
    localZkCount = count;
    usingZk = true;

    // Using ZK. Turn refresh wait back on.

    return configProperty(ExecConstants.ZK_REFRESH, DEFAULT_ZK_REFRESH);
  }

  public FixtureBuilder withRemoteZk(String connStr) {
    usingZk = true;
    return configProperty(ExecConstants.ZK_CONNECTION, connStr);
  }

  /**
   * Run the cluster using a Zookeeper started externally. Use this if
   * multiple tests start a cluster: allows ZK to be started once for
   * the entire suite rather than once per test case.
   *
   * @param zk the global Zookeeper to use
   * @return this builder
   */
  public FixtureBuilder withZk(ZookeeperHelper zk) {
    zkHelper = zk;
    usingZk = true;

    // Using ZK. Turn refresh wait back on.

    configProperty(ExecConstants.ZK_REFRESH, DEFAULT_ZK_REFRESH);
    return this;
  }

  public FixtureBuilder tempDir(File path) {
    this.tempDir = path;
    return this;
  }

  /**
   * Starting with the addition of the CTTAS feature, a Drillbit will
   * not restart unless we delete all local storage files before
   * starting the Drillbit again. In particular, the stored copies
   * of the storage plugin configs cause the temporary workspace
   * check to fail. Normally the cluster fixture cleans up files
   * both before starting and after shutting down the cluster. Set this
   * option to preserve files after shutdown, perhaps to debug the
   * contents.
   * <p>
   * This clean-up is needed only if we enable local storage writes
   * (which we must do, unfortunately, to capture and analyze
   * storage profiles.)
   *
   * @return this builder
   */

  public FixtureBuilder keepLocalFiles() {
    preserveLocalFiles = true;
    return this;
  }

  /**
   * Enable saving of query profiles. The only way to save them is
   * to enable local store provider writes, which also saves the
   * storage plugin configs. Doing so causes the CTTAS feature to
   * fail on the next run, so the test fixture deletes all local
   * files on start and close, unless
   * {@link #keepLocalFiles()} is set.
   *
   * @return this builder
   */

  public FixtureBuilder saveProfiles() {
    configProperty(ExecConstants.SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE, true);
    systemOption(ExecConstants.ENABLE_QUERY_PROFILE_OPTION, true);
    systemOption(ExecConstants.QUERY_PROFILE_DEBUG_OPTION, true);
    return this;
  }

  /**
   * Create the embedded Drillbit and client, applying the options set
   * in the builder. Best to use this in a try-with-resources block:
   * <pre><code>
   * FixtureBuilder builder = ClientFixture.newBuilder()
   *   .property(...)
   *   .sessionOption(...)
   *   ;
   * try (ClusterFixture cluster = builder.build();
   *      ClientFixture client = cluster.clientFixture()) {
   *   // Do the test
   * }
   * </code></pre>
   * Note that you use a single cluster fixture to create any number of
   * drillbits in your cluster. If you want multiple clients, create the
   * first as above, the others (or even the first) using the
   * {@link ClusterFixture#clientBuilder()}. Using the client builder
   * also lets you set client-side options in the rare cases that you
   * need them.
   *
   * @return
   */

  public ClusterFixture build() {
    return new ClusterFixture(this);
  }
}
