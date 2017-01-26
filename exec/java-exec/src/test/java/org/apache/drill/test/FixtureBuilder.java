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

  public static Properties defaultProps() {
    Properties props = new Properties();
    props.putAll(ClusterFixture.TEST_CONFIGURATIONS);
    return props;
  }

  String configResource;
  Properties configProps;
  boolean enableFullCache;
  List<RuntimeOption> sessionOptions;
  List<RuntimeOption> systemOptions;
  int bitCount = 1;
  String bitNames[];
  int zkCount;
  ZookeeperHelper zkHelper;

  /**
   * Use the given configuration properties to start the embedded Drillbit.
   * @param configProps a collection of config properties
   * @return this builder
   * @see {@link #configProperty(String, Object)}
   */

  public FixtureBuilder configProps(Properties configProps) {
    this.configProps = configProps;
    return this;
  }

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

    this.configResource = ClusterFixture.trimSlash(configResource);
    return this;
  }

  /**
   * Add an additional boot-time property for the embedded Drillbit.
   * @param key config property name
   * @param value property value
   * @return this builder
   */

  public FixtureBuilder configProperty(String key, Object value) {
    if (configProps == null) {
      configProps = defaultProps();
    }
    configProps.put(key, value.toString());
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

  public FixtureBuilder enableFullCache() {
    enableFullCache = true;
    return this;
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
  public FixtureBuilder withZk() {
    return withZk(1);
  }

  public FixtureBuilder withZk(int count) {
    zkCount = count;

    // Using ZK. Turn refresh wait back on.

    configProperty(ExecConstants.ZK_REFRESH, DEFAULT_ZK_REFRESH);
    return this;
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

    // Using ZK. Turn refresh wait back on.

    configProperty(ExecConstants.ZK_REFRESH, DEFAULT_ZK_REFRESH);
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
   * @throws Exception
   */
  public ClusterFixture build() throws Exception {
    return new ClusterFixture(this);
  }
}