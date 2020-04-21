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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.drill.common.AutoCloseables;
import org.apache.drill.exec.ExecTest;
import org.apache.drill.exec.store.dfs.ZipCodec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.junit.AfterClass;
import org.junit.ClassRule;

import java.io.IOException;

import static org.junit.Assert.assertNotNull;

/**
 * Base class for tests that use a single cluster fixture for a set of
 * tests. Extend your test case directly from {@link DrillTest} if you
 * need to start up and shut down a cluster multiple times.
 * <p>
 * To create a test with a single cluster config, do the following:
 * <pre><code>
 * public class YourTest extends ClusterTest {
 *   {@literal @}BeforeClass
 *   public static setup( ) throws Exception {
 *     FixtureBuilder builder = ClusterFixture.builder()
 *       // Set options, etc.
 *       ;
 *     startCluster(builder);
 *   }
 *
 *   // Your tests
 * }
 * </code></pre>
 * This class takes care of shutting down the cluster at the end of the test.
 * <p>
 * The simplest possible setup:
 * <pre><code>
 *   {@literal @}BeforeClass
 *   public static setup( ) throws Exception {
 *     startCluster(ClusterFixture.builder( ));
 *   }
 * </code></pre>
 * <p>
 * If you need to start the cluster with different (boot time) configurations,
 * do the following instead:
 * <pre><code>
 * public class YourTest extends DrillTest {
 *   {@literal @}Test
 *   public someTest() throws Exception {
 *     FixtureBuilder builder = ClusterFixture.builder()
 *       // Set options, etc.
 *       ;
 *     try(ClusterFixture cluster = builder.build) {
 *       // Tests here
 *     }
 *   }
 * }
 * </code></pre>
 * The try-with-resources block ensures that the cluster is shut down at
 * the end of each test method.
 */
public class ClusterTest extends DrillTest {

  @ClassRule
  public static final BaseDirTestWatcher dirTestWatcher = new BaseDirTestWatcher();

  protected static ClusterFixture cluster;
  protected static ClientFixture client;

  protected static void startCluster(ClusterFixtureBuilder builder) throws Exception {
    cluster = builder.build();
    client = cluster.clientFixture();
  }

  @AfterClass
  public static void shutdown() throws Exception {
    AutoCloseables.close(client, cluster);
  }

  /**
   * Convenience method when converting classic tests to use the
   * cluster fixture.
   * @return a test builder that works against the cluster fixture
   */
  public TestBuilder testBuilder() {
    return client.testBuilder();
  }

  /**
   * Convenience method when converting classic tests to use the
   * cluster fixture.
   * @return the contents of the resource text file
   */
  public String getFile(String resource) throws IOException {
    return ClusterFixture.getResource(resource);
  }

  public void runAndLog(String sqlQuery) throws Exception {
    client.runQueriesAndLog(sqlQuery);
  }

  public void runAndPrint(String sqlQuery) {
    client.runQueriesAndPrint(sqlQuery);
  }

  public void runAndPrint(String sqlQuery, Object... args) {
    runAndPrint(String.format(sqlQuery, args));
  }

  public static void run(String query, Object... args) throws Exception {
    client.queryBuilder().sql(query, args).run();
  }

  public QueryBuilder queryBuilder( ) {
    return client.queryBuilder();
  }

  /**
   * Generates a compressed version of the file for testing
   * @param fileName Name of the input file
   * @param codecName The desired CODEC to be used.
   * @param outFileName Name of generated compressed file
   * @throws IOException If function cannot generate file, throws IOException
   */
  public void generateCompressedFile(String fileName, String codecName, String outFileName) throws IOException {
    FileSystem fs = ExecTest.getLocalFileSystem();
    Configuration conf = fs.getConf();
    conf.set(CommonConfigurationKeys.IO_COMPRESSION_CODECS_KEY, ZipCodec.class.getCanonicalName());
    CompressionCodecFactory factory = new CompressionCodecFactory(conf);

    CompressionCodec codec = factory.getCodecByName(codecName);
    assertNotNull(codecName + " is not found", codec);

    Path outFile = new Path(dirTestWatcher.getRootDir().getAbsolutePath(), outFileName);
    Path inFile = new Path(dirTestWatcher.getRootDir().getAbsolutePath(), fileName);

    try (InputStream inputStream = new FileInputStream(inFile.toUri().toString());
         OutputStream outputStream = codec.createOutputStream(fs.create(outFile))) {
      IOUtils.copyBytes(inputStream, outputStream, fs.getConf(), false);
    }
  }

}
