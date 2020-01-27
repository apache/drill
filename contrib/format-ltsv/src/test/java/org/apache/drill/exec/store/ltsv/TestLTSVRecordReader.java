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
package org.apache.drill.exec.store.ltsv;

import org.apache.drill.categories.RowSetTests;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.ExecTest;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.physical.rowSet.RowSetBuilder;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.dfs.ZipCodec;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.QueryBuilder;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category(RowSetTests.class)
public class TestLTSVRecordReader extends ClusterTest {

  @BeforeClass
  public static void setup() throws Exception {
    ClusterTest.startCluster(ClusterFixture.builder(dirTestWatcher));

    LTSVFormatPluginConfig formatConfig = new LTSVFormatPluginConfig();
    cluster.defineFormat("cp", "ltsv", formatConfig);

    // Needed for compressed file unit test
    dirTestWatcher.copyResourceToRoot(Paths.get("ltsv/"));
  }

  @Test
  public void testWildcard() throws Exception {
    String sql = "SELECT * FROM cp.`ltsv/simple.ltsv`";
    QueryBuilder q = client.queryBuilder().sql(sql);
    RowSet results = q.rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .addNullable("host",  TypeProtos.MinorType.VARCHAR)
      .addNullable("forwardedfor",  TypeProtos.MinorType.VARCHAR)
      .addNullable("req",  TypeProtos.MinorType.VARCHAR)
      .addNullable("status",  TypeProtos.MinorType.VARCHAR)
      .addNullable("size",  TypeProtos.MinorType.VARCHAR)
      .addNullable("referer",  TypeProtos.MinorType.VARCHAR)
      .addNullable("ua",  TypeProtos.MinorType.VARCHAR)
      .addNullable("reqtime",  TypeProtos.MinorType.VARCHAR)
      .addNullable("apptime",  TypeProtos.MinorType.VARCHAR)
      .addNullable("vhost",  TypeProtos.MinorType.VARCHAR)
      .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow("xxx.xxx.xxx.xxx", "-", "GET /v1/xxx HTTP/1.1", "200", "4968", "-", "Java/1.8.0_131", "2.532", "2.532", "api.example.com")
      .addRow("xxx.xxx.xxx.xxx", "-", "GET /v1/yyy HTTP/1.1", "200", "412", "-", "Java/1.8.0_201", "3.580", "3.580", "api.example.com")
      .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testSelectColumns() throws Exception {
    String sql = "SELECT ua, reqtime FROM cp.`ltsv/simple.ltsv`";

    QueryBuilder q = client.queryBuilder().sql(sql);
    RowSet results = q.rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .addNullable("ua",  TypeProtos.MinorType.VARCHAR)
      .addNullable("reqtime",  TypeProtos.MinorType.VARCHAR)
      .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow("Java/1.8.0_131", "2.532")
      .addRow("Java/1.8.0_201", "3.580")
      .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testQueryWithConditions() throws Exception {
    String sql = "SELECT * FROM cp.`ltsv/simple.ltsv` WHERE reqtime > 3.0";

    QueryBuilder q = client.queryBuilder().sql(sql);
    RowSet results = q.rowSet();
    TupleMetadata expectedSchema = new SchemaBuilder()
      .addNullable("host",  TypeProtos.MinorType.VARCHAR)
      .addNullable("forwardedfor",  TypeProtos.MinorType.VARCHAR)
      .addNullable("req",  TypeProtos.MinorType.VARCHAR)
      .addNullable("status",  TypeProtos.MinorType.VARCHAR)
      .addNullable("size",  TypeProtos.MinorType.VARCHAR)
      .addNullable("referer",  TypeProtos.MinorType.VARCHAR)
      .addNullable("ua",  TypeProtos.MinorType.VARCHAR)
      .addNullable("reqtime",  TypeProtos.MinorType.VARCHAR)
      .addNullable("apptime",  TypeProtos.MinorType.VARCHAR)
      .addNullable("vhost",  TypeProtos.MinorType.VARCHAR)
      .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow("xxx.xxx.xxx.xxx", "-", "GET /v1/yyy HTTP/1.1", "200", "412", "-", "Java/1.8.0_201", "3.580", "3.580", "api.example.com")
      .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testSkipEmptyLines() throws Exception {
    assertEquals(2, queryBuilder().sql("SELECT * FROM cp.`ltsv/emptylines.ltsv`").run().recordCount());
  }

  @Test
  public void testReadException() throws Exception {
    try {
      run("SELECT * FROM cp.`ltsv/invalid.ltsv`");
      fail();
    } catch (UserException e) {
      assertEquals(UserBitShared.DrillPBError.ErrorType.DATA_READ, e.getErrorType());
      assertTrue(e.getMessage().contains("Invalid LTSV format at line 1: time:30/Nov/2016:00:55:08 +0900"));
    }
  }

  @Test
  public void testSerDe() throws Exception {
    String sql = "SELECT COUNT(*) as cnt FROM cp.`ltsv/simple.ltsv`";
    String plan = queryBuilder().sql(sql).explainJson();
    long cnt = queryBuilder().physical(plan).singletonLong();
    assertEquals("Counts should match",2L, cnt);
  }

  @Test
  public void testSelectColumnWithCompressedFile() throws Exception {
    generateCompressedFile("ltsv/simple.ltsv", "zip", "ltsv/simple.ltsv.zip" );

    String sql = "SELECT ua, reqtime FROM dfs.`ltsv/simple.ltsv.zip`";

    QueryBuilder q = client.queryBuilder().sql(sql);
    RowSet results = q.rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .addNullable("ua",  TypeProtos.MinorType.VARCHAR)
      .addNullable("reqtime",  TypeProtos.MinorType.VARCHAR)
      .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow("Java/1.8.0_131", "2.532")
      .addRow("Java/1.8.0_201", "3.580")
      .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  private void generateCompressedFile(String fileName, String codecName, String outFileName) throws IOException {
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
