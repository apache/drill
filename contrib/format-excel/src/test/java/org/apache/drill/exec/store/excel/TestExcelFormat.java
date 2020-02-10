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

package org.apache.drill.exec.store.excel;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.ExecTest;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.physical.rowSet.RowSetBuilder;
import org.apache.drill.exec.store.dfs.ZipCodec;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.QueryBuilder;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.FileInputStream;
import java.nio.file.Paths;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.drill.categories.RowSetTests;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

@Category(RowSetTests.class)
public class TestExcelFormat extends ClusterTest {

  @BeforeClass
  public static void setup() throws Exception {
    ClusterTest.startCluster(ClusterFixture.builder(dirTestWatcher));

    // Needed for compressed file unit test
    dirTestWatcher.copyResourceToRoot(Paths.get("excel/"));
  }

  @Test
  public void testStarQuery() throws Exception {
    String sql = "SELECT * FROM cp.`excel/test_data.xlsx`";

    testBuilder()
      .sqlQuery(sql)
      .ordered().baselineColumns("id", "first_name", "last_name", "email", "gender", "birthdate", "balance", "order_count", "average_order")
      .baselineValues(1.0, "Cornelia", "Matej", "cmatej0@mtv.com", "Female", "10/31/1974", 735.29, 22.0, 33.42227272727273)
      .baselineValues(2.0, "Nydia", "Heintsch", "nheintsch1@godaddy.com", "Female", "12/10/1966", 784.14, 22.0, 35.64272727272727)
      .baselineValues(3.0, "Waiter", "Sherel", "wsherel2@utexas.edu", "Male", "3/12/1961", 172.36, 17.0, 10.138823529411766)
      .baselineValues(4.0, "Cicely", "Lyver", "clyver3@mysql.com", "Female", "5/4/2000", 987.39, 6.0, 164.565)
      .baselineValues(5.0, "Dorie", "Doe", "ddoe4@spotify.com", "Female", "12/28/1955", 852.48, 17.0, 50.14588235294118)
      .go();
  }

  @Test
  public void testExplicitAllQuery() throws RpcException {
    String sql = "SELECT id, first_name, last_name, email, gender, birthdate, balance, order_count, average_order FROM cp.`excel/test_data.xlsx`";

    QueryBuilder q = client.queryBuilder().sql(sql);
    RowSet results = q.rowSet();
    TupleMetadata expectedSchema = new SchemaBuilder()
      .addNullable("id", TypeProtos.MinorType.FLOAT8)
      .addNullable("first_name", TypeProtos.MinorType.VARCHAR)
      .addNullable("last_name", TypeProtos.MinorType.VARCHAR)
      .addNullable("email", TypeProtos.MinorType.VARCHAR)
      .addNullable("gender", TypeProtos.MinorType.VARCHAR)
      .addNullable("birthdate", TypeProtos.MinorType.VARCHAR)
      .addNullable("balance", TypeProtos.MinorType.FLOAT8)
      .addNullable("order_count", TypeProtos.MinorType.FLOAT8)
      .addNullable("average_order", TypeProtos.MinorType.FLOAT8)
      .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow(1.0, "Cornelia", "Matej", "cmatej0@mtv.com", "Female", "10/31/1974", 735.29, 22.0, 33.42227273)
      .addRow(2.0, "Nydia", "Heintsch", "nheintsch1@godaddy.com", "Female", "12/10/1966", 784.14, 22.0, 35.64272727)
      .addRow(3.0, "Waiter", "Sherel", "wsherel2@utexas.edu", "Male", "3/12/1961", 172.36, 17.0, 10.13882353)
      .addRow(4.0, "Cicely", "Lyver", "clyver3@mysql.com", "Female", "5/4/2000", 987.39, 6.0, 164.565)
      .addRow(5.0, "Dorie", "Doe", "ddoe4@spotify.com", "Female", "12/28/1955", 852.48, 17.0, 50.14588235)
      .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testExplicitSomeQuery() throws RpcException {
    String sql = "SELECT id, first_name, order_count FROM cp.`excel/test_data.xlsx`";

    RowSet results = client.queryBuilder().sql(sql).rowSet();
    TupleMetadata expectedSchema = new SchemaBuilder()
      .addNullable("id", TypeProtos.MinorType.FLOAT8)
      .addNullable("first_name", TypeProtos.MinorType.VARCHAR)
      .addNullable("order_count", TypeProtos.MinorType.FLOAT8)
      .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow(1.0, "Cornelia", 22.0)
      .addRow(2.0, "Nydia", 22.0)
      .addRow(3.0, "Waiter", 17.0)
      .addRow(4.0, "Cicely", 6.0)
      .addRow(5.0, "Dorie", 17.0)
      .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testNonDefaultSheetQuery() throws RpcException {
    String sql = "SELECT * FROM  table(cp.`excel/test_data.xlsx` (type => 'excel', sheetName => 'secondSheet'))";

    RowSet results = client.queryBuilder().sql(sql).rowSet();
    TupleMetadata expectedSchema = new SchemaBuilder()
      .addNullable("event_date", TypeProtos.MinorType.VARCHAR)
      .addNullable("ip_address", TypeProtos.MinorType.VARCHAR)
      .addNullable("user_agent", TypeProtos.MinorType.VARCHAR)
      .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow("2019-02-17 11:21:45", "166.11.144.176", "Mozilla/5.0 (Windows; U; Windows NT 5.1; ru-RU) AppleWebKit/533.19.4 (KHTML, like Gecko) Version/5.0.3 Safari/533.19.4")
        .addRow("2019-03-03 04:10:31", "203.221.176.215", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_2) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/13.0.782.215 Safari/535.1")
        .addRow("2018-04-05 08:17:17", "11.134.119.132", "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/14.0.813.0 Safari/535.1")
        .addRow("2018-12-05 05:36:10", "68.145.168.82", "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.8 (KHTML, like Gecko) Chrome/17.0.940.0 Safari/535.8")
        .addRow("2018-04-01 16:25:18", "21.12.166.184", "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_6; it-it) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27")
        .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testExplicitNonDefaultSheetQuery() throws RpcException {
    String sql = "SELECT event_date, ip_address, user_agent FROM  table(cp.`excel/test_data.xlsx` (type => 'excel', sheetName => 'secondSheet'))";

    RowSet results = client.queryBuilder().sql(sql).rowSet();
    TupleMetadata expectedSchema = new SchemaBuilder()
      .addNullable("event_date", TypeProtos.MinorType.VARCHAR)
      .addNullable("ip_address", TypeProtos.MinorType.VARCHAR)
      .addNullable("user_agent", TypeProtos.MinorType.VARCHAR)
      .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow("2019-02-17 11:21:45", "166.11.144.176", "Mozilla/5.0 (Windows; U; Windows NT 5.1; ru-RU) AppleWebKit/533.19.4 (KHTML, like Gecko) Version/5.0.3 Safari/533.19.4")
      .addRow("2019-03-03 04:10:31", "203.221.176.215", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_2) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/13.0.782.215 Safari/535.1")
      .addRow("2018-04-05 08:17:17", "11.134.119.132", "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/14.0.813.0 Safari/535.1")
      .addRow("2018-12-05 05:36:10", "68.145.168.82", "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.8 (KHTML, like Gecko) Chrome/17.0.940.0 Safari/535.8")
      .addRow("2018-04-01 16:25:18", "21.12.166.184", "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_6; it-it) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27")
      .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  /**
   * This test verifies that when a user attempts to open a sheet that doesn't exist, the plugin will throw an error
   */
  @Test
  public void testInvalidSheetQuery() throws Exception {
    String sql = "SELECT * FROM  table(cp.`excel/test_data.xlsx` (type => 'excel', sheetName => 'noSuchSheet')) LIMIT 1";
    try {
      run(sql);
      fail();
    } catch (DrillRuntimeException e) {
      assertTrue(e.getMessage().contains("Could not open sheet "));
    }
  }

  @Test
  public void testDefineColumnsQuery() throws RpcException {
    String sql = "SELECT * FROM  table(cp.`excel/test_data.xlsx` (type => 'excel', firstColumn => 2, lastColumn => 5))";

    RowSet results = client.queryBuilder().sql(sql).rowSet();
    TupleMetadata expectedSchema = new SchemaBuilder()
      .addNullable("first_name", TypeProtos.MinorType.VARCHAR)
      .addNullable("last_name", TypeProtos.MinorType.VARCHAR)
      .addNullable("email", TypeProtos.MinorType.VARCHAR)
      .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow("Cornelia", "Matej", "cmatej0@mtv.com")
      .addRow("Nydia", "Heintsch", "nheintsch1@godaddy.com")
      .addRow("Waiter", "Sherel", "wsherel2@utexas.edu")
      .addRow("Cicely", "Lyver", "clyver3@mysql.com")
      .addRow("Dorie", "Doe", "ddoe4@spotify.com")
      .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testLastRowQuery() throws RpcException {
    String sql = "SELECT event_date, ip_address, user_agent FROM table(cp.`excel/test_data.xlsx` (type => 'excel', sheetName => 'secondSheet', lastRow => 5))";

    RowSet results = client.queryBuilder().sql(sql).rowSet();
    TupleMetadata expectedSchema = new SchemaBuilder()
      .addNullable("event_date", TypeProtos.MinorType.VARCHAR)
      .addNullable("ip_address", TypeProtos.MinorType.VARCHAR)
      .addNullable("user_agent", TypeProtos.MinorType.VARCHAR)
      .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow("2019-02-17 11:21:45", "166.11.144.176", "Mozilla/5.0 (Windows; U; Windows NT 5.1; ru-RU) AppleWebKit/533.19.4 (KHTML, like Gecko) Version/5.0.3 Safari/533.19.4")
      .addRow("2019-03-03 04:10:31", "203.221.176.215", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_2) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/13.0.782.215 Safari/535.1")
      .addRow("2018-04-05 08:17:17", "11.134.119.132", "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/14.0.813.0 Safari/535.1")
      .addRow("2018-12-05 05:36:10", "68.145.168.82", "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.8 (KHTML, like Gecko) Chrome/17.0.940.0 Safari/535.8")
      .addRow("2018-04-01 16:25:18", "21.12.166.184", "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_6; it-it) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27")
      .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testStarNoFieldNamesQuery() throws RpcException {
    String sql = "SELECT * FROM  table(cp.`excel/test_data.xlsx` (type => 'excel', sheetName => 'thirdSheet', headerRow => -1))";

    RowSet results = client.queryBuilder().sql(sql).rowSet();
    TupleMetadata expectedSchema = new SchemaBuilder()
      .addNullable("field_1", TypeProtos.MinorType.VARCHAR)
      .addNullable("field_2", TypeProtos.MinorType.VARCHAR)
      .addNullable("field_3", TypeProtos.MinorType.VARCHAR)
      .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow("2019-02-17 11:21:45", "166.11.144.176", "Mozilla/5.0 (Windows; U; Windows NT 5.1; ru-RU) AppleWebKit/533.19.4 (KHTML, like Gecko) Version/5.0.3 Safari/533.19.4")
      .addRow("2019-03-03 04:10:31", "203.221.176.215", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_2) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/13.0.782.215 Safari/535.1")
      .addRow("2018-04-05 08:17:17", "11.134.119.132", "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/14.0.813.0 Safari/535.1")
      .addRow("2018-12-05 05:36:10", "68.145.168.82", "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.8 (KHTML, like Gecko) Chrome/17.0.940.0 Safari/535.8")
      .addRow("2018-04-01 16:25:18", "21.12.166.184", "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_6; it-it) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27")
      .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testExplicitNoFieldNamesQuery() throws RpcException {
    String sql = "SELECT field_1, field_2, field_3 FROM  table(cp.`excel/test_data.xlsx` (type => 'excel', sheetName => 'thirdSheet', headerRow => -1))";

    RowSet results = client.queryBuilder().sql(sql).rowSet();
    TupleMetadata expectedSchema = new SchemaBuilder()
      .addNullable("field_1", TypeProtos.MinorType.VARCHAR)
      .addNullable("field_2", TypeProtos.MinorType.VARCHAR)
      .addNullable("field_3", TypeProtos.MinorType.VARCHAR)
      .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow("2019-02-17 11:21:45", "166.11.144.176", "Mozilla/5.0 (Windows; U; Windows NT 5.1; ru-RU) AppleWebKit/533.19.4 (KHTML, like Gecko) Version/5.0.3 Safari/533.19.4")
      .addRow("2019-03-03 04:10:31", "203.221.176.215", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_2) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/13.0.782.215 Safari/535.1")
      .addRow("2018-04-05 08:17:17", "11.134.119.132", "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/14.0.813.0 Safari/535.1")
      .addRow("2018-12-05 05:36:10", "68.145.168.82", "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.8 (KHTML, like Gecko) Chrome/17.0.940.0 Safari/535.8")
      .addRow("2018-04-01 16:25:18", "21.12.166.184", "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_6; it-it) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27")
      .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testBlankRowsInFileQuery() throws RpcException {
    String sql = "SELECT * FROM  table(cp.`excel/test_data.xlsx` (type => 'excel', sheetName => 'fourthSheet'))";

    RowSet results = client.queryBuilder().sql(sql).rowSet();
    TupleMetadata expectedSchema = new SchemaBuilder()
      .addNullable("event_date", TypeProtos.MinorType.VARCHAR)
      .addNullable("ip_address", TypeProtos.MinorType.VARCHAR)
      .addNullable("user_agent", TypeProtos.MinorType.VARCHAR)
      .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow("2019-02-17 11:21:45", "166.11.144.176", "Mozilla/5.0 (Windows; U; Windows NT 5.1; ru-RU) AppleWebKit/533.19.4 (KHTML, like Gecko) Version/5.0.3 Safari/533.19.4")
      .addRow("2019-03-03 04:10:31", "203.221.176.215", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_2) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/13.0.782.215 Safari/535.1")
      .addRow("2018-04-05 08:17:17", "11.134.119.132", "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/14.0.813.0 Safari/535.1")
      .addRow("2018-12-05 05:36:10", "68.145.168.82", "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.8 (KHTML, like Gecko) Chrome/17.0.940.0 Safari/535.8")
      .addRow("2018-04-01 16:25:18", "21.12.166.184", "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_6; it-it) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27")
      .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  /**
   * This tests to what happens if you attempt to query a completely empty sheet
   * The result should be an empty data set.
   *
   * @throws RpcException Throws exception if unable to connect to Drill cluster.
   */
  @Test
  public void testEmptySheetQuery() throws RpcException {
    String sql = "SELECT * " + "FROM table(cp.`excel/test_data.xlsx` (type => 'excel', sheetName => 'emptySheet'))";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    assertNull(results);
  }

  @Test
  public void testMissingDataQuery() throws Exception {
    String sql = "SELECT * FROM table(cp.`excel/test_data.xlsx` (type=> 'excel', sheetName => 'missingDataSheet'))";

    testBuilder()
      .sqlQuery(sql)
      .ordered()
      .baselineColumns("col1", "col2", "col3")
      .baselineValues(1.0,2.0,null)
      .baselineValues(2.0,4.0,null)
      .baselineValues(3.0,null,null)
      .baselineValues(null,6.0,null)
      .baselineValues(null,8.0,null)
      .baselineValues(4.0,null,null)
      .baselineValues(5.0,10.0,null)
      .baselineValues(6.0,12.0,null)
      .go();
  }

  @Test
  public void testInconsistentDataQuery() throws Exception {
    String sql = "SELECT * FROM table(cp.`excel/test_data.xlsx` (type=> 'excel', sheetName => 'inconsistentData', allTextMode => true))";

    testBuilder()
      .sqlQuery(sql)
      .ordered().baselineColumns("col1", "col2")
      .baselineValues("1.0", "Bob")
      .baselineValues("2.0", "Steve")
      .baselineValues("3.0", "Anne")
      .baselineValues("Bob", "3.0")
      .go();
  }

  @Test
  public void testSerDe() throws Exception {
    String sql = "SELECT COUNT(*) as cnt FROM table(cp.`excel/test_data.xlsx` (type=> 'excel', sheetName => 'inconsistentData', allTextMode => true))";
    String plan = queryBuilder().sql(sql).explainJson();
    long cnt = queryBuilder().physical(plan).singletonLong();
    assertEquals("Counts should match",4L, cnt);
  }

  @Test
  public void testExplicitSomeQueryWithCompressedFile() throws Exception {
    generateCompressedFile("excel/test_data.xlsx", "zip", "excel/test_data.xlsx.zip" );

    String sql = "SELECT id, first_name, order_count FROM dfs.`excel/test_data.xlsx.zip`";

    RowSet results = client.queryBuilder().sql(sql).rowSet();
    TupleMetadata expectedSchema = new SchemaBuilder()
      .addNullable("id", TypeProtos.MinorType.FLOAT8)
      .addNullable("first_name", TypeProtos.MinorType.VARCHAR)
      .addNullable("order_count", TypeProtos.MinorType.FLOAT8)
      .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow(1.0, "Cornelia", 22.0)
      .addRow(2.0, "Nydia", 22.0)
      .addRow(3.0, "Waiter", 17.0)
      .addRow(4.0, "Cicely", 6.0)
      .addRow(5.0, "Dorie", 17.0)
      .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testFileWithDoubleDates() throws Exception {
    String sql = "SELECT `Close Date`, `Type` FROM table(cp.`excel/test_data.xlsx` (type=> 'excel', sheetName => 'comps')) WHERE style='Contemporary'";

    RowSet results = client.queryBuilder().sql(sql).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .addNullable("Close Date", TypeProtos.MinorType.TIMESTAMP)
      .addNullable("Type", TypeProtos.MinorType.VARCHAR)
      .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow(1412294400000L, "Hi Rise")
      .addRow(1417737600000L, "Hi Rise")
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
