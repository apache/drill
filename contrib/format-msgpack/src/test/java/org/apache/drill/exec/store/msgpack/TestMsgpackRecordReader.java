package org.apache.drill.exec.store.msgpack;
/*
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;

import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.store.easy.json.JSONRecordReader;
import org.apache.drill.exec.store.msgpack.MsgpackFormatPlugin.MsgpackFormatConfig;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.LogFixture;
import org.apache.drill.test.LogFixture.LogFixtureBuilder;
import org.apache.drill.test.QueryRowSetIterator;
import org.apache.drill.test.rowSet.DirectRowSet;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSetBuilder;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.apache.drill.test.rowSet.schema.SchemaBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;

import ch.qos.logback.classic.Level;

public class TestMsgpackRecordReader extends ClusterTest {
  private static int ALL_FIELD_TYPES_SIZE = 12;

  private static byte TIMESTAMP_TYPE = (byte) -1;
  private static final long BATCH_SIZE = MsgpackRecordReader.DEFAULT_ROWS_PER_BATCH;
  private static File testDir;
  private static File schemaLocation;
  private static String defaultFormat = MsgpackFormatPlugin.DEFAULT_NAME;
  private static String schemaName = "data";
  private static long epochSeconds;
  private static long nanoSeconds;

  @BeforeClass
  public static void setup() throws Exception {
    startCluster(ClusterFixture.builder(dirTestWatcher).maxParallelization(1));
    testDir = dirTestWatcher.makeSubDir(Paths.get(schemaName));
    schemaLocation = new File(testDir, ".schema.proto");
    LocalDate now = LocalDate.now(ZoneId.of("UTC"));
    epochSeconds = now.atStartOfDay(ZoneId.of("UTC")).toEpochSecond();
    nanoSeconds = 0;
  }

  private RowSetBuilder rowSetBuilder;
  private TupleMetadata expectedSchema;
  private QueryRowSetIterator rowSetIterator;
  private SchemaBuilder schemaBuilder;

  @Before
  public void before() {
    if (schemaLocation.exists()) {
      boolean deleted = schemaLocation.delete();
      assertTrue(deleted);
    }
    // disable learning and using schema mode.
    cluster.defineWorkspace("dfs", schemaName, testDir.getAbsolutePath(), defaultFormat, noSchemaConfig());

    schemaBuilder = new SchemaBuilder();
  }

  @After
  public void after() {
    this.expectedSchema = null;
    this.rowSetBuilder = null;
    this.rowSetIterator = null;
    this.schemaBuilder = null;
  }

//  LogFixtureBuilder logBuilder = LogFixture.builder()
//      // Log to the console for debugging convenience
//      .toConsole().logger("org.apache.drill.exec", Level.TRACE);
//  try (LogFixture logs = logBuilder.build()) {
//  }

  @Test
  public void testBasic() throws Exception {

    try (MessagePacker packer = testPacker()) {
      packer.packMapHeader(3);
      packer.packString("apple").packInt(1);
      packer.packString("banana").packInt(2);
      packer.packString("orange").packString("infini!!!");

      packer.packMapHeader(3);
      packer.packString("apple").packInt(1);
      packer.packString("banana").packInt(2);
      packer.packString("potato").packDouble(12.12);
    }

    String sql = "select * from `dfs.data`.`test.mp`";
    RowSet actual = client.queryBuilder().sql(sql).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("apple", TypeProtos.MinorType.BIGINT, TypeProtos.DataMode.OPTIONAL)
        .add("banana", TypeProtos.MinorType.BIGINT, TypeProtos.DataMode.OPTIONAL)
        .add("orange", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
        .add("potato", TypeProtos.MinorType.FLOAT8, TypeProtos.DataMode.OPTIONAL).buildSchema();

    //@formatter:off
    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow(1L, 2L, "infini!!!", null)
        .addRow(1L, 2L, null, 12.12d)
        .build();
    //@formatter:on
    new RowSetComparison(expected).verifyAndClearAll(actual);
  }

  @Test
  public void testSelectColumnThatDoesNotExist() throws Exception {
    try (MessagePacker packer = testPacker()) {
      packer.packMapHeader(3);
      packer.packString("apple").packInt(1);
      packer.packString("banana").packInt(2);
      packer.packString("orange").packString("infini!!!");

      packer.packMapHeader(3);
      packer.packString("apple").packInt(1);
      packer.packString("banana").packInt(2);
      packer.packString("potato").packDouble(12.12);
    }

    String sql = "select pinaple from `dfs.data`.`test.mp`";
    RowSet actual = client.queryBuilder().sql(sql).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("pinaple", TypeProtos.MinorType.INT, TypeProtos.DataMode.OPTIONAL).buildSchema();

    //@formatter:off
    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow(new Object[] {null})
        .addRow(new Object[] {null})
        .build();
    //@formatter:on
    new RowSetComparison(expected).verifyAndClearAll(actual);
  }

  @Test
  public void testBasicSelect() throws Exception {
    try (MessagePacker packer = testPacker()) {
      packer.packMapHeader(3);
      packer.packString("apple").packInt(1);
      packer.packString("banana").packInt(2);
      packer.packString("orange").packString("infini!!!");

      packer.packMapHeader(3);
      packer.packString("apple").packInt(1);
      packer.packString("banana").packInt(2);
      packer.packString("potato").packDouble(12.12);
    }

    String sql = "select `apple` as y from `dfs.data`.`test.mp`";
    RowSet actual = client.queryBuilder().sql(sql).rowSet();

    //@formatter:off
    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("y", TypeProtos.MinorType.BIGINT, TypeProtos.DataMode.OPTIONAL)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow(1L)
        .addRow(1L)
        .build();
    //@formatter:on
    new RowSetComparison(expected).verifyAndClearAll(actual);
  }

  @Test
  public void testSkipOverInvalidElementsInArray() throws Exception {
    try (MessagePacker packer = testPacker()) {
      packer.packMapHeader(1);
      packer.packString("anArray");
      packer.packArrayHeader(5);
      packer.packString("stringVal");
      packer.packString("stringVal");
      packer.packString("stringVal");
      packer.packLong(1L);
      packer.packString("stringVal");
    }

    String sql = "select * from `dfs.data`.`test.mp`";
    RowSet actual = client.queryBuilder().sql(sql).rowSet();

    //@formatter:off
    TupleMetadata expectedSchema = new SchemaBuilder()
        .addArray("anArray", TypeProtos.MinorType.VARCHAR)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow(new Object[] {new String[] {"stringVal","stringVal","stringVal","stringVal"}})
        .build();
    //@formatter:on
    new RowSetComparison(expected).verifyAndClearAll(actual);
  }

  @Test
  public void testArrayOfArray() throws Exception {
    try (MessagePacker packer = testPacker()) {
      packer.packMapHeader(1);
      packer.packString("arrayOfArray");
      packer.packArrayHeader(2);
      packer.packArrayHeader(1);
      packer.packInt(1);
      packer.packArrayHeader(2);
      packer.packInt(1);
      packer.packInt(1);
    }

    String sql = "select * from `dfs.data`.`test.mp`";
    RowSet actual = client.queryBuilder().sql(sql).rowSet();

    //@formatter:off
    TupleMetadata expectedSchema = new SchemaBuilder()
        .addRepeatedList("arrayOfArray")
        .addArray(TypeProtos.MinorType.BIGINT)
        .resumeSchema()
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow(new Object[] {new Long[][] {{1L},{1L,1L}}} )
        .build();
    //@formatter:on
    new RowSetComparison(expected).verifyAndClearAll(actual);
  }

  @Test
  @Ignore // this fails because of interall drill bug
  public void testArrayOfArrayMsgpack() throws Exception {
    try (MessagePacker packer = testPacker()) {
      packer.packMapHeader(1);
      packer.packString("arrayOfArray");
      packer.packArrayHeader(2);
      packer.packArrayHeader(1);
      packer.packLong(1);
      packer.packArrayHeader(2);
      packer.packLong(1);
      packer.packLong(1);
      for (int i = 0; i < BATCH_SIZE; i++) {
        packer.packMapHeader(1);
        packer.packString("anInt");
        packer.packInt(1);
      }
    }

    String sql = "select root.arrayOfArray[0][0] as w from `dfs.data`.`test.mp` as root";
    rowSetIterator = client.queryBuilder().sql(sql).rowSetIterator();

    schemaBuilder = new SchemaBuilder();
    schemaBuilder.add("w", TypeProtos.MinorType.BIGINT, DataMode.OPTIONAL);
    expectedSchema = schemaBuilder.buildSchema();

    DirectRowSet batch1 = nextRowSet();
    rowSetBuilder = newRowSetBuilder();
    rowSetBuilder.addRow(1L);
    for (int i = 0; i < BATCH_SIZE - 1; i++) {
      rowSetBuilder.addRow(new Object[] { null });
    }
    verify(rowSetBuilder.build(), batch1);
    DirectRowSet batch2 = nextRowSet();
    rowSetBuilder = newRowSetBuilder();
    rowSetBuilder.addRow(new Object[] { null });
    verify(rowSetBuilder.build(), batch2);
  }

  @Test
  @Ignore // this fails because of interall drill bug
  public void testArrayOfArrayJson() throws Exception {
    try (OutputStreamWriter w = new OutputStreamWriter(new FileOutputStream(new File(testDir, "test.json")))) {
      w.write("{\"arrayOfArray\":[[1],[1,2]]}\n");
      for (int i = 0; i < JSONRecordReader.DEFAULT_ROWS_PER_BATCH; i++) {
        w.write("{\"anInt\":1}\n");
      }
    }
    LogFixtureBuilder logBuilder = LogFixture.builder()
        // Log to the console for debugging convenience
        .toConsole().logger("org.apache.drill.exec", Level.TRACE);
    try (LogFixture logs = logBuilder.build()) {
      String sql = "select root.arrayOfArray[0][0] as w from `dfs.data`.`test.json` as root";
      rowSetIterator = client.queryBuilder().sql(sql).rowSetIterator();

      schemaBuilder = new SchemaBuilder();
      schemaBuilder.add("w", TypeProtos.MinorType.BIGINT, DataMode.OPTIONAL);
      expectedSchema = schemaBuilder.buildSchema();

      DirectRowSet batch1 = nextRowSet();
      rowSetBuilder = newRowSetBuilder();
      rowSetBuilder.addRow(1L);
      for (int i = 0; i < JSONRecordReader.DEFAULT_ROWS_PER_BATCH - 1; i++) {
        rowSetBuilder.addRow(new Object[] { null });
      }
      verify(rowSetBuilder.build(), batch1);
      DirectRowSet batch2 = nextRowSet();
      rowSetBuilder = newRowSetBuilder();
      rowSetBuilder.addRow(new Object[] { null });
      verify(rowSetBuilder.build(), batch2);
    }
  }

  @Test
  public void testArrayOfArrayOfArray() throws Exception {
    try (MessagePacker packer = testPacker()) {
      packer.packMapHeader(1);
      packer.packString("arrayOfArrayOfArray");
      packer.packArrayHeader(1);
      packer.packArrayHeader(1);
      packer.packArrayHeader(1);
      packer.packInt(5);
    }

    String sql = "select * from `dfs.data`.`test.mp`";
    RowSet actual = client.queryBuilder().sql(sql).rowSet();

    //@formatter:off
    TupleMetadata expectedSchema = new SchemaBuilder()
        .addRepeatedList("arrayOfArrayOfArray")
        .addDimension()
        .addArray(TypeProtos.MinorType.BIGINT)
        .resumeList()
        .resumeSchema()
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow(new Object[] {new Long[][][] {{{5L}}}} )
        .build();
    //@formatter:on
    new RowSetComparison(expected).verifyAndClearAll(actual);
  }

  @Test
  public void testSelectMap() throws Exception {
    try (MessagePacker packer = testPacker()) {
      packer.packMapHeader(1);
      packer.packString("aMap");
      packer.packMapHeader(2);
      packer.packString("a").packInt(33);
      packer.packString("b").packInt(44);

      packer.packMapHeader(1);
      packer.packString("anArray");
      packer.packArrayHeader(2).packFloat(0.1f).packFloat(0.342f);
    }
    String sql = "select root.aMap.b as x from `dfs.data`.`test.mp` as root";
    RowSet actual = client.queryBuilder().sql(sql).rowSet();

    //@formatter:off
    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("x", TypeProtos.MinorType.BIGINT, TypeProtos.DataMode.OPTIONAL)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow(44L)
        .addRow(new Object[] {null})
        .build();
    //@formatter:on
    new RowSetComparison(expected).verifyAndClearAll(actual);
  }

  @Test
  public void testSelectArray() throws Exception {
    try (MessagePacker packer = testPacker()) {
      packer.packMapHeader(1);
      packer.packString("aMap");
      packer.packMapHeader(2);
      packer.packString("a").packInt(33);
      packer.packString("b").packInt(44);

      packer.packMapHeader(1);
      packer.packString("anArray");
      packer.packArrayHeader(2).packFloat(0.1f).packFloat(0.342f);
    }
    String sql = "select round(root.anArray[1], 3) as x from `dfs.data`.`test.mp` as root";
    RowSet actual = client.queryBuilder().sql(sql).rowSet();

    //@formatter:off
    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("x", TypeProtos.MinorType.FLOAT8, TypeProtos.DataMode.OPTIONAL)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow(new Object[] {null})
        .addRow(0.342d)
        .build();
    //@formatter:on
    new RowSetComparison(expected).verifyAndClearAll(actual);
  }

  @Test
  public void testTypes() throws Exception {
    try (MessagePacker packer = testPacker()) {
      byte[] bytes = "some data".getBytes();
      byte[] binary = { 0x1, 0x2, 0x9, 0xF };
      packer.packMapHeader(10);
      packer.packString("raw").packRawStringHeader(bytes.length).addPayload(bytes);
      packer.packString("bin").packBinaryHeader(binary.length).addPayload(binary);
      packer.packString("int").packInt(32_000_000);
      packer.packString("big").packBigInteger(BigInteger.valueOf(64_000_000_000L));
      packer.packString("byt").packByte((byte) 77);
      packer.packString("lon").packLong(64_000_000_000L);
      packer.packString("nil").packNil();
      packer.packString("sho").packShort((short) 222);
      packer.packString("dou").packDouble(1.1d);
      packer.packString("flo").packFloat(1.1f);
    }

    String sql = "select * from `dfs.data`.`test.mp` as root";
    RowSet actual = client.queryBuilder().sql(sql).rowSet();

    // nil column will not be read at all.

    //@formatter:off
    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("raw", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
        .add("bin", TypeProtos.MinorType.VARBINARY, TypeProtos.DataMode.OPTIONAL)
        .add("int", TypeProtos.MinorType.BIGINT, TypeProtos.DataMode.OPTIONAL)
        .add("big", TypeProtos.MinorType.BIGINT, TypeProtos.DataMode.OPTIONAL)
        .add("byt", TypeProtos.MinorType.BIGINT, TypeProtos.DataMode.OPTIONAL)
        .add("lon", TypeProtos.MinorType.BIGINT, TypeProtos.DataMode.OPTIONAL)
        .add("sho", TypeProtos.MinorType.BIGINT, TypeProtos.DataMode.OPTIONAL)
        .add("dou", TypeProtos.MinorType.FLOAT8, TypeProtos.DataMode.OPTIONAL)
        .add("flo", TypeProtos.MinorType.FLOAT8, TypeProtos.DataMode.OPTIONAL)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow(
            "some data",
            new byte[]{0x1, 0x2, 0x9, 0xF},
            32_000_000L,
            64_000_000_000L,
            77L,
            64_000_000_000L,
            222L,
            1.1d,
            1.1f)
        .build();
    //@formatter:on
    new RowSetComparison(expected).verifyAndClearAll(actual);
  }

  @Test
  public void testSelectMapOfMap() throws Exception {
    try (MessagePacker packer = testPacker()) {
      packer.packMapHeader(1);
      packer.packString("mapOfMap");
      packer.packMapHeader(2);
      packer.packString("aMap").packMapHeader(1).packString("x").packInt(1);
      packer.packString("aString").packString("x");
    }

    String sql = "select root.mapOfMap.aMap.x as x from `dfs.data`.`test.mp` as root";
    RowSet actual = client.queryBuilder().sql(sql).rowSet();

    //@formatter:off
    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("x", TypeProtos.MinorType.BIGINT, TypeProtos.DataMode.OPTIONAL)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow(1L)
        .build();
    //@formatter:on
    new RowSetComparison(expected).verifyAndClearAll(actual);
  }

  @Test
  public void testSelectArrayOfMap() throws Exception {
    try (MessagePacker packer = testPacker()) {
      packer.packMapHeader(1);
      packer.packString("arrayOfMap");
      packer.packArrayHeader(2);
      packer.packMapHeader(1).packString("x").packInt(1);
      packer.packMapHeader(1).packString("x").packInt(1);
    }

    String sql = "select root.arrayOfMap[0].x as x from `dfs.data`.`test.mp` as root";
    RowSet actual = client.queryBuilder().sql(sql).rowSet();

    //@formatter:off
    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("x", TypeProtos.MinorType.BIGINT, TypeProtos.DataMode.OPTIONAL)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow(1L)
        .build();
    //@formatter:on
    new RowSetComparison(expected).verifyAndClearAll(actual);
  }

  @Test
  public void testdNestedArrayWithMap() throws Exception {
    try (MessagePacker packer = testPacker()) {
      packer.packMapHeader(1);
      packer.packString("arrayWithMap");
      packer.packArrayHeader(2);
      packer.packMapHeader(1).packString("x").packInt(1);
      packer.packMapHeader(1).packString("y").packFloat(2.2f);
    }

    String sql = "select root.arrayWithMap[0].x as x from `dfs.data`.`test.mp` as root";
    RowSet actual = client.queryBuilder().sql(sql).rowSet();

    //@formatter:off
    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("x", TypeProtos.MinorType.BIGINT, TypeProtos.DataMode.OPTIONAL)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow(1L)
        .build();
    //@formatter:on
    new RowSetComparison(expected).verifyAndClearAll(actual);
  }

  @Test
  public void testSelectMapWithArray() throws Exception {
    try (MessagePacker packer = testPacker()) {
      packer.packMapHeader(1);
      packer.packString("mapWithArray");
      packer.packMapHeader(2);
      packer.packString("anArray").packArrayHeader(2).packString("v1").packString("v2");
      packer.packString("aString").packString("x");
    }

    String sql = "select root.mapWithArray.anArray[1] as x from `dfs.data`.`test.mp` as root";
    RowSet actual = client.queryBuilder().sql(sql).rowSet();

    //@formatter:off
    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("x", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow("v2")
        .build();
    //@formatter:on
    new RowSetComparison(expected).verifyAndClearAll(actual);
  }

  @Test
  public void testdMissingFieldName() throws Exception {
    try (MessagePacker packer = testPacker()) {
      packer.packMapHeader(3);
      packer.packString("x").packLong(1);
      packer.packString("y").packLong(2);
      packer.packString("z").packLong(3);

      packer.packMapHeader(3);
      packer.packString("x").packLong(1);
      packer./* missing */packLong(2);
      packer.packString("z").packLong(3);
    }

    String sql = "select * from `dfs.data`.`test.mp`";
    RowSet actual = client.queryBuilder().sql(sql).rowSet();

    //@formatter:off
    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("x", TypeProtos.MinorType.BIGINT, TypeProtos.DataMode.OPTIONAL)
        .add("y", TypeProtos.MinorType.BIGINT, TypeProtos.DataMode.OPTIONAL)
        .add("z", TypeProtos.MinorType.BIGINT, TypeProtos.DataMode.OPTIONAL)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow(1L, 2L, 3L)
        // Incomplete MAP are skipped
        .build();
    //@formatter:on
    new RowSetComparison(expected).verifyAndClearAll(actual);
  }

  @Test
  public void testdCountInvalidMessages() throws Exception {
    try (MessagePacker packer = testPacker()) {
      for (int i = 0; i < 1000; i++) {
        packer.packMapHeader(1);
        packer.packString("x").packLong(1);
      }
      // missing field name, all these maps are skipped
      for (int i = 0; i < 100; i++) {
        packer.packMapHeader(1);
        packer.packLong(1);
      }

      // one map with invalid fields
      packer.packMapHeader(3);
      // 1. good tuple
      packer.packString("x").packLong(1);
      // 2. map is not a good field name, skipped
      packer.packMapHeader(1);
      packer.packString("x").packLong(1);
      // 2. map is not a good field name, skipped
      packer.packMapHeader(1);
      packer.packString("x").packLong(1);
      // 2. tuple okay
      packer.packString("x").packLong(1);
      // 3. tuple okay
      packer.packString("x").packLong(1);

      // out of bad map

      // one more good map
      packer.packMapHeader(1);
      packer.packString("x").packLong(1);
    }

    String sql = "select count(1) as c from `dfs.data`.`test.mp`";
    RowSet actual = client.queryBuilder().sql(sql).rowSet();

    //@formatter:off
    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("c", TypeProtos.MinorType.BIGINT, TypeProtos.DataMode.REQUIRED)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow(1002L)
        .build();
    //@formatter:on
    new RowSetComparison(expected).verifyAndClearAll(actual);
  }

  @Test
  public void testTimestamp() throws Exception {
    LocalDate now = LocalDate.now(ZoneId.of("UTC"));
    long epochSeconds = now.atStartOfDay(ZoneId.of("UTC")).toEpochSecond();
    long nanoSeconds = 0;

    try (MessagePacker packer = testPacker()) {
      packer.packMapHeader(1);
      packer.packString("timestamp");
      byte[] bytes = getTimestampBytes(epochSeconds, nanoSeconds);
      packer.packExtensionTypeHeader(TIMESTAMP_TYPE, bytes.length);
      packer.addPayload(bytes);
    }

    String sql = "select * from `dfs.data`.`test.mp`";
    RowSet actual = client.queryBuilder().sql(sql).rowSet();

    //@formatter:off
    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("timestamp", TypeProtos.MinorType.TIMESTAMP, TypeProtos.DataMode.OPTIONAL)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow(epochSeconds*1000)
        .build();
    //@formatter:on
    new RowSetComparison(expected).verifyAndClearAll(actual);
  }

  @Test
  public void testTimestampWithNanos() throws Exception {
    LocalDate now = LocalDate.now(ZoneId.of("UTC"));
    long epochSeconds = now.atStartOfDay(ZoneId.of("UTC")).toEpochSecond();
    long nanoSeconds = 900_000;

    try (MessagePacker packer = testPacker()) {
      packer.packMapHeader(1);
      packer.packString("timestamp");
      byte[] bytes = getTimestampBytes(epochSeconds, nanoSeconds);
      packer.packExtensionTypeHeader(TIMESTAMP_TYPE, bytes.length);
      packer.addPayload(bytes);
    }

    String sql = "select * from `dfs.data`.`test.mp`";
    RowSet actual = client.queryBuilder().sql(sql).rowSet();

    //@formatter:off
    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("timestamp", TypeProtos.MinorType.TIMESTAMP, TypeProtos.DataMode.OPTIONAL)
        .buildSchema();

    // we are ignoring the nanos
    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow(epochSeconds*1000)
        .build();
    //@formatter:on
    new RowSetComparison(expected).verifyAndClearAll(actual);
  }

  @Test
  public void testTimestampLargeWithNanos() throws Exception {
    long epochSeconds = (long) Math.pow(2, 35);
    long nanoSeconds = 900_000;

    try (MessagePacker packer = testPacker()) {
      packer.packMapHeader(1);
      packer.packString("timestamp");
      byte[] bytes = getTimestampBytes(epochSeconds, nanoSeconds);
      packer.packExtensionTypeHeader(TIMESTAMP_TYPE, bytes.length);
      packer.addPayload(bytes);
    }

    String sql = "select * from `dfs.data`.`test.mp`";
    RowSet actual = client.queryBuilder().sql(sql).rowSet();

    //@formatter:off
    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("timestamp", TypeProtos.MinorType.TIMESTAMP, TypeProtos.DataMode.OPTIONAL)
        .buildSchema();

    // we are ignoring the nanos
    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow(epochSeconds*1000)
        .build();
    //@formatter:on
    new RowSetComparison(expected).verifyAndClearAll(actual);
  }

  @Test
  public void testUnknownExt() throws Exception {
    try (MessagePacker packer = testPacker()) {
      byte type = (byte) 7; // unknown msgpack extension code

      byte[] bytes = new byte[2];
      bytes[0] = 'j';
      bytes[1] = 'k';

      packer.packMapHeader(1);
      packer.packString("x").packExtensionTypeHeader(type, bytes.length);
      packer.addPayload(bytes);
    }

    String sql = "select * from `dfs.data`.`test.mp`";
    RowSet actual = client.queryBuilder().sql(sql).rowSet();

    //@formatter:off
    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("x", TypeProtos.MinorType.VARBINARY, TypeProtos.DataMode.OPTIONAL)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow(new byte[] {'j','k'})
        .build();
    //@formatter:on
    new RowSetComparison(expected).verifyAndClearAll(actual);
  }

  @Test
  public void testMapWithNonStringFieldName() throws Exception {
    try (MessagePacker packer = testPacker()) {
      packer.packMapHeader(5);
      // Int
      packer.packInt(1).packLong(1);
      // Binary
      packer.packBinaryHeader(1).addPayload(new byte[] { 'A' }).packLong(2);
      // Long
      packer.packLong(3).packLong(3);
      // Raw string
      String name = "xyz";
      packer.packRawStringHeader(name.length()).addPayload(name.getBytes()).packLong(4);
      // Short
      packer.packShort((short) 5).packLong(5);
    }

    String sql = "select * from `dfs.data`.`test.mp`";
    RowSet actual = client.queryBuilder().sql(sql).rowSet();

    //@formatter:off
    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("1", TypeProtos.MinorType.BIGINT, TypeProtos.DataMode.OPTIONAL)
        .add("A", TypeProtos.MinorType.BIGINT, TypeProtos.DataMode.OPTIONAL)
        .add("3", TypeProtos.MinorType.BIGINT, TypeProtos.DataMode.OPTIONAL)
        .add("xyz", TypeProtos.MinorType.BIGINT, TypeProtos.DataMode.OPTIONAL)
        .add("5", TypeProtos.MinorType.BIGINT, TypeProtos.DataMode.OPTIONAL)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow(1L, 2L, 3L, 4L, 5L)
        .build();
    //@formatter:on
    new RowSetComparison(expected).verifyAndClearAll(actual);
  }

  @Test
  public void testIntSizeChanging() throws Exception {
    try (MessagePacker packer = testPacker()) {
      for (long i = 0; i < BATCH_SIZE; i++) {
        packer.packMapHeader(1);
        packer.packString("x").packLong(i);
      }

      packer.packMapHeader(1);
      packer.packString("x").packLong(Long.MAX_VALUE);
      packer.packMapHeader(1);
      packer.packString("x").packLong(Long.MAX_VALUE);
    }

    String sql = "select x from `dfs.data`.`test.mp`";
    QueryRowSetIterator it = client.queryBuilder().sql(sql).rowSetIterator();
    assertTrue(it.hasNext());
    DirectRowSet batch1 = it.next();
    assertTrue(it.hasNext());
    DirectRowSet batch2 = it.next();
    //@formatter:off
    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("x", TypeProtos.MinorType.BIGINT, TypeProtos.DataMode.OPTIONAL)
        .buildSchema();
    //@formatter:on
    RowSetBuilder b = new RowSetBuilder(client.allocator(), expectedSchema);
    for (long i = 0; i < BATCH_SIZE; i++) {
      b.addRow(i);
    }

    new RowSetComparison(b.build()).verifyAndClearAll(batch1);

    b = new RowSetBuilder(client.allocator(), expectedSchema);
    b.addRow(Long.MAX_VALUE);
    b.addRow(Long.MAX_VALUE);
    new RowSetComparison(b.build()).verifyAndClearAll(batch2);
  }

  ////////////////////////////////////////////////////////////////////////////////
  ////////////////////////////////////////////////////////////////////////////////
  ////////////////////////////////////////////////////////////////////////////////
  // TESTING SCHEMA CHANGES
  ////////////////////////////////////////////////////////////////////////////////
  ////////////////////////////////////////////////////////////////////////////////
  ////////////////////////////////////////////////////////////////////////////////

  @Test
  public void testSchemaVarcharColumn() throws Exception {
    learnModel();
    String sql = "select root.str as w from dfs.data.`secondBatchHasCompleteModel.mp` as root";
    rowSetIterator = client.queryBuilder().sql(sql).rowSetIterator();

    schemaBuilder.add("w", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL);
    expectedSchema = schemaBuilder.buildSchema();
    verifyFirstBatchNull();

    rowSetBuilder = newRowSetBuilder();
    rowSetBuilder.addRow("string");
    verify(rowSetBuilder.build(), nextRowSet());
  }

  @Test
  public void testSchemaShortColumn() throws Exception {
    learnModel();
    String sql = "select root.sho as w from dfs.data.`secondBatchHasCompleteModel.mp` as root";
    rowSetIterator = client.queryBuilder().sql(sql).rowSetIterator();

    schemaBuilder.add("w", TypeProtos.MinorType.BIGINT, TypeProtos.DataMode.OPTIONAL);
    expectedSchema = schemaBuilder.buildSchema();
    verifyFirstBatchNull();

    rowSetBuilder = newRowSetBuilder();
    rowSetBuilder.addRow(222L);
    verify(rowSetBuilder.build(), nextRowSet());
  }

  @Test
  public void testSchemaMapOfMapShort() throws Exception {
    learnModel();
    String sql = "select root.mapOfmap.aMap.sho as w from dfs.data.`secondBatchHasCompleteModel.mp` as root";
    rowSetIterator = client.queryBuilder().sql(sql).rowSetIterator();

    schemaBuilder.add("w", TypeProtos.MinorType.BIGINT, TypeProtos.DataMode.OPTIONAL);
    expectedSchema = schemaBuilder.buildSchema();
    verifyFirstBatchNull();

    rowSetBuilder = newRowSetBuilder();
    rowSetBuilder.addRow(222L);
    verify(rowSetBuilder.build(), nextRowSet());
  }

  @Test
  public void testSchemaArrayOfInt() throws Exception {
    learnModel();
    String sql = "select root.arrayOfInt[0] as w from dfs.data.`secondBatchHasCompleteModel.mp` as root";
    rowSetIterator = client.queryBuilder().sql(sql).rowSetIterator();

    schemaBuilder.add("w", TypeProtos.MinorType.BIGINT, TypeProtos.DataMode.OPTIONAL);
    expectedSchema = schemaBuilder.buildSchema();
    verifyFirstBatchNull();

    rowSetBuilder = newRowSetBuilder();
    rowSetBuilder.addRow(1L);
    verify(rowSetBuilder.build(), nextRowSet());
  }

  @Test
  @Ignore // this fails because of interall drill bug
  public void testSchemaArrayOfArrayCell() throws Exception {
    learnModel();
    String sql = "select root.arrayOfarray[0][0] as w from dfs.data.`secondBatchHasCompleteModel.mp` as root";
    rowSetIterator = client.queryBuilder().sql(sql).rowSetIterator();

    schemaBuilder.add("w", TypeProtos.MinorType.BIGINT, TypeProtos.DataMode.OPTIONAL);
    expectedSchema = schemaBuilder.buildSchema();
    verifyFirstBatchNull();

    rowSetBuilder = newRowSetBuilder();
    rowSetBuilder.addRow(1L);
    verify(rowSetBuilder.build(), nextRowSet());
  }

  @Test
  public void testSchemaArrayOfMapByte() throws Exception {
    learnModel();
    String sql = "select root.arrayOfMap[0].byt as w from dfs.data.`secondBatchHasCompleteModel.mp` as root";
    rowSetIterator = client.queryBuilder().sql(sql).rowSetIterator();

    schemaBuilder.add("w", TypeProtos.MinorType.BIGINT, TypeProtos.DataMode.OPTIONAL);
    expectedSchema = schemaBuilder.buildSchema();
    verifyFirstBatchNull();

    rowSetBuilder = newRowSetBuilder();
    rowSetBuilder.addRow(-127L);
    verify(rowSetBuilder.build(), nextRowSet());
  }

  @Test
  public void testSchemaArrayOfMapVarchar() throws Exception {
    learnModel();
    String sql = "select root.arrayOfMap[0].str as w from dfs.data.`secondBatchHasCompleteModel.mp` as root";
    rowSetIterator = client.queryBuilder().sql(sql).rowSetIterator();

    schemaBuilder.add("w", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL);
    expectedSchema = schemaBuilder.buildSchema();
    verifyFirstBatchNull();

    rowSetBuilder = newRowSetBuilder();
    rowSetBuilder.addRow("string");
    verify(rowSetBuilder.build(), nextRowSet());
  }

  @Test
  public void testSchemaMapWithAnArrayCell() throws Exception {
    learnModel();
    String sql = "select root.mapWithArray.anArray[0] as w from dfs.data.`secondBatchHasCompleteModel.mp` as root";
    rowSetIterator = client.queryBuilder().sql(sql).rowSetIterator();

    schemaBuilder.add("w", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL);
    expectedSchema = schemaBuilder.buildSchema();
    verifyFirstBatchNull();

    rowSetBuilder = newRowSetBuilder();
    rowSetBuilder.addRow("v1");
    verify(rowSetBuilder.build(), nextRowSet());
  }

  @Test
  public void testSchemaInconsitentColumn() throws Exception {
    learnModel();

    try (MessagePacker packer = testPacker()) {
      packer.packMapHeader(1);
      packer.packString("str");
      packer.packLong(1L);
      packer.packMapHeader(1);
      packer.packString("str");
      packer.packString("data");
    }
    String sql = "select root.str as w from dfs.data.`test.mp` as root";
    rowSetIterator = client.queryBuilder().sql(sql).rowSetIterator();

    schemaBuilder.add("w", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL);
    expectedSchema = schemaBuilder.buildSchema();
    rowSetBuilder = newRowSetBuilder();
    // record not matching schema are skipped.
    // rowSetBuilder.addRow(new Object[] {null});
    rowSetBuilder.addRow("data");
    verify(rowSetBuilder.build(), nextRowSet());
  }

  private static byte[] getTimestampBytes(long epochSeconds, long nanoSeconds) {
    if (epochSeconds >> 34 == 0) {
      long data64 = (nanoSeconds << 34) | epochSeconds;
      if ((data64 & 0xffffffff00000000L) == 0) {
        // timestamp 32
        return writeBytes(data64, 4);
      } else {
        // timestamp 64
        return writeBytes(data64, 8);
      }
    } else {
      byte[] nanoUnsigned = writeBytes(nanoSeconds, 4);
      byte[] epochSecondsSigned = writeBytes(epochSeconds, 8);
      byte[] ret = new byte[12];
      System.arraycopy(nanoUnsigned, 0, ret, 0, 4);
      System.arraycopy(epochSecondsSigned, 0, ret, 4, 8);
      return ret;
    }
  }

  private static byte[] writeBytes(long data64, int n) {
    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.putLong(data64);
    byte[] bytes = new byte[n];
    buffer.position(Long.BYTES - n);
    buffer.get(bytes);
    return bytes;
  }

  private static MessagePacker testPacker() throws IOException {
    return MessagePack.newDefaultPacker(new FileOutputStream(new File(testDir, "test.mp")));
  }

  private static MessagePacker secondBatchPacker() throws IOException {
    return MessagePack.newDefaultPacker(new FileOutputStream(new File(testDir, "secondBatchHasCompleteModel.mp")));
  }

  private static MessagePacker completeModelPacker() throws IOException {
    return MessagePack.newDefaultPacker(new FileOutputStream(new File(testDir, "completeModel.mp")));
  }

  private static void secondBatchHasCompleteModel() throws IOException {
    try (MessagePacker packer = secondBatchPacker()) {
      for (int i = 0; i < BATCH_SIZE; i++) {
        packer.packMapHeader(1);
        packer.packString("int").packInt(0);
      }
      writeCompleteModel(packer);
    }
  }

  private static void completeModel() throws IOException {
    try (MessagePacker packer = completeModelPacker()) {
      writeCompleteModel(packer);
    }
  }

  private static void writeCompleteModel(MessagePacker packer) throws IOException {
    int numColumns = 18;
    int numColumnCounter = 0;
    packer.packMapHeader(numColumns);

    // 0
    packer.packString("arrayOfArray");
    packer.packArrayHeader(2);
    packer.packArrayHeader(3);
    packer.packInt(1);
    packer.packInt(1);
    packer.packInt(1);
    packer.packArrayHeader(5);
    packer.packInt(1);
    packer.packInt(1);
    packer.packInt(1);
    packer.packInt(1);
    packer.packInt(1);
    numColumnCounter++;

    // 1
    packer.packString("arrayOfMap");
    packer.packArrayHeader(2);
    packer.packMapHeader(ALL_FIELD_TYPES_SIZE);
    addAllFieldTypes(packer);
    packer.packMapHeader(ALL_FIELD_TYPES_SIZE);
    addAllFieldTypes(packer);
    numColumnCounter++;

    // 2
    packer.packString("mapOfMap");
    packer.packMapHeader(1).packString("aMap").packMapHeader(ALL_FIELD_TYPES_SIZE);
    // 10 sub fields
    addAllFieldTypes(packer);
    numColumnCounter++;

    // 3
    packer.packString("mapWithArray");
    packer.packMapHeader(2);
    packer.packString("anArray").packArrayHeader(2).packString("v1").packString("v2");
    packer.packString("aString").packString("x");
    numColumnCounter++;

    // 4
    packer.packString("arrayWithMap");
    packer.packArrayHeader(2);
    packer.packMapHeader(1).packString("x").packInt(1);
    packer.packMapHeader(1).packString("y").packFloat(2.2f);
    numColumnCounter++;

    // 5
    packer.packString("arrayOfInt");
    packer.packArrayHeader(2);
    packer.packInt(1);
    packer.packInt(1);
    numColumnCounter++;
    // 12 + 5
    addAllFieldTypes(packer);
    numColumnCounter += ALL_FIELD_TYPES_SIZE;
    assertEquals(numColumns, numColumnCounter);
  }

  private static void addAllFieldTypes(MessagePacker packer) throws IOException {
    int numFieldCounter = 0;
    byte[] bytes = "some data".getBytes();
    packer.packString("raw").packRawStringHeader(bytes.length).addPayload(bytes);
    numFieldCounter++;
    byte[] binary = { 0x1, 0x2, 0x9, 0xF };
    packer.packString("bin").packBinaryHeader(binary.length).addPayload(binary);
    numFieldCounter++;
    packer.packString("int").packInt(32_000_000);
    numFieldCounter++;
    packer.packString("big").packBigInteger(BigInteger.valueOf(64_000_000_000L));
    numFieldCounter++;
    packer.packString("byt").packByte((byte) 129);
    numFieldCounter++;
    packer.packString("lon").packLong(64_000_000_000L);
    numFieldCounter++;
    packer.packString("nil").packNil();
    numFieldCounter++;
    packer.packString("sho").packShort((short) 222);
    numFieldCounter++;
    packer.packString("dou").packDouble(1.1d);
    numFieldCounter++;
    packer.packString("flo").packFloat(1.1f);
    numFieldCounter++;
    packer.packString("str").packString("string");
    numFieldCounter++;

    byte[] tsBytes = getTimestampBytes(epochSeconds, nanoSeconds);
    packer.packString("ts").packExtensionTypeHeader(TIMESTAMP_TYPE, tsBytes.length).addPayload(tsBytes);
    numFieldCounter++;

    assertEquals(ALL_FIELD_TYPES_SIZE, numFieldCounter);
  }

  private static void learnModel() throws Exception {
    completeModel();
    secondBatchHasCompleteModel();
    // schema learning mode.
    cluster.defineWorkspace("dfs", schemaName, testDir.getAbsolutePath(), defaultFormat, learnSchemaConfig());

    String sql = "select * from `dfs.data`.`completeModel.mp`";
    List<QueryDataBatch> results = client.queryBuilder().sql(sql).results();
    for (QueryDataBatch batch : results) {
      batch.release();
    }
    // schema using mode.
    cluster.defineWorkspace("dfs", schemaName, testDir.getAbsolutePath(), defaultFormat, useSchemaConfig());
  }

  private static MsgpackFormatConfig learnSchemaConfig() {
    return buildConfig(true, true);
  }

  private static MsgpackFormatConfig useSchemaConfig() {
    return buildConfig(false, true);
  }

  private static MsgpackFormatConfig noSchemaConfig() {
    return buildConfig(false, false);
  }

  private static MsgpackFormatConfig buildConfig(boolean learnSchema, boolean useSchema) {
    MsgpackFormatConfig msgFormat = new MsgpackFormatConfig();
    msgFormat.setLenient(true);
    msgFormat.setPrintToConsole(true);
    msgFormat.setLearnSchema(learnSchema);
    msgFormat.setUseSchema(useSchema);
    msgFormat.setExtensions(ImmutableList.of("mp"));
    return msgFormat;
  }

  private RowSetBuilder newRowSetBuilder() {
    return new RowSetBuilder(client.allocator(), expectedSchema);
  }

  private DirectRowSet nextRowSet() {
    assertTrue(rowSetIterator.hasNext());
    return rowSetIterator.next();
  }

  private void verify(RowSet expected, RowSet actual) {
    new RowSetComparison(expected).verifyAndClearAll(actual);
  }

  private void verifyFirstBatchNull() {
    rowSetBuilder = newRowSetBuilder();
    for (long i = 0; i < BATCH_SIZE; i++) {
      rowSetBuilder.addRow(new Object[] { null });
    }
    verify(rowSetBuilder.build(), nextRowSet());
  }

}
