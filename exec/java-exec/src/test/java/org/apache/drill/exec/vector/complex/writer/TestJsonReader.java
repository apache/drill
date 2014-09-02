/**
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
package org.apache.drill.exec.vector.complex.writer;

import static org.jgroups.util.Util.assertTrue;
import static org.junit.Assert.assertEquals;
import io.netty.buffer.DrillBuf;
import static org.junit.Assert.assertNull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

import com.google.common.io.Files;
import org.apache.drill.BaseTestQuery;
import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.TopLevelAllocator;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.NullableBigIntVector;
import org.apache.drill.exec.vector.NullableIntVector;
import org.apache.drill.exec.vector.RepeatedBigIntVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.MapVector;
import org.apache.drill.exec.vector.complex.fn.JsonReader;
import org.apache.drill.exec.vector.complex.fn.JsonReaderWithState;
import org.apache.drill.exec.vector.complex.fn.JsonWriter;
import org.apache.drill.exec.vector.complex.fn.ReaderJSONRecordSplitter;
import org.apache.drill.exec.vector.complex.impl.ComplexWriterImpl;
import org.apache.drill.exec.vector.complex.reader.FieldReader;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;

public class TestJsonReader extends BaseTestQuery {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestJsonReader.class);

  private static BufferAllocator allocator;
  private static final boolean VERBOSE_DEBUG = true;

  @BeforeClass
  public static void setupAllocator(){
    allocator = new TopLevelAllocator();
  }

  @AfterClass
  public static void destroyAllocator(){
    allocator.close();
  }

  public void runTestsOnFile(String filename, UserBitShared.QueryType queryType, String[] queries, long[] rowCounts) throws Exception {
    if (VERBOSE_DEBUG) {
      System.out.println("===================");
      System.out.println("source data in json");
      System.out.println("===================");
      System.out.println(Files.toString(FileUtils.getResourceAsFile(filename), Charsets.UTF_8));
    }

    int i = 0;
    for (String query : queries) {
      if (VERBOSE_DEBUG) {
        System.out.println("=====");
        System.out.println("query");
        System.out.println("=====");
        System.out.println(query);
        System.out.println("======");
        System.out.println("result");
        System.out.println("======");
      }
      int rowCount = testRunAndPrint(queryType, query);
      assertEquals(rowCounts[i], rowCount);
      System.out.println();
      i++;
    }
  }

  @Test
  public void testSingleColumnRead_vector_fill_bug() throws Exception {
    String[] queries = {"select * from cp.`/store/json/single_column_long_file.json`"};
    long[] rowCounts = {13512};
    String filename = "/store/json/single_column_long_file.json";
    runTestsOnFile(filename, UserBitShared.QueryType.SQL, queries, rowCounts);
  }

  @Test
  public void testNonExistentColumnReadAlone() throws Exception {
    String[] queries = {"select non_existent_column from cp.`/store/json/single_column_long_file.json`"};
    long[] rowCounts = {13512};
    String filename = "/store/json/single_column_long_file.json";
    runTestsOnFile(filename, UserBitShared.QueryType.SQL, queries, rowCounts);
  }

  public void testAllTextMode() throws Exception {
    test("alter system set `store.json.all_text_mode` = true");
    String[] queries = {"select * from cp.`/store/json/schema_change_int_to_string.json`"};
    long[] rowCounts = {3};
    String filename = "/store/json/schema_change_int_to_string.json";
    runTestsOnFile(filename, UserBitShared.QueryType.SQL, queries, rowCounts);
  }

  @Test
  public void readComplexWithStar() throws Exception {
    List<QueryResultBatch> results = testSqlWithResults("select * from cp.`/store/json/test_complex_read_with_star.json`");
    assertEquals(2, results.size());

    RecordBatchLoader batchLoader = new RecordBatchLoader(getAllocator());
    QueryResultBatch batch = results.get(0);

    assertTrue(batchLoader.load(batch.getHeader().getDef(), batch.getData()));
    assertEquals(3, batchLoader.getSchema().getFieldCount());
    testExistentColumns(batchLoader, batch);

    batch.release();
    batchLoader.clear();
  }

  @Test
  public void testNullWhereListExpected() throws Exception {
    test("alter system set `store.json.all_text_mode` = true");
    String[] queries = {"select * from cp.`/store/json/null_where_list_expected.json`"};
    long[] rowCounts = {3};
    String filename = "/store/json/null_where_list_expected.json";
    runTestsOnFile(filename, UserBitShared.QueryType.SQL, queries, rowCounts);
  }

  @Test
  public void testNullWhereMapExpected() throws Exception {
    test("alter system set `store.json.all_text_mode` = true");
    String[] queries = {"select * from cp.`/store/json/null_where_map_expected.json`"};
    long[] rowCounts = {3};
    String filename = "/store/json/null_where_map_expected.json";
    runTestsOnFile(filename, UserBitShared.QueryType.SQL, queries, rowCounts);
  }

  // The project pushdown rule is correctly adding the projected columns to the scan, however it is not removing
  // the redundant project operator after the scan, this tests runs a physical plan generated from one of the tests to
  // ensure that the project is filtering out the correct data in the scan alone
  @Test
  public void testProjectPushdown() throws Exception {
    String[] queries = {Files.toString(FileUtils.getResourceAsFile("/store/json/project_pushdown_json_physical_plan.json"), Charsets.UTF_8)};
    long[] rowCounts = {3};
    String filename = "/store/json/schema_change_int_to_string.json";
    test("alter system set `store.json.all_text_mode` = false");
    runTestsOnFile(filename, UserBitShared.QueryType.PHYSICAL, queries, rowCounts);

    List<QueryResultBatch> results = testPhysicalWithResults(queries[0]);
    assertEquals(2, results.size());
    // "`field_1`", "`field_3`.`inner_1`", "`field_3`.`inner_2`", "`field_4`.`inner_1`"

    RecordBatchLoader batchLoader = new RecordBatchLoader(getAllocator());
    QueryResultBatch batch = results.get(0);
    assertTrue(batchLoader.load(batch.getHeader().getDef(), batch.getData()));
    assertEquals(5, batchLoader.getSchema().getFieldCount());
    testExistentColumns(batchLoader, batch);

    VectorWrapper vw = batchLoader.getValueAccessorById(
        NullableIntVector.class, //
        batchLoader.getValueVectorId(SchemaPath.getCompoundPath("non_existent_at_root")).getFieldIds() //
    );
    assertNull(vw.getValueVector().getAccessor().getObject(0));
    assertNull(vw.getValueVector().getAccessor().getObject(1));
    assertNull(vw.getValueVector().getAccessor().getObject(2));

    vw = batchLoader.getValueAccessorById(
        NullableIntVector.class, //
        batchLoader.getValueVectorId(SchemaPath.getCompoundPath("non_existent", "nested","field")).getFieldIds() //
    );
    assertNull(vw.getValueVector().getAccessor().getObject(0));
    assertNull(vw.getValueVector().getAccessor().getObject(1));
    assertNull(vw.getValueVector().getAccessor().getObject(2));

    vw.getValueVector().clear();
    batch.release();
    batchLoader.clear();
  }

  private void testExistentColumns(RecordBatchLoader batchLoader, QueryResultBatch batch) throws SchemaChangeException {

    assertTrue(batchLoader.load(batch.getHeader().getDef(), batch.getData()));

    VectorWrapper<?> vw = batchLoader.getValueAccessorById(
        RepeatedBigIntVector.class, //
        batchLoader.getValueVectorId(SchemaPath.getCompoundPath("field_1")).getFieldIds() //
    );
    assertEquals("[1]", vw.getValueVector().getAccessor().getObject(0).toString());
    assertEquals("[5]", vw.getValueVector().getAccessor().getObject(1).toString());
    assertEquals("[5,10,15]", vw.getValueVector().getAccessor().getObject(2).toString());

    vw = batchLoader.getValueAccessorById(
        IntVector.class, //
        batchLoader.getValueVectorId(SchemaPath.getCompoundPath("field_3", "inner_1")).getFieldIds() //
    );
    assertNull(vw.getValueVector().getAccessor().getObject(0));
    assertEquals(2l, vw.getValueVector().getAccessor().getObject(1));
    assertEquals(5l, vw.getValueVector().getAccessor().getObject(2));

    vw = batchLoader.getValueAccessorById(
        IntVector.class, //
        batchLoader.getValueVectorId(SchemaPath.getCompoundPath("field_3", "inner_2")).getFieldIds() //
    );
    assertNull(vw.getValueVector().getAccessor().getObject(0));
    assertNull(vw.getValueVector().getAccessor().getObject(1));
    assertEquals(3l, vw.getValueVector().getAccessor().getObject(2));

    vw = batchLoader.getValueAccessorById(
        RepeatedBigIntVector.class, //
        batchLoader.getValueVectorId(SchemaPath.getCompoundPath("field_4", "inner_1")).getFieldIds() //
    );
    assertEquals("[]", vw.getValueVector().getAccessor().getObject(0).toString());
    assertEquals("[1,2,3]", vw.getValueVector().getAccessor().getObject(1).toString());
    assertEquals("[4,5,6]", vw.getValueVector().getAccessor().getObject(2).toString());
  }

  @Test
  public void testReader() throws Exception{
    final int repeatSize = 10;

    String simple = " { \"b\": \"hello\", \"c\": \"goodbye\"}\n " +
        "{ \"b\": \"yellow\", \"c\": \"red\", \"p\":" +
    "{ \"integer\" : 2001, \n" +
        "  \"float\"   : 1.2,\n" +
        "  \"x\": {\n" +
        "    \"y\": \"friends\",\n" +
        "    \"z\": \"enemies\"\n" +
        "  },\n" +
        "  \"z\": [\n" +
        "    {\"orange\" : \"black\" },\n" +
        "    {\"pink\" : \"purple\" }\n" +
        "  ]\n" +
        "  \n" +
        "}\n }";

    String compound = simple;
    for(int i =0; i < repeatSize; i++) compound += simple;

//    simple = "{ \"integer\" : 2001, \n" +
//        "  \"float\"   : 1.2\n" +
//        "}\n" +
//        "{ \"integer\" : -2002,\n" +
//        "  \"float\"   : -1.2 \n" +
//        "}";
    MapVector v = new MapVector("", allocator);
    ComplexWriterImpl writer = new ComplexWriterImpl("col", v);
    writer.allocate();

    DrillBuf buffer = allocator.buffer(255);
    JsonReaderWithState jsonReader = new JsonReaderWithState(new ReaderJSONRecordSplitter(compound), buffer,
        GroupScan.ALL_COLUMNS, false);
    int i =0;
    List<Integer> batchSizes = Lists.newArrayList();

    outside: while(true){
      writer.setPosition(i);
      switch(jsonReader.write(writer)){
      case WRITE_SUCCEED:
        i++;
        break;
      case NO_MORE:
        batchSizes.add(i);
        System.out.println("no more records - main loop");
        break outside;

      case WRITE_FAILED:
        System.out.println("==== hit bounds at " + i);
        //writer.setValueCounts(i - 1);
        batchSizes.add(i);
        i = 0;
        writer.allocate();
        writer.reset();

        switch(jsonReader.write(writer)){
        case NO_MORE:
          System.out.println("no more records - new alloc loop.");
          break outside;
        case WRITE_FAILED:
          throw new RuntimeException("Failure while trying to write.");
        case WRITE_SUCCEED:
          i++;
        };

      };
    }

    int total = 0;
    int lastRecordCount = 0;
    for(Integer records : batchSizes){
      total += records;
      lastRecordCount = records;
    }


    ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();

    ow.writeValueAsString(v.getAccessor().getObject(0));
    ow.writeValueAsString(v.getAccessor().getObject(1));
    FieldReader reader = v.get("col", MapVector.class).getAccessor().getReader();

    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    JsonWriter jsonWriter = new JsonWriter(stream, true);

    reader.setPosition(0);
    jsonWriter.write(reader);
    reader.setPosition(1);
    jsonWriter.write(reader);
    System.out.print("Json Read: ");
    System.out.println(new String(stream.toByteArray(), Charsets.UTF_8));
//    System.out.println(compound);

    System.out.println("Total Records Written " + batchSizes);

    reader.setPosition(lastRecordCount - 2);
    assertEquals("goodbye", reader.reader("c").readText().toString());
    reader.setPosition(lastRecordCount - 1);
    assertEquals("red", reader.reader("c").readText().toString());
    assertEquals((repeatSize+1) * 2, total);

    writer.clear();
    buffer.release();
  }
}
