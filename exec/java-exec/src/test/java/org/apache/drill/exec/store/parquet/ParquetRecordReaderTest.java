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
package org.apache.drill.exec.store.parquet;

import static org.apache.drill.exec.store.parquet.TestFileGenerator.intVals;
import static org.apache.drill.exec.store.parquet.TestFileGenerator.populateFieldInfoMap;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import mockit.Injectable;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.TopLevelAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.proto.BitControl;
import org.apache.drill.exec.proto.UserProtos;
import org.apache.drill.exec.proto.UserProtos.QueryType;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.rpc.user.UserServer;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.apache.drill.exec.store.CachedSingleFileSystem;
import org.apache.drill.exec.store.TestOutputMutator;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import parquet.bytes.BytesInput;
import parquet.column.page.Page;
import parquet.column.page.PageReadStore;
import parquet.column.page.PageReader;
import parquet.hadoop.CodecFactoryExposer;
import parquet.hadoop.Footer;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.schema.MessageType;

import com.google.common.base.Charsets;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

public class ParquetRecordReaderTest extends BaseTestQuery{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ParquetRecordReaderTest.class);

  static boolean VERBOSE_DEBUG = false;

  static final int numberRowGroups = 1;
  static final int recordsPerRowGroup = 300;
  static int DEFAULT_BYTES_PER_PAGE = 1024 * 1024 * 1;
  static final String fileName = "/tmp/parquet_test_file_many_types";

  @BeforeClass
  public static void generateFile() throws Exception{
    File f = new File(fileName);
    ParquetTestProperties props = new ParquetTestProperties(numberRowGroups, recordsPerRowGroup, DEFAULT_BYTES_PER_PAGE, new HashMap<String, FieldInfo>());
    populateFieldInfoMap(props);
    if(!f.exists()) TestFileGenerator.generateParquetFile(fileName, props);
  }


  @Test
  public void testMultipleRowGroupsAndReads3() throws Exception {
    String planName = "/parquet/parquet_scan_screen.json";
    testParquetFullEngineLocalPath(planName, fileName, 2, numberRowGroups, recordsPerRowGroup);
  }

  public String getPlanForFile(String pathFileName, String parquetFileName) throws IOException {
    return Files.toString(FileUtils.getResourceAsFile(pathFileName), Charsets.UTF_8)
        .replaceFirst("&REPLACED_IN_PARQUET_TEST&", parquetFileName);
  }

  @Test
  public void testMultipleRowGroupsAndReads2() throws Exception {
    String readEntries;
    readEntries = "";
    // number of times to read the file
    int i = 3;
    for (int j = 0; j < i; j++){
      readEntries += "\""+fileName+"\"";
      if (j < i - 1)
        readEntries += ",";
    }
    String planText = Files.toString(FileUtils.getResourceAsFile("/parquet/parquet_scan_screen_read_entry_replace.json"), Charsets.UTF_8).replaceFirst( "&REPLACED_IN_PARQUET_TEST&", readEntries);
    testParquetFullEngineLocalText(planText, fileName, i, numberRowGroups, recordsPerRowGroup);
  }

  @Test
  public void testLocalDistributed() throws Exception {
    String planName = "/parquet/parquet_scan_union_screen_physical.json";
    testParquetFullEngineLocalTextDistributed(planName, fileName, 1, numberRowGroups, recordsPerRowGroup);
  }

  @Test
  @Ignore
  public void testRemoteDistributed() throws Exception {
    String planName = "/parquet/parquet_scan_union_screen_physical.json";
    testParquetFullEngineRemote(planName, fileName, 1, numberRowGroups, recordsPerRowGroup);
  }


  public void testParquetFullEngineLocalPath(String planFileName, String filename, int numberOfTimesRead /* specified in json plan */, int numberOfRowGroups, int recordsPerRowGroup) throws Exception{
    testParquetFullEngineLocalText(Files.toString(FileUtils.getResourceAsFile(planFileName), Charsets.UTF_8), filename, numberOfTimesRead, numberOfRowGroups, recordsPerRowGroup);
  }

  //specific tests should call this method, but it is not marked as a test itself intentionally
  public void testParquetFullEngineLocalText(String planText, String filename, int numberOfTimesRead /* specified in json plan */, int numberOfRowGroups, int recordsPerRowGroup) throws Exception{
    testFull(QueryType.LOGICAL, planText, filename, numberOfTimesRead, numberOfRowGroups, recordsPerRowGroup);
  }

  private void testFull(QueryType type, String planText, String filename, int numberOfTimesRead /* specified in json plan */, int numberOfRowGroups, int recordsPerRowGroup) throws Exception{

//    RecordBatchLoader batchLoader = new RecordBatchLoader(getAllocator());
    HashMap<String, FieldInfo> fields = new HashMap<>();
    ParquetTestProperties props = new ParquetTestProperties(numberRowGroups, recordsPerRowGroup, DEFAULT_BYTES_PER_PAGE, fields);
    TestFileGenerator.populateFieldInfoMap(props);
    ParquetResultListener resultListener = new ParquetResultListener(getAllocator(), props, numberOfTimesRead, true);
    Stopwatch watch = new Stopwatch().start();
    testWithListener(type, planText, resultListener);
    resultListener.getResults();
//    batchLoader.clear();
    System.out.println(String.format("Took %d ms to run query", watch.elapsed(TimeUnit.MILLISECONDS)));

  }


  //use this method to submit physical plan
  public void testParquetFullEngineLocalTextDistributed(String planName, String filename, int numberOfTimesRead /* specified in json plan */, int numberOfRowGroups, int recordsPerRowGroup) throws Exception{
    String planText = Files.toString(FileUtils.getResourceAsFile(planName), Charsets.UTF_8);
    testFull(QueryType.PHYSICAL, planText, filename, numberOfTimesRead, numberOfRowGroups, recordsPerRowGroup);
  }

  public String pad(String value, int length) {
    return pad(value, length, " ");
  }

  public String pad(String value, int length, String with) {
    StringBuilder result = new StringBuilder(length);
    result.append(value);

    while (result.length() < length) {
      result.insert(0, with);
    }

    return result.toString();
  }

  public void testParquetFullEngineRemote(String plan, String filename, int numberOfTimesRead /* specified in json plan */, int numberOfRowGroups, int recordsPerRowGroup) throws Exception{
    HashMap<String, FieldInfo> fields = new HashMap<>();
    ParquetTestProperties props = new ParquetTestProperties(numberRowGroups, recordsPerRowGroup, DEFAULT_BYTES_PER_PAGE, fields);
    TestFileGenerator.populateFieldInfoMap(props);
    ParquetResultListener resultListener = new ParquetResultListener(getAllocator(), props, numberOfTimesRead, true);
    testWithListener(UserProtos.QueryType.PHYSICAL, Files.toString(FileUtils.getResourceAsFile(plan), Charsets.UTF_8), resultListener);
    resultListener.getResults();
  }

  class MockOutputMutator implements OutputMutator {
    List<MaterializedField> removedFields = Lists.newArrayList();
    List<ValueVector> addFields = Lists.newArrayList();


    List<MaterializedField> getRemovedFields() {
      return removedFields;
    }

    List<ValueVector> getAddFields() {
      return addFields;
    }

    @Override
    public void addFields(List<ValueVector> vv) {
      return;
    }

    @Override
    public <T extends ValueVector> T addField(MaterializedField field, Class<T> clazz) throws SchemaChangeException {
      return null;
    }

    @Override
    public void allocate(int recordCount) {

    }

    @Override
    public boolean isNewSchema() {
      return false;
    }
  }


  private void validateFooters(final List<Footer> metadata) {
    logger.debug(metadata.toString());
    assertEquals(3, metadata.size());
    for (Footer footer : metadata) {
      final File file = new File(footer.getFile().toUri());
      assertTrue(file.getName(), file.getName().startsWith("part"));
      assertTrue(file.getPath(), file.exists());
      final ParquetMetadata parquetMetadata = footer.getParquetMetadata();
      assertEquals(2, parquetMetadata.getBlocks().size());
      final Map<String, String> keyValueMetaData = parquetMetadata.getFileMetaData().getKeyValueMetaData();
      assertEquals("bar", keyValueMetaData.get("foo"));
      assertEquals(footer.getFile().getName(), keyValueMetaData.get(footer.getFile().getName()));
    }
  }


  private void validateContains(MessageType schema, PageReadStore pages, String[] path, int values, BytesInput bytes)
      throws IOException {
    PageReader pageReader = pages.getPageReader(schema.getColumnDescription(path));
    Page page = pageReader.readPage();
    assertEquals(values, page.getValueCount());
    assertArrayEquals(bytes.toByteArray(), page.getBytes().toByteArray());
  }


  @Test
  public void testMultipleRowGroups() throws Exception {
    HashMap<String, FieldInfo> fields = new HashMap<>();
    ParquetTestProperties props = new ParquetTestProperties(3, 3000, DEFAULT_BYTES_PER_PAGE, fields);
    populateFieldInfoMap(props);
    testParquetFullEngineEventBased(true, "/parquet/parquet_scan_screen.json", "/tmp/test.parquet", 1, props);
  }

  // TODO - Test currently marked ignore to prevent breaking of the build process, requires a binary file that was
  // generated using pig. Will need to find a good place to keep files like this.
  // For now I will upload it to the JIRA as an attachment.
  @Ignore
  @Test
  public void testNullableColumns() throws Exception {
    HashMap<String, FieldInfo> fields = new HashMap<>();
    ParquetTestProperties props = new ParquetTestProperties(1, 3000000, DEFAULT_BYTES_PER_PAGE, fields);
    Object[] boolVals = {true, null, null};
    props.fields.put("a", new FieldInfo("boolean", "a", 1, boolVals, TypeProtos.MinorType.BIT, props));
    testParquetFullEngineEventBased(false, "/parquet/parquet_nullable.json", "/tmp/nullable_test.parquet", 1, props);
  }

  @Ignore
  @Test
  /**
   * Tests the reading of nullable var length columns, runs the tests twice, once on a file that has
   * a converted type of UTF-8 to make sure it can be read
   */
  public void testNullableColumnsVarLen() throws Exception {
    HashMap<String, FieldInfo> fields = new HashMap<>();
    ParquetTestProperties props = new ParquetTestProperties(1, 3000000, DEFAULT_BYTES_PER_PAGE, fields);
    byte[] val = {'b'};
    byte[] val2 = {'b', '2'};
    byte[] val3 = {'b', '3'};
    byte[] val4 = { 'l','o','n','g','e','r',' ','s','t','r','i','n','g'};
    Object[] boolVals = { val, val2, val4};
    props.fields.put("a", new FieldInfo("boolean", "a", 1, boolVals, TypeProtos.MinorType.BIT, props));
    testParquetFullEngineEventBased(false, "/parquet/parquet_nullable_varlen.json", "/tmp/nullable_varlen.parquet", 1, props);
    fields.clear();
    // pass strings instead of byte arrays
    Object[] boolVals2 = { new org.apache.hadoop.io.Text("b"), new org.apache.hadoop.io.Text("b2"),
        new org.apache.hadoop.io.Text("b3")};
    props.fields.put("a", new FieldInfo("boolean", "a", 1, boolVals2, TypeProtos.MinorType.BIT, props));
    testParquetFullEngineEventBased(false, "/parquet/parquet_scan_screen_read_entry_replace.json",
        "\"/tmp/varLen.parquet/a\"", "unused", 1, props);
  }

  @Ignore
  @Test
  public void testDictionaryEncoding() throws Exception {
    HashMap<String, FieldInfo> fields = new HashMap<>();
    ParquetTestProperties props = new ParquetTestProperties(1, 25, DEFAULT_BYTES_PER_PAGE, fields);
    Object[] boolVals = null;
    props.fields.put("n_name", null);
    props.fields.put("n_nationkey", null);
    props.fields.put("n_regionkey", null);
    props.fields.put("n_comment", null);
    testParquetFullEngineEventBased(false, false, "/parquet/parquet_scan_screen_read_entry_replace.json",
        "\"/tmp/nation_dictionary_fail.parquet\"", "unused", 1, props, true);

    fields = new HashMap<>();
    props = new ParquetTestProperties(1, 5, DEFAULT_BYTES_PER_PAGE, fields);
    props.fields.put("employee_id", null);
    props.fields.put("name", null);
    props.fields.put("role", null);
    props.fields.put("phone", null);
    props.fields.put("password_hash", null);
    props.fields.put("gender_male", null);
    props.fields.put("height", null);
    props.fields.put("hair_thickness", null);
    testParquetFullEngineEventBased(false, false, "/parquet/parquet_scan_screen_read_entry_replace.json",
        "\"/tmp/employees_5_16_14.parquet\"", "unused", 1, props, true);
  }

  @Test
  public void testMultipleRowGroupsAndReads() throws Exception {
    HashMap<String, FieldInfo> fields = new HashMap<>();
    ParquetTestProperties props = new ParquetTestProperties(4, 3000, DEFAULT_BYTES_PER_PAGE, fields);
    populateFieldInfoMap(props);
    String readEntries = "";
    // number of times to read the file
    int i = 3;
    for (int j = 0; j < i; j++){
      readEntries += "\"/tmp/test.parquet\"";
      if (j < i - 1)
        readEntries += ",";
    }
    testParquetFullEngineEventBased(true, "/parquet/parquet_scan_screen_read_entry_replace.json", readEntries,
        "/tmp/test.parquet", i, props);
  }


  @Ignore
  @Test
  public void testReadBug_Drill_418() throws Exception {
    HashMap<String, FieldInfo> fields = new HashMap<>();
    ParquetTestProperties props = new ParquetTestProperties(5, 300000, DEFAULT_BYTES_PER_PAGE, fields);
    TestFileGenerator.populateDrill_418_fields(props);
    String readEntries = "\"/tmp/customer.plain.parquet\"";
    testParquetFullEngineEventBased(false, false, "/parquet/parquet_scan_screen_read_entry_replace.json", readEntries,
        "unused, no file is generated", 1, props, true);
  }

  // requires binary file generated by pig from TPCH data, also have to disable assert where data is coming in
  @Ignore
  @Test
  public void testMultipleRowGroupsAndReadsPigError() throws Exception {
    HashMap<String, FieldInfo> fields = new HashMap<>();
    ParquetTestProperties props = new ParquetTestProperties(5, 300000, DEFAULT_BYTES_PER_PAGE, fields);
    TestFileGenerator.populatePigTPCHCustomerFields(props);
    String readEntries = "\"/tmp/tpc-h/customer\"";
    testParquetFullEngineEventBased(false, false, "/parquet/parquet_scan_screen_read_entry_replace.json", readEntries,
        "unused, no file is generated", 1, props, true);

    fields = new HashMap();
    props = new ParquetTestProperties(5, 300000, DEFAULT_BYTES_PER_PAGE, fields);
    TestFileGenerator.populatePigTPCHSupplierFields(props);
    readEntries = "\"/tmp/tpc-h/supplier\"";
    testParquetFullEngineEventBased(false, false, "/parquet/parquet_scan_screen_read_entry_replace.json", readEntries,
        "unused, no file is generated", 1, props, true);
  }

  @Test
  public void testMultipleRowGroupsEvent() throws Exception {
    HashMap<String, FieldInfo> fields = new HashMap<>();
    ParquetTestProperties props = new ParquetTestProperties(4, 3000, DEFAULT_BYTES_PER_PAGE, fields);
    populateFieldInfoMap(props);
    testParquetFullEngineEventBased(true, "/parquet/parquet_scan_screen.json", "/tmp/test.parquet", 1, props);
  }


  /**
   * Tests the attribute in a scan node to limit the columns read by a scan.
   *
   * The functionality of selecting all columns is tested in all of the other tests that leave out the attribute.
   * @throws Exception
   */
  @Test
  public void testSelectColumnRead() throws Exception {
    HashMap<String, FieldInfo> fields = new HashMap<>();
    ParquetTestProperties props = new ParquetTestProperties(4, 3000, DEFAULT_BYTES_PER_PAGE, fields);
    // generate metatdata for a series of test columns, these columns are all generated in the test file
    populateFieldInfoMap(props);
    TestFileGenerator.generateParquetFile("/tmp/test.parquet", props);
    fields.clear();
    // create a new object to describe the dataset expected out of the scan operation
    // the fields added below match those requested in the plan specified in parquet_selective_column_read.json
    // that is used below in the test query
    props = new ParquetTestProperties(4, 3000, DEFAULT_BYTES_PER_PAGE, fields);
    props.fields.put("integer", new FieldInfo("int32", "integer", 32, TestFileGenerator.intVals, TypeProtos.MinorType.INT, props));
    props.fields.put("bigInt", new FieldInfo("int64", "bigInt", 64, TestFileGenerator.longVals, TypeProtos.MinorType.BIGINT, props));
    props.fields.put("bin", new FieldInfo("binary", "bin", -1, TestFileGenerator.binVals, TypeProtos.MinorType.VARBINARY, props));
    props.fields.put("bin2", new FieldInfo("binary", "bin2", -1, TestFileGenerator.bin2Vals, TypeProtos.MinorType.VARBINARY, props));
    testParquetFullEngineEventBased(true, false, "/parquet/parquet_selective_column_read.json", null, "/tmp/test.parquet", 1, props, false);
  }

  public static void main(String[] args) throws Exception{
    // TODO - not sure why this has a main method, test below can be run directly
    //new ParquetRecordReaderTest().testPerformance();
  }

  @Test
  @Ignore
  public void testPerformance(@Injectable final DrillbitContext bitContext,
                              @Injectable UserServer.UserClientConnection connection) throws Exception {
    DrillConfig c = DrillConfig.create();
    FunctionImplementationRegistry registry = new FunctionImplementationRegistry(c);
    FragmentContext context = new FragmentContext(bitContext, BitControl.PlanFragment.getDefaultInstance(), connection, registry);

//    new NonStrictExpectations() {
//      {
//        context.getAllocator(); result = BufferAllocator.getAllocator(DrillConfig.create());
//      }
//    };

    final String fileName = "/tmp/parquet_test_performance.parquet";
    HashMap<String, FieldInfo> fields = new HashMap<>();
    ParquetTestProperties props = new ParquetTestProperties(1, 20 * 1000 * 1000, DEFAULT_BYTES_PER_PAGE, fields);
    populateFieldInfoMap(props);
    //generateParquetFile(fileName, props);

    Configuration dfsConfig = new Configuration();
    List<Footer> footers = ParquetFileReader.readFooters(dfsConfig, new Path(fileName));
    Footer f = footers.iterator().next();

    List<SchemaPath> columns = Lists.newArrayList();
    columns.add(new SchemaPath("_MAP.integer", ExpressionPosition.UNKNOWN));
    columns.add(new SchemaPath("_MAP.bigInt", ExpressionPosition.UNKNOWN));
    columns.add(new SchemaPath("_MAP.f", ExpressionPosition.UNKNOWN));
    columns.add(new SchemaPath("_MAP.d", ExpressionPosition.UNKNOWN));
    columns.add(new SchemaPath("_MAP.b", ExpressionPosition.UNKNOWN));
    columns.add(new SchemaPath("_MAP.bin", ExpressionPosition.UNKNOWN));
    columns.add(new SchemaPath("_MAP.bin2", ExpressionPosition.UNKNOWN));
    int totalRowCount = 0;

    FileSystem fs = new CachedSingleFileSystem(fileName);
    BufferAllocator allocator = new TopLevelAllocator();
    for(int i = 0; i < 25; i++){
      ParquetRecordReader rr = new ParquetRecordReader(context, 256000, fileName, 0, fs,
          new CodecFactoryExposer(dfsConfig), f.getParquetMetadata(), columns);
      TestOutputMutator mutator = new TestOutputMutator(allocator);
      rr.setup(mutator);
      Stopwatch watch = new Stopwatch();
      watch.start();

      int rowCount = 0;
      while ((rowCount = rr.next()) > 0) {
        totalRowCount += rowCount;
      }
      System.out.println(String.format("Time completed: %s. ", watch.elapsed(TimeUnit.MILLISECONDS)));
      rr.cleanup();
    }

    allocator.close();
    System.out.println(String.format("Total row count %s", totalRowCount));
  }

  // specific tests should call this method, but it is not marked as a test itself intentionally
  public void testParquetFullEngineEventBased(boolean generateNew, String plan, String readEntries, String filename,
                                              int numberOfTimesRead /* specified in json plan */, ParquetTestProperties props) throws Exception{
    testParquetFullEngineEventBased(true, generateNew, plan, readEntries,filename,
                                              numberOfTimesRead /* specified in json plan */, props, true);
  }


  // specific tests should call this method, but it is not marked as a test itself intentionally
  public void testParquetFullEngineEventBased(boolean generateNew, String plan, String filename, int numberOfTimesRead /* specified in json plan */, ParquetTestProperties props) throws Exception{
    testParquetFullEngineEventBased(true, generateNew, plan, null, filename, numberOfTimesRead, props, true);
  }

  // specific tests should call this method, but it is not marked as a test itself intentionally
  public void testParquetFullEngineEventBased(boolean testValues, boolean generateNew, String plan, String readEntries, String filename,
                                              int numberOfTimesRead /* specified in json plan */, ParquetTestProperties props,
                                              boolean runAsLogicalPlan) throws Exception{
    if (generateNew) TestFileGenerator.generateParquetFile(filename, props);

    ParquetResultListener resultListener = new ParquetResultListener(getAllocator(), props, numberOfTimesRead, testValues);
    long C = System.nanoTime();
    String planText = Files.toString(FileUtils.getResourceAsFile(plan), Charsets.UTF_8);
    // substitute in the string for the read entries, allows reuse of the plan file for several tests
    if (readEntries != null) {
      planText = planText.replaceFirst( "&REPLACED_IN_PARQUET_TEST&", readEntries);
    }
    if (runAsLogicalPlan){
      this.testWithListener(QueryType.LOGICAL, planText, resultListener);
    }else{
      this.testWithListener(QueryType.PHYSICAL, planText, resultListener);
    }
    resultListener.getResults();
    long D = System.nanoTime();
    System.out.println(String.format("Took %f s to run query", (float)(D-C) / 1E9));
  }

}
