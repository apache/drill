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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static parquet.column.Encoding.PLAIN;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserProtos;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.user.ConnectionThrottle;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.rpc.user.UserResultsListener;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.apache.drill.exec.store.ByteArrayUtil;
import org.apache.drill.exec.vector.BaseDataValueVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Ignore;
import org.junit.Test;

import parquet.bytes.BytesInput;
import parquet.column.ColumnDescriptor;
import parquet.hadoop.ParquetFileWriter;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;

import com.beust.jcommander.internal.Lists;
import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.google.common.util.concurrent.SettableFuture;

public class ParquetRecordReaderTest2 {
  org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ParquetRecordReaderTest2.class);

  private static final boolean VERBOSE_DEBUG = false;

  // { 00000001, 00000010, 00000100, 00001000, 00010000, ... }
  byte[] bitFields = {1, 2, 4, 8, 16, 32, 64, -128};
  byte allBitsTrue = -1;
  byte allBitsFalse = 0;
  int DEFAULT_BYTES_PER_PAGE = 1024 * 1024 * 1;
  static Object[] intVals = {-200, 100, Integer.MAX_VALUE };
  static Object[] longVals = { -5000l, 5000l, Long.MAX_VALUE};
  static Object[] floatVals = { 1.74f, Float.MAX_VALUE, Float.MIN_VALUE};
  static Object[] doubleVals = {100.45d, Double.MAX_VALUE, Double.MIN_VALUE,};
  static Object[] boolVals = {false, false, true};
  static byte[] varLen1 = {50, 51, 52, 53, 54, 55, 56, 57, 58, 59};
  static byte[] varLen2 = {15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1};
  static byte[] varLen3 = {100, 99, 98};
  static Object[] binVals = { varLen3, varLen2, varLen3};
  static Object[] bin2Vals = { varLen3, varLen2, varLen1};

  private void populateFieldInfoMap(ParquetTestProperties props){
    props.fields.put("integer", new FieldInfo("int32", "integer", 32, intVals, TypeProtos.MinorType.INT, props));
    props.fields.put("bigInt", new FieldInfo("int64", "bigInt", 64, longVals, TypeProtos.MinorType.BIGINT, props));
    props.fields.put("f", new FieldInfo("float", "f", 32, floatVals, TypeProtos.MinorType.FLOAT4, props));
    props.fields.put("d", new FieldInfo("double", "d", 64, doubleVals, TypeProtos.MinorType.FLOAT8, props));
    props.fields.put("b", new FieldInfo("boolean", "b", 1, boolVals, TypeProtos.MinorType.BIT, props));
    props.fields.put("bin", new FieldInfo("binary", "bin", -1, binVals, TypeProtos.MinorType.VARBINARY, props));
    props.fields.put("bin2", new FieldInfo("binary", "bin2", -1, bin2Vals, TypeProtos.MinorType.VARBINARY, props));
  }

  private void populatePigTPCHCustomerFields(ParquetTestProperties props){
    // all of the data in the fieldInfo constructors doesn't matter because the file is generated outside the test
    props.fields.put("C_CUSTKEY", new FieldInfo("int32", "integer", 32, intVals, TypeProtos.MinorType.INT, props));
    props.fields.put("C_NATIONKEY", new FieldInfo("int64", "bigInt", 64, longVals, TypeProtos.MinorType.BIGINT, props));
    props.fields.put("C_ACCTBAL", new FieldInfo("float", "f", 32, floatVals, TypeProtos.MinorType.FLOAT4, props));
    props.fields.put("C_NAME", new FieldInfo("double", "d", 64, doubleVals, TypeProtos.MinorType.FLOAT8, props));
    props.fields.put("C_ADDRESS", new FieldInfo("boolean", "b", 1, boolVals, TypeProtos.MinorType.BIT, props));
    props.fields.put("C_PHONE", new FieldInfo("binary", "bin", -1, binVals, TypeProtos.MinorType.VARBINARY, props));
    props.fields.put("C_MKTSEGMENT", new FieldInfo("binary", "bin2", -1, bin2Vals, TypeProtos.MinorType.VARBINARY, props));
    props.fields.put("C_COMMENT", new FieldInfo("binary", "bin2", -1, bin2Vals, TypeProtos.MinorType.VARBINARY, props));
  }

  private void populatePigTPCHSupplierFields(ParquetTestProperties props){
    // all of the data in the fieldInfo constructors doesn't matter because the file is generated outside the test
    props.fields.put("S_SUPPKEY", new FieldInfo("int32", "integer", 32, intVals, TypeProtos.MinorType.INT, props));
    props.fields.put("S_NATIONKEY", new FieldInfo("int64", "bigInt", 64, longVals, TypeProtos.MinorType.BIGINT, props));
    props.fields.put("S_ACCTBAL", new FieldInfo("float", "f", 32, floatVals, TypeProtos.MinorType.FLOAT4, props));
    props.fields.put("S_NAME", new FieldInfo("double", "d", 64, doubleVals, TypeProtos.MinorType.FLOAT8, props));
    props.fields.put("S_ADDRESS", new FieldInfo("boolean", "b", 1, boolVals, TypeProtos.MinorType.BIT, props));
    props.fields.put("S_PHONE", new FieldInfo("binary", "bin", -1, binVals, TypeProtos.MinorType.VARBINARY, props));
    props.fields.put("S_COMMENT", new FieldInfo("binary", "bin2", -1, bin2Vals, TypeProtos.MinorType.VARBINARY, props));
  }

  @Test
  public void testMultipleRowGroups() throws Exception {
    HashMap<String, FieldInfo> fields = new HashMap<>();
    ParquetTestProperties props = new ParquetTestProperties(3, 3000, DEFAULT_BYTES_PER_PAGE, fields);
    populateFieldInfoMap(props);
    testParquetFullEngine(true, "/parquet_scan_screen.json", "/tmp/test.parquet", 1, props);
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
    testParquetFullEngine(false, "/parquet_nullable.json", "/tmp/nullable.parquet", 1, props);
  }

  @Ignore
  @Test
  public void testNullableColumnsVarLen() throws Exception {
    HashMap<String, FieldInfo> fields = new HashMap<>();
    ParquetTestProperties props = new ParquetTestProperties(1, 3000000, DEFAULT_BYTES_PER_PAGE, fields);
    byte[] val = {'b'};
//    Object[] boolVals = { val, null, null};
//    Object[] boolVals = { null, null, null};
    Object[] boolVals = { val, val, val};
    props.fields.put("a", new FieldInfo("boolean", "a", 1, boolVals, TypeProtos.MinorType.BIT, props));
    testParquetFullEngine(false, "/parquet_nullable_varlen.json", "/tmp/nullable.parquet", 1, props);
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
    generateParquetFile("/tmp/test.parquet", props);
    fields.clear();
    // create a new object to describe the dataset expected out of the scan operation
    // the fields added below match those requested in the plan specified in parquet_selective_column_read.json
    // that is used below in the test query
    props = new ParquetTestProperties(4, 3000, DEFAULT_BYTES_PER_PAGE, fields);
    props.fields.put("integer", new FieldInfo("int32", "integer", 32, intVals, TypeProtos.MinorType.INT, props));
    props.fields.put("bigInt", new FieldInfo("int64", "bigInt", 64, longVals, TypeProtos.MinorType.BIGINT, props));
    props.fields.put("bin", new FieldInfo("binary", "bin", -1, binVals, TypeProtos.MinorType.VARBINARY, props));
    props.fields.put("bin2", new FieldInfo("binary", "bin2", -1, bin2Vals, TypeProtos.MinorType.VARBINARY, props));
    testParquetFullEngineEventBased(false, "/parquet_selective_column_read.json", null, "/tmp/test.parquet", 1, props, false);
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
      readEntries += "{path: \"/tmp/test.parquet\"}";
      if (j < i - 1)
        readEntries += ",";
    }
    testParquetFullEngineEventBased(true, "/parquet_scan_screen_read_entry_replace.json", readEntries,
        "/tmp/test.parquet", i, props, true);
  }

  // requires binary file generated by pig from TPCH data, also have to disable assert where data is coming in
  @Ignore
  @Test
  public void testMultipleRowGroupsAndReadsPigError() throws Exception {
    HashMap<String, FieldInfo> fields = new HashMap<>();
    ParquetTestProperties props = new ParquetTestProperties(4, 3000, DEFAULT_BYTES_PER_PAGE, fields);
    populatePigTPCHCustomerFields(props);
//    populatePigTPCHSupplierFields(props);
    String readEntries = "";
    // number of times to read the file
    int i = 1;
    for (int j = 0; j < i; j++){
      readEntries += "{path: \"/tmp/tpc-h/customer\"}";
      if (j < i - 1)
        readEntries += ",";
    }
    testParquetFullEngineEventBased(false, "/parquet_scan_screen_read_entry_replace.json", readEntries,
        "/tmp/test.parquet", i, props, true);
  }
/*
  @Test
  public void testMultipleRowGroupsEvent() throws Exception {
    HashMap<String, FieldInfo> fields = new HashMap<>();
    ParquetTestProperties props = new ParquetTestProperties(10, 30000, DEFAULT_BYTES_PER_PAGE, fields);
    props.fields.put("a", new FieldInfo("a", "asdf", 1, new Object[3], TypeProtos.MinorType.BIGINT, props));
    testParquetFullEngineEventBased(false, "/parquet_scan_screen.json", "/tmp/out", 1, props);
  }

*/
  private class ParquetTestProperties{
    int numberRowGroups;
    int recordsPerRowGroup;
    int bytesPerPage = 1024 * 1024 * 1;
    HashMap<String, FieldInfo> fields = new HashMap<>();

    public ParquetTestProperties(int numberRowGroups, int recordsPerRowGroup, int bytesPerPage,
                                 HashMap<String, FieldInfo> fields){
      this.numberRowGroups = numberRowGroups;
      this.recordsPerRowGroup = recordsPerRowGroup;
      this.bytesPerPage = bytesPerPage;
      this.fields = fields;
    }

  }

  private static class FieldInfo {

    String parquetType;
    String name;
    int bitLength;
    int numberOfPages;
    Object[] values;
    TypeProtos.MinorType type;

    FieldInfo(String parquetType, String name, int bitLength, Object[] values, TypeProtos.MinorType type, ParquetTestProperties props){
      this.parquetType = parquetType;
      this.name = name;
      this.bitLength  = bitLength;
      this.numberOfPages = Math.max(1, (int) Math.ceil( ((long) props.recordsPerRowGroup) * bitLength / 8.0 / props.bytesPerPage));
      this.values = values;
      // generator is designed to use 3 values
      assert values.length == 3;
      this.type = type;
    }
  }

  private String getResource(String resourceName) {
    return "resource:" + resourceName;
  }

  public void generateParquetFile(String filename, ParquetTestProperties props) throws Exception {

    int currentBooleanByte = 0;
    WrapAroundCounter booleanBitCounter = new WrapAroundCounter(7);

    Configuration configuration = new Configuration();
    configuration.set("fs.default.name", "file:///");
    //"message m { required int32 integer; required int64 integer64; required boolean b; required float f; required double d;}"

    FileSystem fs = FileSystem.get(configuration);
    Path path = new Path(filename);
    if (fs.exists(path)) fs.delete(path, false);


    String messageSchema = "message m {";
    for (FieldInfo fieldInfo : props.fields.values()) {
      messageSchema += " required " + fieldInfo.parquetType + " " + fieldInfo.name + ";";
    }
    // remove the last semicolon, java really needs a join method for strings...
    // TODO - nvm apparently it requires a semicolon after every field decl, might want to file a bug
    //messageSchema = messageSchema.substring(schemaType, messageSchema.length() - 1);
    messageSchema += "}";

    MessageType schema = MessageTypeParser.parseMessageType(messageSchema);

    CompressionCodecName codec = CompressionCodecName.UNCOMPRESSED;
    ParquetFileWriter w = new ParquetFileWriter(configuration, schema, path);
    w.start();
    HashMap<String, Integer> columnValuesWritten = new HashMap();
    int valsWritten;
    for (int k = 0; k < props.numberRowGroups; k++){
      w.startBlock(1);
      currentBooleanByte = 0;
      booleanBitCounter.reset();

      for (FieldInfo fieldInfo : props.fields.values()) {

        if ( ! columnValuesWritten.containsKey(fieldInfo.name)){
          columnValuesWritten.put((String) fieldInfo.name, 0);
          valsWritten = 0;
        } else {
          valsWritten = columnValuesWritten.get(fieldInfo.name);
        }

        String[] path1 = {(String) fieldInfo.name};
        ColumnDescriptor c1 = schema.getColumnDescription(path1);

        w.startColumn(c1, props.recordsPerRowGroup, codec);
        int valsPerPage = (int) Math.ceil(props.recordsPerRowGroup / (float) fieldInfo.numberOfPages);
        byte[] bytes;
        // for variable length binary fields
        int bytesNeededToEncodeLength = 4;
        if ((int) fieldInfo.bitLength > 0) {
          bytes = new byte[(int) Math.ceil(valsPerPage * (int) fieldInfo.bitLength / 8.0)];
        } else {
          // the twelve at the end is to account for storing a 4 byte length with each value
          int totalValLength = ((byte[]) fieldInfo.values[0]).length + ((byte[]) fieldInfo.values[1]).length + ((byte[]) fieldInfo.values[2]).length + 3 * bytesNeededToEncodeLength;
          // used for the case where there is a number of values in this row group that is not divisible by 3
          int leftOverBytes = 0;
          if ( valsPerPage % 3 > 0 ) leftOverBytes += ((byte[])fieldInfo.values[1]).length + bytesNeededToEncodeLength;
          if ( valsPerPage % 3 > 1 ) leftOverBytes += ((byte[])fieldInfo.values[2]).length + bytesNeededToEncodeLength;
          bytes = new byte[valsPerPage / 3 * totalValLength + leftOverBytes];
        }
        int bytesPerPage = (int) (valsPerPage * ((int) fieldInfo.bitLength / 8.0));
        int bytesWritten = 0;
        for (int z = 0; z < (int) fieldInfo.numberOfPages; z++, bytesWritten = 0) {
          for (int i = 0; i < valsPerPage; i++) {
            //System.out.print(i + ", " + (i % 25 == 0 ? "\n gen " + fieldInfo.name + ": " : ""));
            if (fieldInfo.values[0] instanceof Boolean) {

              bytes[currentBooleanByte] |= bitFields[booleanBitCounter.val] & ((boolean) fieldInfo.values[valsWritten % 3]
                  ? allBitsTrue : allBitsFalse);
              booleanBitCounter.increment();
              if (booleanBitCounter.val == 0) {
                currentBooleanByte++;
              }
              valsWritten++;
              if (currentBooleanByte > bytesPerPage) break;
            } else {
              if (fieldInfo.values[valsWritten % 3] instanceof byte[]){
                System.arraycopy(ByteArrayUtil.toByta(((byte[])fieldInfo.values[valsWritten % 3]).length),
                    0, bytes, bytesWritten, bytesNeededToEncodeLength);
                try{
                System.arraycopy(fieldInfo.values[valsWritten % 3],
                    0, bytes, bytesWritten + bytesNeededToEncodeLength, ((byte[])fieldInfo.values[valsWritten % 3]).length);
                }
                catch (Exception ex){
                  Math.min(4, 5);
                }
                bytesWritten += ((byte[])fieldInfo.values[valsWritten % 3]).length + bytesNeededToEncodeLength;
              }
              else{
                System.arraycopy( ByteArrayUtil.toByta(fieldInfo.values[valsWritten % 3]),
                    0, bytes, i * ((int) fieldInfo.bitLength / 8), (int) fieldInfo.bitLength / 8);
              }
              valsWritten++;
            }

          }
          w.writeDataPage((int) (props.recordsPerRowGroup / (int) fieldInfo.numberOfPages), bytes.length, BytesInput.from(bytes), PLAIN, PLAIN, PLAIN);
          currentBooleanByte = 0;
        }
        w.endColumn();
        columnValuesWritten.remove((String) fieldInfo.name);
        columnValuesWritten.put((String) fieldInfo.name, valsWritten);
      }

      w.endBlock();
    }
    w.end(new HashMap<String, String>());
    logger.debug("Finished generating parquet file.");
  }

  private class ParquetResultListener implements UserResultsListener {
    private SettableFuture<Void> future = SettableFuture.create();
    int count = 0;
    RecordBatchLoader batchLoader;

    int batchCounter = 1;
    HashMap<String, Integer> valuesChecked = new HashMap();
    ParquetTestProperties props;

    ParquetResultListener(RecordBatchLoader batchLoader, ParquetTestProperties props){
      this.batchLoader = batchLoader;
      this.props = props;
    }

    @Override
    public void submissionFailed(RpcException ex) {
      logger.debug("Submission failed.", ex);
      future.setException(ex);
    }

    @Override
    public void resultArrived(QueryResultBatch result, ConnectionThrottle throttle) {
      logger.debug("result arrived in test batch listener.");
      if(result.getHeader().getIsLastChunk()){
        future.set(null);
      }
      int columnValCounter = 0;
      int i = 0;
      FieldInfo currentField;
      count += result.getHeader().getRowCount();
      boolean schemaChanged = false;
      try {
        schemaChanged = batchLoader.load(result.getHeader().getDef(), result.getData());
      } catch (SchemaChangeException e) {
        throw new RuntimeException(e);
      }

      int recordCount = 0;
      // print headers.
      if (schemaChanged) {
      } // do not believe any change is needed for when the schema changes, with the current mock scan use case

      for (VectorWrapper vw : batchLoader) {
        ValueVector vv = vw.getValueVector();
        currentField = props.fields.get(vv.getField().getName());
        if (VERBOSE_DEBUG){
          System.out.println("\n" + (String) currentField.name);
        }
        if ( ! valuesChecked.containsKey(vv.getField().getName())){
          valuesChecked.put(vv.getField().getName(), 0);
          columnValCounter = 0;
        } else {
          columnValCounter = valuesChecked.get(vv.getField().getName());
        }
        for (int j = 0; j < ((BaseDataValueVector)vv).getAccessor().getValueCount(); j++) {
          if (VERBOSE_DEBUG){
            System.out.print(vv.getAccessor().getObject(j) + ", " + (j % 25 == 0 ? "\n batch:" + batchCounter + " v:" + j + " - " : ""));
          }
          assertField(vv, j, (TypeProtos.MinorType) currentField.type,
              currentField.values[columnValCounter % 3], (String) currentField.name + "/");
          columnValCounter++;
        }
        if (VERBOSE_DEBUG){
          System.out.println("\n" + ((BaseDataValueVector)vv).getAccessor().getValueCount());
        }
        valuesChecked.remove(vv.getField().getName());
        valuesChecked.put(vv.getField().getName(), columnValCounter);
      }

      if (VERBOSE_DEBUG){
        for (i = 0; i < batchLoader.getRecordCount(); i++) {
          recordCount++;
          if (i % 50 == 0){
            System.out.println();
            for (VectorWrapper vw : batchLoader) {
              ValueVector v = vw.getValueVector();
              System.out.print(pad(v.getField().getName(), 20) + " ");

            }
            System.out.println();
            System.out.println();
          }

          for (VectorWrapper vw : batchLoader) {
            ValueVector v = vw.getValueVector();
            System.out.print(pad(v.getAccessor().getObject(i).toString(), 20) + " ");
          }
          System.out.println(

          );
        }
      }
      batchCounter++;
      if(result.getHeader().getIsLastChunk()){
        future.set(null);
      }
    }

    public void getResults() throws RpcException{
      try{
        future.get();
      }catch(Throwable t){
        throw RpcException.mapException(t);
      }
    }

    @Override
    public void queryIdArrived(QueryId queryId) {
    }
  }



  // specific tests should call this method, but it is not marked as a test itself intentionally
  public void testParquetFullEngineEventBased(boolean generateNew, String plan, String filename, int numberOfTimesRead /* specified in json plan */, ParquetTestProperties props) throws Exception{
    testParquetFullEngineEventBased(generateNew, plan, null, filename, numberOfTimesRead, props, true);
  }

  // specific tests should call this method, but it is not marked as a test itself intentionally
  public void testParquetFullEngineEventBased(boolean generateNew, String plan, String readEntries, String filename,
                                              int numberOfTimesRead /* specified in json plan */, ParquetTestProperties props,
                                              boolean runAsLogicalPlan) throws Exception{
    RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();

    if (generateNew) generateParquetFile(filename, props);

    DrillConfig config = DrillConfig.create();

    try(Drillbit bit1 = new Drillbit(config, serviceSet); DrillClient client = new DrillClient(config, serviceSet.getCoordinator());){
      bit1.run();
      client.connect();
      RecordBatchLoader batchLoader = new RecordBatchLoader(bit1.getContext().getAllocator());
      ParquetResultListener resultListener = new ParquetResultListener(batchLoader, props);
      long C = System.nanoTime();
      String planText = Files.toString(FileUtils.getResourceAsFile(plan), Charsets.UTF_8);
      // substitute in the string for the read entries, allows reuse of the plan file for several tests
      if (readEntries != null) {
        planText = planText.replaceFirst( "&REPLACED_IN_PARQUET_TEST&", readEntries);
      }
      if (runAsLogicalPlan)
        client.runQuery(UserProtos.QueryType.LOGICAL, planText, resultListener);
      else
        client.runQuery(UserProtos.QueryType.PHYSICAL, planText, resultListener);
      resultListener.getResults();
      for (String s : resultListener.valuesChecked.keySet()) {
        assertEquals("Record count incorrect for column: " + s,
            props.recordsPerRowGroup * props.numberRowGroups * numberOfTimesRead, (long) resultListener.valuesChecked.get(s));
        logger.debug("Column {}, Values read:{}", s, resultListener.valuesChecked.get(s));
      }
      long D = System.nanoTime();
      System.out.println(String.format("Took %f s to run query", (float)(D-C) / 1E9));
    }
  }

  // specific tests should call this method, but it is not marked as a test itself intentionally
  public void testParquetFullEngine(boolean generateNew, String plan, String filename, int numberOfTimesRead /* specified in json plan */, ParquetTestProperties props) throws Exception{
    testParquetFullEngine(generateNew, plan, null, filename, numberOfTimesRead, props);
  }

  // specific tests should call this method, but it is not marked as a test itself intentionally
  public void testParquetFullEngine(boolean generateNew, String plan, String readEntries, String filename,
                                    int numberOfTimesRead /* specified in json plan */, ParquetTestProperties props) throws Exception{
    RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();

    if (generateNew) generateParquetFile(filename, props);

    DrillConfig config = DrillConfig.create();

    try(Drillbit bit1 = new Drillbit(config, serviceSet); DrillClient client = new DrillClient(config, serviceSet.getCoordinator())) {
      long A = System.nanoTime();
      bit1.run();
      long B = System.nanoTime();
      client.connect();
      long C = System.nanoTime();
      List<QueryResultBatch> results;
      // insert a variable number of reads
      if (readEntries != null){
        results = client.runQuery(UserProtos.QueryType.LOGICAL, (Files.toString(FileUtils.getResourceAsFile(plan), Charsets.UTF_8).replaceFirst( "&REPLACED_IN_PARQUET_TEST&", readEntries)));
      }
      else{
        results = client.runQuery(UserProtos.QueryType.LOGICAL, Files.toString(FileUtils.getResourceAsFile(plan), Charsets.UTF_8));
      }
//      List<QueryResultBatch> results = client.runQuery(UserProtos.QueryType.PHYSICAL, Files.toString(FileUtils.getResourceAsFile("/parquet_scan_union_screen_physical.json"), Charsets.UTF_8));
      long D = System.nanoTime();
      System.out.println(String.format("Took %f s to start drillbit", (float)(B-A) / 1E9));
      System.out.println(String.format("Took %f s to connect", (float)(C-B) / 1E9));
      System.out.println(String.format("Took %f s to run query", (float)(D-C) / 1E9));
      //List<QueryResultBatch> results = client.runQuery(UserProtos.QueryType.PHYSICAL, Files.toString(FileUtils.getResourceAsFile("/parquet_scan_union_screen_physical.json"), Charsets.UTF_8));
      int count = 0;
//      RecordBatchLoader batchLoader = new RecordBatchLoader(new BootStrapContext(config).getAllocator());
      RecordBatchLoader batchLoader = new RecordBatchLoader(bit1.getContext().getAllocator());
      byte[] bytes;

      int batchCounter = 1;
      int columnValCounter = 0;
      int i = 0;
      FieldInfo currentField;
      HashMap<String, Integer> valuesChecked = new HashMap();
      for(QueryResultBatch b : results){

        count += b.getHeader().getRowCount();
        boolean schemaChanged = batchLoader.load(b.getHeader().getDef(), b.getData());

        int recordCount = 0;
        // print headers.
        if (schemaChanged) {
        } // do not believe any change is needed for when the schema changes, with the current mock scan use case

        for (VectorWrapper vw : batchLoader) {
          ValueVector vv = vw.getValueVector();
          currentField = props.fields.get(vv.getField().getName());
          if (VERBOSE_DEBUG){
            System.out.println("\n" + (String) currentField.name);
          }
          if ( ! valuesChecked.containsKey(vv.getField().getName())){
            valuesChecked.put(vv.getField().getName(), 0);
            columnValCounter = 0;
          } else {
            columnValCounter = valuesChecked.get(vv.getField().getName());
          }
          for (int j = 0; j < vv.getAccessor().getValueCount(); j++) {
            if (VERBOSE_DEBUG){
              System.out.print(vv.getAccessor().getObject(j) + ", " + (j % 25 == 0 ? "\n batch:" + batchCounter + " v:" + j + " - " : ""));
            }
            assertField(vv, j, currentField.type,
                currentField.values[columnValCounter % 3], currentField.name + "/");
            columnValCounter++;
          }
          if (VERBOSE_DEBUG){
            System.out.println("\n" + vv.getAccessor().getValueCount());
          }
          valuesChecked.remove(vv.getField().getName());
          valuesChecked.put(vv.getField().getName(), columnValCounter);
        }

        if (VERBOSE_DEBUG){
          for (i = 0; i < batchLoader.getRecordCount(); i++) {
            recordCount++;
            if (i % 50 == 0){
              System.out.println();
              for (VectorWrapper vw : batchLoader) {
                ValueVector v = vw.getValueVector();
                System.out.print(pad(v.getField().getName(), 20) + " ");

              }
              System.out.println();
              System.out.println();
            }

            for (VectorWrapper vw : batchLoader) {
              ValueVector v = vw.getValueVector();
              System.out.print(pad(v.getAccessor().getObject(i) + "", 20) + " ");
            }
            System.out.println(

            );
          }
        }
        batchCounter++;
      }
      for (String s : valuesChecked.keySet()) {
        assertEquals("Record count incorrect for column: " + s, props.recordsPerRowGroup * props.numberRowGroups * numberOfTimesRead, (long) valuesChecked.get(s));
      }
      assert valuesChecked.keySet().size() > 0;
    }
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

  class MockOutputMutator implements OutputMutator {
    List<MaterializedField> removedFields = Lists.newArrayList();
    List<ValueVector> addFields = Lists.newArrayList();

    @Override
    public void removeField(MaterializedField field) throws SchemaChangeException {
      removedFields.add(field);
    }

    @Override
    public void addField(ValueVector vector) throws SchemaChangeException {
      addFields.add(vector);
    }

    @Override
    public void removeAllFields() {
      addFields.clear();
    }

    @Override
    public void setNewSchema() throws SchemaChangeException {
    }

    List<MaterializedField> getRemovedFields() {
      return removedFields;
    }

    List<ValueVector> getAddFields() {
      return addFields;
    }
  }

  private <T> void assertField(ValueVector valueVector, int index, TypeProtos.MinorType expectedMinorType, Object value, String name) {
    assertField(valueVector, index, expectedMinorType, value, name, 0);
  }

  private <T> void assertField(ValueVector valueVector, int index, TypeProtos.MinorType expectedMinorType, T value, String name, int parentFieldId) {
//    UserBitShared.FieldMetadata metadata = valueVector.getMetadata();
//    SchemaDefProtos.FieldDef def = metadata.getDef();
//    assertEquals(expectedMinorType, def.getMajorType().getMinorType());
//    assertEquals(name, def.getNameList().get(0).getName());
//    assertEquals(parentFieldId, def.getParentId());

    if (expectedMinorType == TypeProtos.MinorType.MAP) {
      return;
    }

    T val = (T) valueVector.getAccessor().getObject(index);
    if (val instanceof String){
      assertEquals(value, val);
    }
    else if (val instanceof byte[]) {
      assertTrue(Arrays.equals((byte[]) value, (byte[]) val));
    } else {
      assertEquals(value, val);
    }
  }

  private class WrapAroundCounter {

    int maxVal;
    int val;

    public WrapAroundCounter(int maxVal) {
      this.maxVal = maxVal;
    }

    public int increment() {
      val++;
      if (val > maxVal) {
        val = 0;
      }
      return val;
    }

    public void reset() {
      val = 0;
    }

  }
}
