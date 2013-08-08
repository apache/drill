/*******************************************************************************
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
package org.apache.drill.exec.store;

import com.beust.jcommander.internal.Lists;
import com.google.common.base.Charsets;
import com.google.common.io.Files;

import com.google.common.util.concurrent.SettableFuture;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.physical.impl.OutputMutator;

import org.apache.drill.exec.proto.UserProtos;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.rpc.user.UserResultsListener;
import org.apache.drill.exec.server.BootStrapContext;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.RemoteServiceSet;

import org.apache.drill.exec.store.parquet.ParquetStorageEngine;
import org.apache.drill.exec.vector.BaseDataValueVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import parquet.bytes.BytesInput;
import parquet.column.ColumnDescriptor;
import parquet.column.page.Page;
import parquet.column.page.PageReadStore;
import parquet.column.page.PageReader;
import parquet.hadoop.Footer;

import parquet.hadoop.ParquetFileWriter;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import static parquet.column.Encoding.PLAIN;

public class ParquetRecordReaderTest {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StorageEngineRegistry.class);

  private boolean VERBOSE_DEBUG = false;

  @Test
  public void testMultipleRowGroupsAndReads() throws Exception {
    testParquetFullEngine(true, "/parquet_scan_screen.json", "/tmp/testParquetFile_many_types_3", 2, numberRowGroups, recordsPerRowGroup);
  }

  @Test
  public void testMultipleRowGroupsAndReadsEvent() throws Exception {
    testParquetFullEngineEventBased(true, "/parquet_scan_screen.json", "/tmp/testParquetFile_many_types_3", 2, numberRowGroups, recordsPerRowGroup);
  }

  int numberRowGroups = 20;
  static int recordsPerRowGroup = 3000000;

  // 10 mb per page
  static int bytesPerPage = 1024 * 1024 * 10;
  // { 00000001, 00000010, 00000100, 00001000, 00010000, ... }
  byte[] bitFields = {1, 2, 4, 8, 16, 32, 64, -128};
  static final byte allBitsTrue = -1;
  static final byte allBitsFalse = 0;
  static final byte[] varLen1 = {50, 51, 52, 53, 54, 55, 56};
  static final byte[] varLen2 = {15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1};
  static final byte[] varLen3 = {100, 99, 98};

  private static class FieldInfo {

    String parquetType;
    String name;
    int bitLength;
    int numberOfPages;
    Object[] values;
    TypeProtos.MinorType type;

    FieldInfo(String parquetType, String name, int bitLength, Object[] values, TypeProtos.MinorType type){
      this.parquetType = parquetType;
      this.name = name;
      this.bitLength  = bitLength;
      this.numberOfPages = Math.max(1, (int) Math.ceil(recordsPerRowGroup * bitLength / 8.0 / bytesPerPage));
      this.values = values;
      // generator is designed to use 3 values
      assert values.length == 3;
      this.type = type;
    }
  }

  
  private static HashMap<String, FieldInfo> fields = new HashMap<>();
  static {
    Object[] intVals = {-200, 100, Integer.MAX_VALUE };
    Object[] longVals = { -5000l, 5000l, Long.MAX_VALUE};
    Object[] floatVals = { 1.74f, Float.MAX_VALUE, Float.MIN_VALUE};
    Object[] doubleVals = {100.45d, Double.MAX_VALUE, Double.MIN_VALUE,};
    Object[] boolVals = {false, false, true};
    Object[] binVals = { varLen1, varLen2, varLen3};
    Object[] bin2Vals = { varLen3, varLen2, varLen1};
    fields.put("integer/", new FieldInfo("int32", "integer", 32, intVals, TypeProtos.MinorType.INT));
    fields.put("bigInt/", new FieldInfo("int64", "bigInt", 64, longVals, TypeProtos.MinorType.BIGINT));
    fields.put("f/", new FieldInfo("float", "f", 32, floatVals, TypeProtos.MinorType.FLOAT4));
    fields.put("d/", new FieldInfo("double", "d", 64, doubleVals, TypeProtos.MinorType.FLOAT8));
//    fields.put("b/", new FieldInfo("binary", "b", 1, boolVals, TypeProtos.MinorType.BIT));
    fields.put("bin/", new FieldInfo("binary", "bin", -1, binVals, TypeProtos.MinorType.VARBINARY));
    fields.put("bin2/", new FieldInfo("binary", "bin2", -1, bin2Vals, TypeProtos.MinorType.VARBINARY));
  }


  private String getResource(String resourceName) {
    return "resource:" + resourceName;
  }

  public void generateParquetFile(String filename, int numberRowGroups, int recordsPerRowGroup) throws Exception {

    int currentBooleanByte = 0;
    WrapAroundCounter booleanBitCounter = new WrapAroundCounter(7);

    Configuration configuration = new Configuration();
    configuration.set(ParquetStorageEngine.HADOOP_DEFAULT_NAME, "file:///");
    //"message m { required int32 integer; required int64 integer64; required boolean b; required float f; required double d;}"

    FileSystem fs = FileSystem.get(configuration);
    Path path = new Path(filename);
    if (fs.exists(path)) fs.delete(path, false);


    String messageSchema = "message m {";
    for (FieldInfo fieldInfo : fields.values()) {
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
    for (int k = 0; k < numberRowGroups; k++){
      w.startBlock(1);

      for (FieldInfo fieldInfo : fields.values()) {

        if ( ! columnValuesWritten.containsKey(fieldInfo.name)){
          columnValuesWritten.put((String) fieldInfo.name, 0);
          valsWritten = 0;
        } else {
          valsWritten = columnValuesWritten.get(fieldInfo.name);
        }

        String[] path1 = {(String) fieldInfo.name};
        ColumnDescriptor c1 = schema.getColumnDescription(path1);

        w.startColumn(c1, recordsPerRowGroup, codec);
        int valsPerPage = (int) Math.ceil(recordsPerRowGroup / (float) ((int) fieldInfo.numberOfPages));
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
          if ( valsPerPage % 3 > 0 ) leftOverBytes += ((byte[])fieldInfo.values[1]).length + 4;
          if ( valsPerPage % 3 > 1 ) leftOverBytes += ((byte[])fieldInfo.values[2]).length + 4;
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
                System.arraycopy(fieldInfo.values[valsWritten % 3],
                    0, bytes, bytesWritten + bytesNeededToEncodeLength, ((byte[])fieldInfo.values[valsWritten % 3]).length);
                bytesWritten += ((byte[])fieldInfo.values[valsWritten % 3]).length + bytesNeededToEncodeLength;
              }
              else{
                System.arraycopy( ByteArrayUtil.toByta(fieldInfo.values[valsWritten % 3]),
                    0, bytes, i * ((int) fieldInfo.bitLength / 8), (int) fieldInfo.bitLength / 8);
              }
              valsWritten++;
            }

          }
          w.writeDataPage((int)(recordsPerRowGroup / (int) fieldInfo.numberOfPages), bytes.length, BytesInput.from(bytes), PLAIN, PLAIN, PLAIN);
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
    private Vector<QueryResultBatch> results = new Vector<QueryResultBatch>();
    private SettableFuture<List<QueryResultBatch>> future = SettableFuture.create();
    int count = 0;
    RecordBatchLoader batchLoader;
    byte[] bytes;

    int batchCounter = 1;
    int columnValCounter = 0;
    int i = 0;
    FieldInfo currentField;
    HashMap<String, Integer> valuesChecked = new HashMap();

    ParquetResultListener(RecordBatchLoader batchLoader){
      this.batchLoader = batchLoader;
    }

    @Override
    public void submissionFailed(RpcException ex) {
      logger.debug("Submission failed.", ex);
      future.setException(ex);
    }

    @Override
    public void resultArrived(QueryResultBatch result) {
      logger.debug("result arrived in test batch listener.");
      int columnValCounter = 0;
      int i = 0;
      FieldInfo currentField;
      count += result.getHeader().getRowCount();
      boolean schemaChanged = false;
      try {
        schemaChanged = batchLoader.load(result.getHeader().getDef(), result.getData());
      } catch (SchemaChangeException e) {
        e.printStackTrace();
      }

      int recordCount = 0;
      // print headers.
      if (schemaChanged) {
      } // do not believe any change is needed for when the schema changes, with the current mock scan use case

      for (VectorWrapper vw : batchLoader) {
        ValueVector vv = vw.getValueVector();
        currentField = fields.get(vv.getField().getName());
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
        future.set(results);
      }
    }

    public List<QueryResultBatch> getResults() throws RpcException{
      try{
        return future.get();
      }catch(Throwable t){
        throw RpcException.mapException(t);
      }
    }
  }

  // specific tests should call this method, but it is not marked as a test itself intentionally
  public void testParquetFullEngineEventBased(boolean generateNew, String plan, String filename, int numberOfTimesRead /* specified in json plan */, int numberRowGroups, int recordsPerRowGroup) throws Exception{
    RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();

    if (generateNew) generateParquetFile(filename, numberRowGroups, recordsPerRowGroup);

    DrillConfig config = DrillConfig.create();

    try(Drillbit bit1 = new Drillbit(config, serviceSet); DrillClient client = new DrillClient(config, serviceSet.getCoordinator());){
      bit1.run();
      client.connect();
      List<QueryResultBatch> results = client.runQuery(UserProtos.QueryType.LOGICAL, Files.toString(FileUtils.getResourceAsFile(plan), Charsets.UTF_8));
      int count = 0;
      RecordBatchLoader batchLoader = new RecordBatchLoader(bit1.getContext().getAllocator());
      ParquetResultListener resultListener = new ParquetResultListener(batchLoader);
      client.runQuery(UserProtos.QueryType.LOGICAL, Files.toString(FileUtils.getResourceAsFile(plan), Charsets.UTF_8), resultListener);
    }
  }


  // specific tests should call this method, but it is not marked as a test itself intentionally
  public void testParquetFullEngine(boolean generateNew, String plan, String filename, int numberOfTimesRead /* specified in json plan */, int numberRowGroups, int recordsPerRowGroup) throws Exception{
    RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();

    if (generateNew) generateParquetFile(filename, numberRowGroups, recordsPerRowGroup);

    DrillConfig config = DrillConfig.create();

    try(Drillbit bit1 = new Drillbit(config, serviceSet); DrillClient client = new DrillClient(config, serviceSet.getCoordinator())) {
      long A = System.nanoTime();
      bit1.run();
      long B = System.nanoTime();
      client.connect();
      long C = System.nanoTime();
      System.out.println( new SimpleDateFormat("mm:ss S").format(new Date()) + " :Start query");
      List<QueryResultBatch> results = client.runQuery(UserProtos.QueryType.LOGICAL, Files.toString(FileUtils.getResourceAsFile("/parquet_scan_screen.json"), Charsets.UTF_8));
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
          currentField = fields.get(vv.getField().getName());
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
          for (i = 1; i < batchLoader.getRecordCount(); i++) {
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
      }
      for (String s : valuesChecked.keySet()) {
        assertEquals("Record count incorrect for column: " + s, recordsPerRowGroup * numberRowGroups * numberOfTimesRead, (long) valuesChecked.get(s));
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
    if (val instanceof byte[]) {
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

}
