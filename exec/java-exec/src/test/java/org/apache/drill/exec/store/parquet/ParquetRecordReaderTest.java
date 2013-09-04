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
package org.apache.drill.exec.store.parquet;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.apache.drill.exec.store.parquet.TestFileGenerator.FieldInfo;
import org.apache.drill.exec.vector.BaseDataValueVector;
import org.apache.drill.exec.vector.ValueVector;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import parquet.bytes.BytesInput;
import parquet.column.page.Page;
import parquet.column.page.PageReadStore;
import parquet.column.page.PageReader;
import parquet.hadoop.Footer;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.schema.MessageType;

import com.google.common.base.Charsets;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.util.concurrent.SettableFuture;

public class ParquetRecordReaderTest {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ParquetRecordReaderTest.class);

  private boolean VERBOSE_DEBUG = false;
  private boolean checkValues = true;

  static final int numberRowGroups = 1;
  static final int recordsPerRowGroup = 300;
  static final String fileName = "/tmp/parquet_test_file_many_types";

  @BeforeClass
  public static void generateFile() throws Exception{
    File f = new File(fileName);
    if(!f.exists()) TestFileGenerator.generateParquetFile(fileName, numberRowGroups, recordsPerRowGroup);
  }
 
  @Test
  public void testMultipleRowGroupsAndReads() throws Exception {
    String planName = "/parquet/parquet_scan_screen.json";
    testParquetFullEngineLocalPath(planName, fileName, 2, numberRowGroups, recordsPerRowGroup);
  }
  
  @Test
  public void testMultipleRowGroupsAndReads2() throws Exception {
    String readEntries;
    readEntries = "";
    // number of times to read the file
    int i = 3;
    for (int j = 0; j < i; j++){
      readEntries += "{path: \""+fileName+"\"}";
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


  private class ParquetResultListener implements UserResultsListener {
    private SettableFuture<Void> future = SettableFuture.create();
    RecordBatchLoader batchLoader;

    int batchCounter = 1;
    private final HashMap<String, Long> valuesChecked = new HashMap<>();
    private final Map<String, FieldInfo> fields;
    private final long totalRecords;
    
    ParquetResultListener(int recordsPerRowGroup, RecordBatchLoader batchLoader, int numberRowGroups, int numberOfTimesRead){
      this.batchLoader = batchLoader;
      this.fields = TestFileGenerator.getFieldMap(recordsPerRowGroup);
      this.totalRecords = recordsPerRowGroup * numberRowGroups * numberOfTimesRead;
    }

    @Override
    public void submissionFailed(RpcException ex) {
      logger.debug("Submission failed.", ex);
      future.setException(ex);
    }

    @Override
    public void resultArrived(QueryResultBatch result) {
      long columnValCounter = 0;
      int i = 0;
      FieldInfo currentField;

      boolean schemaChanged = false;
      try {
        schemaChanged = batchLoader.load(result.getHeader().getDef(), result.getData());
      } catch (SchemaChangeException e) {
        logger.error("Failure while loading batch", e);
      }

      // print headers.
      if (schemaChanged) {
      } // do not believe any change is needed for when the schema changes, with the current mock scan use case

      for (VectorWrapper<?> vw : batchLoader) {
        ValueVector vv = vw.getValueVector();
        currentField = fields.get(vv.getField().getName());
        if (VERBOSE_DEBUG){
          System.out.println("\n" + (String) currentField.name);
        }
        if ( ! valuesChecked.containsKey(vv.getField().getName())){
          valuesChecked.put(vv.getField().getName(), (long) 0);
          columnValCounter = 0;
        } else {
          columnValCounter = valuesChecked.get(vv.getField().getName());
        }
        for (int j = 0; j < ((BaseDataValueVector)vv).getAccessor().getValueCount(); j++) {
          if (VERBOSE_DEBUG){
            System.out.print(vv.getAccessor().getObject(j) + ", " + (j % 25 == 0 ? "\n batch:" + batchCounter + " v:" + j + " - " : ""));
          }
          if (checkValues) {
            try {
              assertField(vv, j, (TypeProtos.MinorType) currentField.type,
                currentField.values[(int) (columnValCounter % 3)], (String) currentField.name + "/");
            } catch (AssertionError e) { submissionFailed(new RpcException(e)); }
          }
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
          if (i % 50 == 0){
            System.out.println();
            for (VectorWrapper<?> vw : batchLoader) {
              ValueVector v = vw.getValueVector();
              System.out.print(pad(v.getField().getName(), 20) + " ");
            }
            System.out.println();
            System.out.println();
          }

          for (VectorWrapper<?> vw : batchLoader) {
            ValueVector v = vw.getValueVector();
            System.out.print(pad(v.getAccessor().getObject(i).toString(), 20) + " ");
          }
          System.out.println(

          );
        }
      }

      for(VectorWrapper<?> vw : batchLoader){
        vw.release();
      }
      result.release();
      
      batchCounter++;
      if(result.getHeader().getIsLastChunk()){
        for (String s : valuesChecked.keySet()) {
          try {
          assertEquals("Record count incorrect for column: " + s, totalRecords, (long) valuesChecked.get(s));
          } catch (AssertionError e) { submissionFailed(new RpcException(e)); }
        }
        
        assert valuesChecked.keySet().size() > 0;
        future.set(null);
      }
    }

    public void get() throws RpcException{
      try{
        future.get();
        return;
      }catch(Throwable t){
        throw RpcException.mapException(t);
      }
    }
  }

  
  
  
  public void testParquetFullEngineRemote(String plan, String filename, int numberOfTimesRead /* specified in json plan */, int numberOfRowGroups, int recordsPerRowGroup) throws Exception{
    
    DrillConfig config = DrillConfig.create();

    checkValues = false;

    try(DrillClient client = new DrillClient(config);){
      client.connect();
      RecordBatchLoader batchLoader = new RecordBatchLoader(client.getAllocator());
      ParquetResultListener resultListener = new ParquetResultListener(recordsPerRowGroup, batchLoader, numberOfRowGroups, numberOfTimesRead);
      client.runQuery(UserProtos.QueryType.PHYSICAL, Files.toString(FileUtils.getResourceAsFile(plan), Charsets.UTF_8), resultListener);
      resultListener.get();
    }
    
  }
  
  
  public void testParquetFullEngineLocalPath(String planFileName, String filename, int numberOfTimesRead /* specified in json plan */, int numberOfRowGroups, int recordsPerRowGroup) throws Exception{
    testParquetFullEngineLocalText(Files.toString(FileUtils.getResourceAsFile(planFileName), Charsets.UTF_8), filename, numberOfTimesRead, numberOfRowGroups, recordsPerRowGroup);
  }
  
  //specific tests should call this method, but it is not marked as a test itself intentionally
  public void testParquetFullEngineLocalText(String planText, String filename, int numberOfTimesRead /* specified in json plan */, int numberOfRowGroups, int recordsPerRowGroup) throws Exception{
    
    RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();

    DrillConfig config = DrillConfig.create();

    try(Drillbit bit1 = new Drillbit(config, serviceSet); DrillClient client = new DrillClient(config, serviceSet.getCoordinator());){
      bit1.run();
      client.connect();
      RecordBatchLoader batchLoader = new RecordBatchLoader(client.getAllocator());
      ParquetResultListener resultListener = new ParquetResultListener(recordsPerRowGroup, batchLoader, numberOfRowGroups, numberOfTimesRead);
      Stopwatch watch = new Stopwatch().start();
      client.runQuery(UserProtos.QueryType.LOGICAL, planText, resultListener);
      resultListener.get();
      System.out.println(String.format("Took %d ms to run query", watch.elapsed(TimeUnit.MILLISECONDS)));

    }
    
  }


  //use this method to submit physical plan
  public void testParquetFullEngineLocalTextDistributed(String planName, String filename, int numberOfTimesRead /* specified in json plan */, int numberOfRowGroups, int recordsPerRowGroup) throws Exception{

    RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();

    checkValues = false;

    DrillConfig config = DrillConfig.create();

    try(Drillbit bit1 = new Drillbit(config, serviceSet); DrillClient client = new DrillClient(config, serviceSet.getCoordinator());){
      bit1.run();
      client.connect();
      RecordBatchLoader batchLoader = new RecordBatchLoader(client.getAllocator());
      ParquetResultListener resultListener = new ParquetResultListener(recordsPerRowGroup, batchLoader, numberOfRowGroups, numberOfTimesRead);
      Stopwatch watch = new Stopwatch().start();
      client.runQuery(UserProtos.QueryType.PHYSICAL, Files.toString(FileUtils.getResourceAsFile(planName), Charsets.UTF_8), resultListener);
      resultListener.get();
      System.out.println(String.format("Took %d ms to run query", watch.elapsed(TimeUnit.MILLISECONDS)));

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

  @SuppressWarnings("unchecked")
  private <T> void assertField(ValueVector valueVector, int index, TypeProtos.MinorType expectedMinorType, T value, String name, int parentFieldId) {

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
