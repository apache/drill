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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.*;
import static parquet.column.Encoding.PLAIN;


public class MockScantTest {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StorageEngineRegistry.class);

  private boolean VERBOSE_DEBUG = false;

  private class ParquetResultListener implements UserResultsListener {

    CountDownLatch latch = new CountDownLatch(1);
    @Override
    public void submissionFailed(RpcException ex) {
      latch.countDown();
    }

    @Override
    public void resultArrived(QueryResultBatch result) {
      if(result.getHeader().getIsLastChunk()) latch.countDown();
      result.getData().release(1);
    }

    public void await() throws Exception {
      latch.await();
    }
  }


  @Test
  public void testMockScanFullEngine() throws Exception{
    RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();

    DrillConfig config = DrillConfig.create();

//    try(Drillbit bit1 = new Drillbit(config, serviceSet); DrillClient client = new DrillClient(config, serviceSet.getCoordinator());){
    try(DrillClient client = new DrillClient(config)){
      long A = System.nanoTime();
//      bit1.run();
      long B = System.nanoTime();
      client.connect();
      long C = System.nanoTime();
      ParquetResultListener listener = new ParquetResultListener();
      client.runQuery(UserProtos.QueryType.PHYSICAL, Files.toString(FileUtils.getResourceAsFile("/mock-scan.json"), Charsets.UTF_8), listener);
      listener.await();
      long D = System.nanoTime();
      System.out.println(String.format("Took %f s to run query", (float)(D-C) / 1E9));
    }
  }
}
