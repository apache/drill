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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.logical.data.Scan;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.ReadEntry;
import org.apache.drill.exec.physical.ReadEntryWithPath;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStorageEngine;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.mock.MockStorageEngine;

import com.google.common.collect.ListMultimap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

import parquet.format.converter.ParquetMetadataConverter;
import parquet.hadoop.CodecFactoryExposer;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.ColumnChunkMetaData;
import parquet.hadoop.metadata.ParquetMetadata;

public class ParquetStorageEngine extends AbstractStorageEngine{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MockStorageEngine.class);

  private final DrillbitContext context;
  private final ParquetStorageEngineConfig configuration;
  private FileSystem fs;
  private Configuration conf;
  static final ParquetMetadataConverter parquetMetadataConverter = new ParquetMetadataConverter();
  private CodecFactoryExposer codecFactoryExposer;
  final ParquetMetadata footer;
  public static final String HADOOP_DEFAULT_NAME = "fs.default.name";


  public ParquetStorageEngine(ParquetStorageEngineConfig configuration, DrillbitContext context){
    this.context = context;
    this.configuration = configuration;
    this.footer = null;
    try {
      this.conf = new Configuration();
      this.conf.set(HADOOP_DEFAULT_NAME, configuration.getDfsName());
      this.fs = FileSystem.get(conf);
      codecFactoryExposer = new CodecFactoryExposer(conf);
    } catch (IOException ie) {
      throw new RuntimeException("Error setting up filesystem");
    }
  }

  public Configuration getHadoopConfig() {
    return this.conf;
  }

  public FileSystem getFileSystem() {
    return this.fs;
  }

  public DrillbitContext getContext() {
    return this.context;
  }

  @Override
  public boolean supportsRead() {
    return true;
  }

  @Override
  public ParquetGroupScan getPhysicalScan(Scan scan) throws IOException {

    ArrayList<ReadEntryWithPath> readEntries = scan.getSelection().getListWith(new ObjectMapper(),
        new TypeReference<ArrayList<ReadEntryWithPath>>() {});

    return new ParquetGroupScan(readEntries, this);
  }

  @Override
  public ListMultimap<ReadEntry, DrillbitEndpoint> getReadLocations(Collection<ReadEntry> entries) {
    return null;
  }

  @Override
  public RecordReader getReader(FragmentContext context, ReadEntry readEntry) throws IOException {
    return null;
  }


  public CodecFactoryExposer getCodecFactoryExposer() {
    return codecFactoryExposer;
  }
}
