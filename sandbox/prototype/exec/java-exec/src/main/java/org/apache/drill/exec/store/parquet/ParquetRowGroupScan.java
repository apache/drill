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

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.logical.StorageEngineConfig;
import org.apache.drill.exec.exception.SetupException;
import org.apache.drill.exec.physical.OperatorCost;
import org.apache.drill.exec.physical.ReadEntryFromHDFS;
import org.apache.drill.exec.physical.base.AbstractBase;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.physical.base.Size;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.store.StorageEngineRegistry;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;

// Class containing information for reading a single parquet row group form HDFS
@JsonTypeName("parquet-row-group-scan")
public class ParquetRowGroupScan extends AbstractBase implements SubScan {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ParquetRowGroupScan.class);

  public final StorageEngineConfig engineConfig;
  private final ParquetStorageEngine parquetStorageEngine;
  private final List<RowGroupReadEntry> rowGroupReadEntries;
  private final FieldReference ref;

  @JsonCreator
  public ParquetRowGroupScan(@JacksonInject StorageEngineRegistry registry, @JsonProperty("engineConfig") StorageEngineConfig engineConfig,
                             @JsonProperty("rowGroupReadEntries") LinkedList<RowGroupReadEntry> rowGroupReadEntries, @JsonProperty("ref") FieldReference ref) throws ExecutionSetupException {
    parquetStorageEngine = (ParquetStorageEngine) registry.getEngine(engineConfig);
    this.rowGroupReadEntries = rowGroupReadEntries;
    this.engineConfig = engineConfig;
    this.ref = ref;
  }

  public ParquetRowGroupScan(ParquetStorageEngine engine, ParquetStorageEngineConfig config,
                              List<RowGroupReadEntry> rowGroupReadEntries, FieldReference ref) {
    parquetStorageEngine = engine;
    engineConfig = config;
    this.rowGroupReadEntries = rowGroupReadEntries;
    this.ref = ref;
  }

  public List<RowGroupReadEntry> getRowGroupReadEntries() {
    return rowGroupReadEntries;
  }

  public StorageEngineConfig getEngineConfig() {
    return engineConfig;
  }

  @Override
  public OperatorCost getCost() {
    return null;
  }

  
  public FieldReference getRef() {
    return ref;
  }

  @Override
  public Size getSize() {
    return null;
  }

  @Override
  public boolean isExecutable() {
    return false;
  }

  @JsonIgnore
  public ParquetStorageEngine getStorageEngine(){
    return parquetStorageEngine;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitSubScan(this, value);
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    return new ParquetRowGroupScan(parquetStorageEngine, (ParquetStorageEngineConfig) engineConfig, rowGroupReadEntries, ref);
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return Iterators.emptyIterator();
  }

  public static class RowGroupReadEntry extends ReadEntryFromHDFS {

    private int rowGroupIndex;

    @parquet.org.codehaus.jackson.annotate.JsonCreator
    public RowGroupReadEntry(@JsonProperty("path") String path, @JsonProperty("start") long start,
                             @JsonProperty("length") long length, @JsonProperty("rowGroupIndex") int rowGroupIndex) {
      super(path, start, length);
      this.rowGroupIndex = rowGroupIndex;
    }

    @Override
    public OperatorCost getCost() {
      return new OperatorCost(1, 2, 1, 1);
    }

    @Override
    public Size getSize() {
      // TODO - these values are wrong, I cannot know these until after I read a file
      return new Size(10, 10);
    }

    @JsonIgnore
    public RowGroupReadEntry getRowGroupReadEntry() {
      return new RowGroupReadEntry(this.getPath(), this.getStart(), this.getLength(), this.rowGroupIndex);
    }

    public int getRowGroupIndex(){
      return rowGroupIndex;
    }
  }

}
