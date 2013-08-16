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

package org.apache.drill.exec.store.json;

import java.util.Iterator;
import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.logical.StorageEngineConfig;
import org.apache.drill.exec.physical.OperatorCost;
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
import com.google.common.collect.Iterators;

@JsonTypeName("json-sub-scan")
public class JSONSubScan extends AbstractBase implements SubScan {

  protected final List<JSONGroupScan.ScanEntry> readEntries;
  private final OperatorCost cost;
  private final Size size;
  private final JSONStorageEngine storageEngine;
  private final FieldReference ref;

  @JsonCreator
  public JSONSubScan(@JacksonInject StorageEngineRegistry registry,
                     @JsonProperty("engineConfig") StorageEngineConfig engineConfig,
                     @JsonProperty("readEntries") List<JSONGroupScan.ScanEntry> readEntries, @JsonProperty("ref") FieldReference ref) throws ExecutionSetupException {
    this(readEntries, (JSONStorageEngine) registry.getEngine(engineConfig), ref);
  }
  
  JSONSubScan(List<JSONGroupScan.ScanEntry> readEntries, JSONStorageEngine engine, FieldReference ref){
    this.readEntries = readEntries;
    this.storageEngine = engine;
    OperatorCost cost = new OperatorCost(0, 0, 0, 0);
    Size size = new Size(0, 0);
    for (JSONGroupScan.ScanEntry r : readEntries) {
      cost = cost.add(r.getCost());
      size = size.add(r.getSize());
    }
    this.cost = cost;
    this.size = size;
    this.ref = ref;
  }
  
  public FieldReference getRef() {
    return ref;
  }

  public List<JSONGroupScan.ScanEntry> getReadEntries() {
    return readEntries;
  }

  public StorageEngineConfig getEngineConfig() {
    return storageEngine.getConfig();
  }

  @JsonIgnore
  public JSONStorageEngine getStorageEngine() {
    return storageEngine;
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException{
    throw new UnsupportedOperationException();
  }

  @Override
  public OperatorCost getCost() {
    return cost;
  }

  @Override
  public Size getSize() {
    return size;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitSubScan(this, value);
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return Iterators.emptyIterator();
  }
}
