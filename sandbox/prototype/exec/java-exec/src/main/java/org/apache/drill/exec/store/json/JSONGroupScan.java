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

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.OperatorCost;
import org.apache.drill.exec.physical.ReadEntry;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.Size;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.store.StorageEngineRegistry;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("json-scan")
public class JSONGroupScan extends AbstractGroupScan {
  private static int ESTIMATED_RECORD_SIZE = 1024; // 1kb
  private final JSONStorageEngine engine;

  private LinkedList<JSONGroupScan.ScanEntry>[] mappings;
  private final List<JSONGroupScan.ScanEntry> readEntries;
  private final OperatorCost cost;
  private final Size size;
  private final FieldReference ref;

  @JsonCreator
  public JSONGroupScan(@JsonProperty("entries") List<ScanEntry> entries,
                       @JsonProperty("storageengine") JSONStorageEngineConfig storageEngineConfig,
                       @JacksonInject StorageEngineRegistry engineRegistry, FieldReference ref) throws ExecutionSetupException {
    this(entries, (JSONStorageEngine) engineRegistry.getEngine(storageEngineConfig), ref);
  }

  public JSONGroupScan(List<ScanEntry> entries, JSONStorageEngine engine, FieldReference ref) {
    this.engine = engine;
    this.readEntries = entries;
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
  
  @SuppressWarnings("unchecked")
  @Override
  public void applyAssignments(List<CoordinationProtos.DrillbitEndpoint> endpoints) {
    checkArgument(endpoints.size() <= readEntries.size());

    mappings = new LinkedList[endpoints.size()];

    int i = 0;
    for (ScanEntry e : readEntries) {
      if (i == endpoints.size()) i = 0;
      LinkedList entries = mappings[i];
      if (entries == null) {
        entries = new LinkedList<>();
        mappings[i] = entries;
      }
      entries.add(e);
      i++;
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public SubScan getSpecificScan(int minorFragmentId) throws ExecutionSetupException{
    checkArgument(minorFragmentId < mappings.length, "Mappings length [%s] should be longer than minor fragment id [%s] but it isn't.", mappings.length, minorFragmentId);
    return new JSONSubScan(mappings[minorFragmentId], engine, ref);
  }

  @Override
  public List<EndpointAffinity> getOperatorAffinity() {
    return Collections.emptyList();
  }

  @Override
  @JsonIgnore
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    return new JSONGroupScan(readEntries, engine, ref);
  }

  public static class ScanEntry implements ReadEntry {
    private final String path;
    private Size size;

    @JsonCreator
    public ScanEntry(@JsonProperty("path") String path) {
      this.path = path;
      size = new Size(ESTIMATED_RECORD_SIZE, ESTIMATED_RECORD_SIZE);
    }

    @Override
    public OperatorCost getCost() {
      return new OperatorCost(1, 1, 2, 2);
    }

    @Override
    public Size getSize() {
      return size;
    }

    public String getPath() {
      return path;
    }
  }

  @Override
  public int getMaxParallelizationWidth() {
    return readEntries.size();
  }

  @Override
  public OperatorCost getCost() {
    return cost;
  }

  @Override
  public Size getSize() {
    return size;
  }
}
