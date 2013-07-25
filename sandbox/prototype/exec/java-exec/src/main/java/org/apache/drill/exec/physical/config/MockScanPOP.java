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
package org.apache.drill.exec.physical.config;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.OperatorCost;
import org.apache.drill.exec.physical.ReadEntry;
import org.apache.drill.exec.physical.base.AbstractScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.Scan;
import org.apache.drill.exec.physical.base.Size;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.vector.TypeHelper;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;

@JsonTypeName("mock-scan")
public class MockScanPOP extends AbstractScan<MockScanPOP.MockScanEntry> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MockScanPOP.class);

  private final String url;
  private  LinkedList<MockScanEntry>[] mappings;

  @JsonCreator
  public MockScanPOP(@JsonProperty("url") String url, @JsonProperty("entries") List<MockScanEntry> readEntries) {
    super(readEntries);
    this.url = url;
  }

  public String getUrl() {
    return url;
  }

  
  public static class MockScanEntry implements ReadEntry {

    private final int records;
    private final MockColumn[] types;
    private final int recordSize;
    

    @JsonCreator
    public MockScanEntry(@JsonProperty("records") int records, @JsonProperty("types") MockColumn[] types) {
      this.records = records;
      this.types = types;
      int size = 0;
      for(MockColumn dt : types){
        size += TypeHelper.getSize(dt.getMajorType());
      }
      this.recordSize = size;
    }

    @Override
    public OperatorCost getCost() {
      return new OperatorCost(1, 2, 1, 1);
    }

    
    public int getRecords() {
      return records;
    }

    public MockColumn[] getTypes() {
      return types;
    }

    @Override
    public Size getSize() {
      return new Size(records, recordSize);
    }
  }
  
  @JsonInclude(Include.NON_NULL)
  public static class MockColumn{
    @JsonProperty("type") public MinorType minorType;
    public String name;
    public DataMode mode;
    public Integer width;
    public Integer precision;
    public Integer scale;
    
    
    @JsonCreator
    public MockColumn(@JsonProperty("name") String name, @JsonProperty("type") MinorType minorType, @JsonProperty("mode") DataMode mode, @JsonProperty("width") Integer width, @JsonProperty("precision") Integer precision, @JsonProperty("scale") Integer scale) {
      this.name = name;
      this.minorType = minorType;
      this.mode = mode;
      this.width = width;
      this.precision = precision;
      this.scale = scale;
    }
    
    @JsonProperty("type")
    public MinorType getMinorType() {
      return minorType;
    }
    public String getName() {
      return name;
    }
    public DataMode getMode() {
      return mode;
    }
    public Integer getWidth() {
      return width;
    }
    public Integer getPrecision() {
      return precision;
    }
    public Integer getScale() {
      return scale;
    }
    
    @JsonIgnore
    public MajorType getMajorType(){
      MajorType.Builder b = MajorType.newBuilder();
      b.setMode(mode);
      b.setMinorType(minorType);
      if(precision != null) b.setPrecision(precision);
      if(width != null) b.setWidth(width);
      if(scale != null) b.setScale(scale);
      return b.build();
    }
    
  }

  @Override
  public List<EndpointAffinity> getOperatorAffinity() {
    return Collections.emptyList();
  }

  @SuppressWarnings("unchecked")
  @Override
  public void applyAssignments(List<DrillbitEndpoint> endpoints) {
    Preconditions.checkArgument(endpoints.size() <= getReadEntries().size());
    
    mappings = new LinkedList[endpoints.size()];

    int i =0;
    for(MockScanEntry e : this.getReadEntries()){
      if(i == endpoints.size()) i -= endpoints.size();
      LinkedList<MockScanEntry> entries = mappings[i];
      if(entries == null){
        entries = new LinkedList<MockScanEntry>();
        mappings[i] = entries;
      }
      entries.add(e);
      i++;
    }
  }

  @Override
  public Scan<?> getSpecificScan(int minorFragmentId) {
    assert minorFragmentId < mappings.length : String.format("Mappings length [%d] should be longer than minor fragment id [%d] but it isn't.", mappings.length, minorFragmentId);
    return new MockScanPOP(url, mappings[minorFragmentId]);
  }

  @Override
  @JsonIgnore
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    return new MockScanPOP(url, readEntries);

  }

}
