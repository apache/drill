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
package org.apache.drill.common.physical;

import java.util.Collections;
import java.util.List;

import org.apache.drill.common.physical.pop.base.AbstractScan;
import org.apache.drill.common.physical.pop.base.Scan;
import org.apache.drill.common.proto.CoordinationProtos.DrillbitEndpoint;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("mock-scan")
public class MockScanPOP extends AbstractScan<MockScanPOP.MockScanEntry>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MockScanPOP.class);
  
  private final String url;
  
  @JsonCreator
  public MockScanPOP(@JsonProperty("url") String url, @JsonProperty("entries") List<MockScanEntry> readEntries) {
    super(readEntries);
    this.url = url;
  }
  
  public String getUrl() {
    return url;
  }

  public static class MockScanEntry implements ReadEntry{
    public int id;
  }

  @Override
  public List<EndpointAffinity> getOperatorAffinity() {
    return Collections.emptyList();
  }

  @Override
  public void applyAssignments(List<DrillbitEndpoint> endpoints) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Scan<?> getSpecificScan(int minorFragmentId) {
    return this;
  }
  

  
  
}
