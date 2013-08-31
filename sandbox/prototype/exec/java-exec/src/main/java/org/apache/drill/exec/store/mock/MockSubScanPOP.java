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
package org.apache.drill.exec.store.mock;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.drill.exec.physical.OperatorCost;
import org.apache.drill.exec.physical.base.AbstractBase;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.physical.base.Size;
import org.apache.drill.exec.physical.base.SubScan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;

@JsonTypeName("mock-sub-scan")
public class MockSubScanPOP extends AbstractBase implements SubScan {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MockGroupScanPOP.class);

  private final String url;
  protected final List<MockGroupScanPOP.MockScanEntry> readEntries;
  private final OperatorCost cost;
  private final Size size;
  private  LinkedList<MockGroupScanPOP.MockScanEntry>[] mappings;

  @JsonCreator
  public MockSubScanPOP(@JsonProperty("url") String url, @JsonProperty("entries") List<MockGroupScanPOP.MockScanEntry> readEntries) {
    this.readEntries = readEntries;
    OperatorCost cost = new OperatorCost(0,0,0,0);
    Size size = new Size(0,0);
    for(MockGroupScanPOP.MockScanEntry r : readEntries){
      cost = cost.add(r.getCost());
      size = size.add(r.getSize());
    }
    this.cost = cost;
    this.size = size;
    this.url = url;
  }

  public String getUrl() {
    return url;
  }

  @JsonProperty("entries")
  public List<MockGroupScanPOP.MockScanEntry> getReadEntries() {
    return readEntries;
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return Iterators.emptyIterator();
  }

  @Override
  public OperatorCost getCost() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Size getSize() {
    throw new UnsupportedOperationException();
  }

  // will want to replace these two methods with an interface above for AbstractSubScan
  @Override
  public boolean isExecutable() {
    return true;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E{
    return physicalVisitor.visitSubScan(this, value);
  }
  // see comment above about replacing this

  @Override
  @JsonIgnore
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    return new MockSubScanPOP(url, readEntries);

  }

}
