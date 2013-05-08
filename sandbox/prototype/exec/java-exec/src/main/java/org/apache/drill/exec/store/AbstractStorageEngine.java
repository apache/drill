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

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.drill.common.logical.data.Scan;
import org.apache.drill.common.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.ops.FragmentContext;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;

public class AbstractStorageEngine implements StorageEngine{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractStorageEngine.class);

  @Override
  public boolean supportsRead() {
    return false;
  }

  @Override
  public boolean supportsWrite() {
    return false;
  }

  @Override
  public List<QueryOptimizerRule> getOptimizerRules() {
    return Collections.emptyList();
  }

  @Override
  public Collection<ReadEntry> getReadEntries(Scan scan) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListMultimap<ReadEntry, DrillbitEndpoint> getReadLocations(Collection<ReadEntry> entries) {
    throw new UnsupportedOperationException();
  }

  @Override
  public RecordReader getReader(FragmentContext context, ReadEntry readEntry) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public RecordRecorder getWriter(FragmentContext context, WriteEntry writeEntry) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Multimap<DrillbitEndpoint, ReadEntry> getEntryAssignments(List<DrillbitEndpoint> assignments,
      Collection<ReadEntry> entries) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Multimap<DrillbitEndpoint, WriteEntry> getWriteAssignments(List<DrillbitEndpoint> assignments,
      Collection<ReadEntry> entries) {
    throw new UnsupportedOperationException();
  }
  
  
}
