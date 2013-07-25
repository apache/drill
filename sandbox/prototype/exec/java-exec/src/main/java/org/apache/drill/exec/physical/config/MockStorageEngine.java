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

import java.io.IOException;
import java.util.Collection;

import org.apache.drill.common.logical.data.Scan;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.store.AbstractStorageEngine;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.StorageEngine;
import org.apache.drill.exec.store.StorageEngine.ReadEntry;

import com.google.common.collect.ListMultimap;

public class MockStorageEngine extends AbstractStorageEngine{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MockStorageEngine.class);

  @Override
  public boolean supportsRead() {
    return true;
  }

  @Override
  public Collection<ReadEntry> getReadEntries(Scan scan) throws IOException {
    return null;
  }

  @Override
  public ListMultimap<ReadEntry, DrillbitEndpoint> getReadLocations(Collection<ReadEntry> entries) {
    return null;
  }

  @Override
  public RecordReader getReader(FragmentContext context, ReadEntry readEntry) throws IOException {
    return null;
  }

  
  
}
