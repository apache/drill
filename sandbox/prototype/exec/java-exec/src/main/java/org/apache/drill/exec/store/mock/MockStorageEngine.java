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

import java.io.IOException;
import java.util.ArrayList;

import org.apache.drill.common.logical.data.Scan;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStorageEngine;
import org.apache.drill.exec.store.SchemaProvider;
import org.apache.drill.exec.store.mock.MockGroupScanPOP.MockScanEntry;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class MockStorageEngine extends AbstractStorageEngine {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MockStorageEngine.class);

  public MockStorageEngine(MockStorageEngineConfig configuration, DrillbitContext context) {

  }

  @Override
  public AbstractGroupScan getPhysicalScan(Scan scan) throws IOException {

    ArrayList<MockScanEntry> readEntries = scan.getSelection().getListWith(new ObjectMapper(),
        new TypeReference<ArrayList<MockScanEntry>>() {
        });
    
    return new MockGroupScanPOP(null, readEntries);
  }

  @Override
  public SchemaProvider getSchemaProvider() {
    throw new UnsupportedOperationException();
  }

}
