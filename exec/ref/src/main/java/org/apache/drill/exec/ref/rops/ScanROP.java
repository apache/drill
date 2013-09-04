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
package org.apache.drill.exec.ref.rops;

import java.io.IOException;

import org.apache.drill.common.logical.data.Scan;
import org.apache.drill.exec.ref.IteratorRegistry;
import org.apache.drill.exec.ref.RecordIterator;
import org.apache.drill.exec.ref.RunOutcome.OutcomeType;
import org.apache.drill.exec.ref.exceptions.SetupException;
import org.apache.drill.exec.ref.rse.RecordReader;
import org.apache.drill.exec.ref.rse.ReferenceStorageEngine;
import org.apache.drill.exec.ref.rse.ReferenceStorageEngine.ReadEntry;

public class ScanROP extends ROPBase<Scan>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ScanROP.class);
  
  private ReadEntry entry;
  private ReferenceStorageEngine engine;
  private RecordReader reader; 
      
  public ScanROP(Scan config, ReadEntry entry, ReferenceStorageEngine engine) {
    super(config);
    this.entry = entry;
    this.engine = engine;
    
  }

  
  @Override
  protected void setupIterators(IteratorRegistry registry) throws SetupException {
    try{
      super.setupIterators(registry);
      reader = engine.getReader(entry, this);
      reader.setup();
    }catch(IOException e){
      throw new SetupException(String.format("Failure while setting up reader for Entry %s.", entry), e);
    }
  }


  @Override
  protected RecordIterator getIteratorInternal() {
    return reader.getIterator();
  }
  
  
  @Override
  public void cleanup(OutcomeType outcome) {
    reader.cleanup();
    super.cleanup(outcome);
  }

}
