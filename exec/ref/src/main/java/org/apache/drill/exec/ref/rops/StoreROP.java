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

import org.apache.drill.common.logical.data.Store;
import org.apache.drill.exec.ref.RecordPointer;
import org.apache.drill.exec.ref.RunOutcome.OutcomeType;
import org.apache.drill.exec.ref.rse.RecordRecorder;
import org.apache.drill.exec.ref.rse.ReferenceStorageEngine;

public class StoreROP extends BaseSinkROP<Store> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StoreROP.class);
  
  private ReferenceStorageEngine engine;
  private RecordRecorder writer;
  
  public StoreROP(Store config, ReferenceStorageEngine engine) {
    super(config);
    this.engine = engine;
  }
  
  @Override
  public long sinkRecord(RecordPointer r) throws IOException {
    return writer.recordRecord(r);
  }

  @Override
  protected void setupSink() throws IOException {
    writer = engine.getWriter(config);
    writer.setup();
  }

  @Override
  public void cleanup(OutcomeType outcome) {
    
    try {
      writer.finish(outcome);
    } catch (IOException e) {
      logger.warn("Failure while cleaning up storage ROP", e);
    }
    super.cleanup(outcome);
  }
  
  

}
