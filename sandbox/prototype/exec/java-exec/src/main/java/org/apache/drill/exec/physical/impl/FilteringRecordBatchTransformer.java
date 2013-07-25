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
package org.apache.drill.exec.physical.impl;

import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.selection.SelectionVector2;

public abstract class FilteringRecordBatchTransformer {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FilteringRecordBatchTransformer.class);
  
  final RecordBatch incoming;
  final SelectionVector2 selectionVector;
  final BatchSchema schema;
  
  public FilteringRecordBatchTransformer(RecordBatch incoming, OutputMutator output, SelectionVector2 selectionVector) {
    super();
    this.incoming = incoming;
    this.selectionVector = selectionVector;
    this.schema = innerSetup();
  }

  public abstract BatchSchema innerSetup();
  
  /**
   * Applies the filter to the selection index.  Ignores any values in the selection vector, instead creating a.
   * @return
   */
  public abstract int apply();
  
  /**
   * Applies the filter to the selection index.  Utilizes the existing selection index and only evaluates on those records.
   * @return
   */
  public abstract int applyWithSelection();

  public BatchSchema getSchema() {
    return schema;
  }
  
  
  
}
