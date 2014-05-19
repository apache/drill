/**
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
 */
package org.apache.drill.exec.physical.impl.partitionsender;

import javax.inject.Named;

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.selection.SelectionVector2;

public abstract class PartitionerSV2Template implements Partitioner {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PartitionerSV2Template.class);

  private SelectionVector2 sv2;

  public PartitionerSV2Template() throws SchemaChangeException {
  }

  @Override
  public final void setup(FragmentContext context,
                          RecordBatch incoming,
                          OutgoingRecordBatch[] outgoing) throws SchemaChangeException {

    this.sv2 = incoming.getSelectionVector2();

    doSetup(context, incoming, outgoing);

  }

  @Override
  public void partitionBatch(RecordBatch incoming) {

    for (int recordId = 0; recordId < incoming.getRecordCount(); ++recordId) {
      // for each record
      doEval(sv2.getIndex(recordId), 0);
    }

  }

  public abstract void doSetup(@Named("context") FragmentContext context, @Named("incoming") RecordBatch incoming, @Named("outgoing") OutgoingRecordBatch[] outgoing) throws SchemaChangeException;
  public abstract void doEval(@Named("inIndex") int inIndex, @Named("outIndex") int outIndex);


}
