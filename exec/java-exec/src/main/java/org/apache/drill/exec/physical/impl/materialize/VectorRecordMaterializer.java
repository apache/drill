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
package org.apache.drill.exec.physical.impl.materialize;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.QueryData;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.WritableBatch;

public class VectorRecordMaterializer implements RecordMaterializer{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(VectorRecordMaterializer.class);

  private QueryId queryId;
  private RecordBatch batch;
  private BufferAllocator allocator;

  public VectorRecordMaterializer(FragmentContext context, OperatorContext oContext, RecordBatch batch) {
    this.queryId = context.getHandle().getQueryId();
    this.batch = batch;
    this.allocator = oContext.getAllocator();
    BatchSchema schema = batch.getSchema();
    assert schema != null : "Schema must be defined.";

//    for (MaterializedField f : batch.getSchema()) {
//      logger.debug("New Field: {}", f);
//    }
  }

  public QueryWritableBatch convertNext() {
    //batch.getWritableBatch().getDef().getRecordCount()
    WritableBatch w = batch.getWritableBatch().transfer(allocator);

    QueryData header = QueryData.newBuilder() //
        .setQueryId(queryId) //
        .setRowCount(batch.getRecordCount()) //
        .setDef(w.getDef()).build();
    QueryWritableBatch batch = new QueryWritableBatch(header, w.getBuffers());
    return batch;
  }
}
