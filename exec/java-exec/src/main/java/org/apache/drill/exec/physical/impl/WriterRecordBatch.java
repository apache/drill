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

package org.apache.drill.exec.physical.impl;

import java.io.IOException;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.base.Writer;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.record.AbstractRecordBatch;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordValueAccessor;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.store.EventBasedRecordWriter;
import org.apache.drill.exec.store.RecordWriter;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.BigIntVector;
import org.apache.drill.exec.vector.VarCharVector;

/* Write the RecordBatch to the given RecordWriter. */
public class WriterRecordBatch extends AbstractRecordBatch<Writer> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(WriterRecordBatch.class);

  private EventBasedRecordWriter eventBasedRecordWriter;
  private RecordWriter recordWriter;
  private int counter = 0;
  private final RecordBatch incoming;
  private boolean first = true;
  private boolean processed = false;
  private String fragmentUniqueId;

  public WriterRecordBatch(Writer writer, RecordBatch incoming, FragmentContext context, RecordWriter recordWriter) throws OutOfMemoryException {
    super(writer, context);
    this.incoming = incoming;

    FragmentHandle handle = context.getHandle();
    fragmentUniqueId = String.format("%d_%d", handle.getMajorFragmentId(), handle.getMinorFragmentId());
    this.recordWriter = recordWriter;
  }

  @Override
  public int getRecordCount() {
    return container.getRecordCount();
  }

  @Override
  protected void killIncoming(boolean sendUpstream) {
    incoming.kill(sendUpstream);
  }

  @Override
  public IterOutcome innerNext() {
    if(processed) {
      // if the upstream record batch is already processed and next() is called by
      // downstream then return NONE to indicate completion
      return IterOutcome.NONE;
    }

    // process the complete upstream in one next() call
    IterOutcome upstream;
    do {
      upstream = next(incoming);
      if(first && upstream == IterOutcome.OK)
        upstream = IterOutcome.OK_NEW_SCHEMA;
      first = false;

      switch(upstream) {
        case NOT_YET:
        case NONE:
        case STOP:
          cleanup();
          if (upstream == IterOutcome.STOP)
            return upstream;
          break;

        case OK_NEW_SCHEMA:
          try{
            setupNewSchema();
          }catch(Exception ex){
            kill(false);
            logger.error("Failure during query", ex);
            context.fail(ex);
            return IterOutcome.STOP;
          }
          // fall through.
        case OK:
          try {
            counter += eventBasedRecordWriter.write(incoming.getRecordCount());
            logger.debug("Total records written so far: {}", counter);
          } catch(IOException ex) {
            throw new RuntimeException(ex);
          }

          for(VectorWrapper v : incoming)
            v.getValueVector().clear();

          break;

        default:
          throw new UnsupportedOperationException();
      }
    } while(upstream != IterOutcome.NONE);

    // Create two vectors for:
    //   1. Fragment unique id.
    //   2. Summary: currently contains number of records written.
    MaterializedField fragmentIdField = MaterializedField.create(SchemaPath.getSimplePath("Fragment"), Types.required(MinorType.VARCHAR));
    MaterializedField summaryField = MaterializedField.create(SchemaPath.getSimplePath("Number of records written"), Types.required(MinorType.BIGINT));

    VarCharVector fragmentIdVector = (VarCharVector) TypeHelper.getNewVector(fragmentIdField, context.getAllocator());
    AllocationHelper.allocate(fragmentIdVector, 1, TypeHelper.getSize(Types.required(MinorType.VARCHAR)));
    BigIntVector summaryVector = (BigIntVector) TypeHelper.getNewVector(summaryField, context.getAllocator());
    AllocationHelper.allocate(summaryVector, 1, TypeHelper.getSize(Types.required(MinorType.VARCHAR)));


    container.add(fragmentIdVector);
    container.add(summaryVector);
    container.buildSchema(BatchSchema.SelectionVectorMode.NONE);

    fragmentIdVector.getMutator().setSafe(0, fragmentUniqueId.getBytes());
    fragmentIdVector.getMutator().setValueCount(1);
    summaryVector.getMutator().setSafe(0, counter);
    summaryVector.getMutator().setValueCount(1);

    container.setRecordCount(1);
    processed = true;

    return IterOutcome.OK_NEW_SCHEMA;
  }

  protected void setupNewSchema() throws Exception {
    try {
      // update the schema in RecordWriter
      stats.startSetup();
      recordWriter.updateSchema(incoming.getSchema());
    } catch(IOException ex) {
      throw new RuntimeException("Failed to update schema in RecordWriter", ex);
    } finally{
      stats.stopSetup();
    }

    eventBasedRecordWriter = new EventBasedRecordWriter(incoming, recordWriter);
  }

  @Override
  public void cleanup() {
    super.cleanup();
    incoming.cleanup();
    try {
      if (recordWriter != null) {
        recordWriter.cleanup();
      }
    } catch(IOException ex) {
      throw new RuntimeException("Failed to close RecordWriter", ex);
    }
  }
}
