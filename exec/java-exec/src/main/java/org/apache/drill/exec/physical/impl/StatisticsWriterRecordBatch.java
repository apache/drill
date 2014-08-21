/*
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


import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.base.Writer;
import org.apache.drill.exec.proto.ExecProtos;
import org.apache.drill.exec.record.AbstractRecordBatch;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.store.StatisticsRecordWriter;
import org.apache.drill.exec.store.StatisticsRecordWriterImpl;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.BigIntVector;
import org.apache.drill.exec.vector.VarCharVector;

import java.io.IOException;

public class StatisticsWriterRecordBatch extends AbstractRecordBatch<Writer> {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StatisticsWriterRecordBatch.class);

  private StatisticsRecordWriterImpl statsRecordWriterImpl;
  private StatisticsRecordWriter recordWriter;
  private long counter = 0;
  private final RecordBatch incoming;
  private boolean processed = false;
  private final String fragmentUniqueId;
  private BatchSchema schema;

  public StatisticsWriterRecordBatch(Writer writer, RecordBatch incoming, FragmentContext context,
                                     StatisticsRecordWriter recordWriter) throws OutOfMemoryException {
    super(writer, context, false);
    this.incoming = incoming;

    final ExecProtos.FragmentHandle handle = context.getHandle();
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
  public BatchSchema getSchema() {
    return schema;
  }

  @Override
  public void dump() {
    logger.error("StatisticsWriterRecordBatch[container={}, popConfig={}, counter={}, fragmentUniqueId={}, schema={}]",
        container, popConfig, counter, fragmentUniqueId, schema);
  }

  @Override
  public IterOutcome innerNext() {
    if (processed) {
      // if the upstream record batch is already processed and next() is called by
      // downstream then return NONE to indicate completion
      return IterOutcome.NONE;
    }
    // process the complete upstream in one next() call
    IterOutcome upstream;
    try {
      do {
        upstream = next(incoming);

        switch(upstream) {
          case OUT_OF_MEMORY:
          case STOP:
            return upstream;

          case NOT_YET:
          case NONE:
            break;

          case OK_NEW_SCHEMA:
            setupNewSchema();
            // $FALL-THROUGH$
          case OK:
            counter += statsRecordWriterImpl.writeStatistics(incoming.getRecordCount());
            logger.debug("Total records written so far: {}", counter);

            for(final VectorWrapper<?> v : incoming) {
              v.getValueVector().clear();
            }
            break;

          default:
            throw new UnsupportedOperationException();
        }
      } while(upstream != IterOutcome.NONE);
      // Flush blocking writers now
      statsRecordWriterImpl.flushBlockingWriter();
    } catch(IOException ex) {
      logger.error("Failure during query", ex);
      kill(false);
      context.getExecutorState().fail(ex);
      return IterOutcome.STOP;
    }

    addOutputContainerData();
    processed = true;

    closeWriter();

    return IterOutcome.OK_NEW_SCHEMA;
  }

  private void addOutputContainerData() {
    final VarCharVector fragmentIdVector = (VarCharVector) container.getValueAccessorById(
        VarCharVector.class,
        container.getValueVectorId(SchemaPath.getSimplePath("Fragment")).getFieldIds())
        .getValueVector();
    AllocationHelper.allocate(fragmentIdVector, 1, 50);
    final BigIntVector summaryVector = (BigIntVector) container.getValueAccessorById(BigIntVector.class,
        container.getValueVectorId(SchemaPath.getSimplePath("Number of records written")).getFieldIds())
        .getValueVector();
    AllocationHelper.allocate(summaryVector, 1, 8);
    fragmentIdVector.getMutator().setSafe(0, fragmentUniqueId.getBytes());
    fragmentIdVector.getMutator().setValueCount(1);
    summaryVector.getMutator().setSafe(0, counter);
    summaryVector.getMutator().setValueCount(1);

    container.setRecordCount(1);
  }

  protected void setupNewSchema() throws IOException {
    try {
      // update the schema in RecordWriter
      stats.startSetup();
      recordWriter.updateSchema(incoming);
      // Create two vectors for:
      //   1. Fragment unique id.
      //   2. Summary: currently contains number of records written.
      final MaterializedField fragmentIdField =
          MaterializedField.create("Fragment", Types.required(TypeProtos.MinorType.VARCHAR));
      final MaterializedField summaryField =
          MaterializedField.create("Number of records written",
              Types.required(TypeProtos.MinorType.BIGINT));

      container.addOrGet(fragmentIdField);
      container.addOrGet(summaryField);
      container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
    } finally {
      stats.stopSetup();
    }

    statsRecordWriterImpl = new StatisticsRecordWriterImpl(incoming, recordWriter);
    container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
    schema = container.getSchema();
  }

  /** Clean up needs to be performed before closing writer. Partially written data will be removed. */
  private void closeWriter() {
    if (recordWriter == null) {
      return;
    }

    try {
      //Perform any cleanup prior to closing the writer
      recordWriter.cleanup();
    } catch(IOException ex) {
      context.getExecutorState().fail(ex);
    } finally {
      try {
        if (!processed) {
          recordWriter.abort();
        }
      } catch (IOException e) {
        logger.error("Abort failed. There could be leftover output files.", e);
      } finally {
        recordWriter = null;
      }
    }
  }

  @Override
  public void close() {
    closeWriter();
    super.close();
  }

}
