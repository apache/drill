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
package org.apache.drill.exec.record;

import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.vector.SchemaChangeCallBack;

public abstract class AbstractSingleRecordBatch<T extends PhysicalOperator> extends AbstractRecordBatch<T> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(new Object() {}.getClass().getEnclosingClass());

  protected final RecordBatch incoming;
  protected boolean outOfMemory = false;
  protected SchemaChangeCallBack callBack = new SchemaChangeCallBack();

  public AbstractSingleRecordBatch(T popConfig, FragmentContext context, RecordBatch incoming) throws OutOfMemoryException {
    super(popConfig, context, false);
    this.incoming = incoming;
  }

  @Override
  protected void killIncoming(boolean sendUpstream) {
    incoming.kill(sendUpstream);
  }

  @Override
  public IterOutcome innerNext() {
    // Short circuit if record batch has already sent all data and is done
    if (state == BatchState.DONE) {
      return IterOutcome.NONE;
    }

    IterOutcome upstream = next(incoming);
    if (state != BatchState.FIRST && upstream == IterOutcome.OK && incoming.getRecordCount() == 0) {
      do {
        for (final VectorWrapper<?> w : incoming) {
          w.clear();
        }
      } while ((upstream = next(incoming)) == IterOutcome.OK && incoming.getRecordCount() == 0);
    }
    if ((state == BatchState.FIRST) && upstream == IterOutcome.OK) {
      upstream = IterOutcome.OK_NEW_SCHEMA;
    }
    switch (upstream) {
    case NONE:
    case NOT_YET:
    case STOP:
      if (state == BatchState.FIRST) {
        container.buildSchema(SelectionVectorMode.NONE);
      }
      return upstream;
    case OUT_OF_MEMORY:
      return upstream;
    case OK_NEW_SCHEMA:
      if (state == BatchState.FIRST) {
        state = BatchState.NOT_FIRST;
      }
      try {
        stats.startSetup();
        if (!setupNewSchema()) {
          upstream = IterOutcome.OK;
        }
      } catch (SchemaChangeException ex) {
        kill(false);
        logger.error("Failure during query", ex);
        context.fail(ex);
        return IterOutcome.STOP;
      } finally {
        stats.stopSetup();
      }
      // fall through.
    case OK:
      assert state != BatchState.FIRST : "First batch should be OK_NEW_SCHEMA";
      container.zeroVectors();
      IterOutcome out = doWork();

      // since doWork method does not know if there is a new schema, it will always return IterOutcome.OK if it was successful.
      // But if upstream is IterOutcome.OK_NEW_SCHEMA, we should return that
      if (out != IterOutcome.OK) {
        upstream = out;
      }

      if (outOfMemory) {
        outOfMemory = false;
        return IterOutcome.OUT_OF_MEMORY;
      }

      // Check if schema has changed
      if (callBack.getSchemaChangedAndReset()) {
        return IterOutcome.OK_NEW_SCHEMA;
      }

      return upstream; // change if upstream changed, otherwise normal.
    default:
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public BatchSchema getSchema() {
    if (container.hasSchema()) {
      return container.getSchema();
    }

    return null;
  }

  protected abstract boolean setupNewSchema() throws SchemaChangeException;
  protected abstract IterOutcome doWork();
}
