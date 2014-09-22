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

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.base.PhysicalOperator;

public abstract class AbstractSingleRecordBatch<T extends PhysicalOperator> extends AbstractRecordBatch<T> {
  final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(this.getClass());

  protected final RecordBatch incoming;
  private boolean first = true;
  protected boolean done = false;
  protected boolean outOfMemory = false;

  public AbstractSingleRecordBatch(T popConfig, FragmentContext context, RecordBatch incoming) throws OutOfMemoryException {
    super(popConfig, context);
    this.incoming = incoming;
  }

  @Override
  protected void killIncoming(boolean sendUpstream) {
    incoming.kill(sendUpstream);
  }

  @Override
  public IterOutcome innerNext() {
    // Short circuit if record batch has already sent all data and is done
    if (done) {
      return IterOutcome.NONE;
    }

    IterOutcome upstream = next(incoming);
    if (!first && upstream == IterOutcome.OK && incoming.getRecordCount() == 0) {
      do {
        for (VectorWrapper w : incoming) {
          w.clear();
        }
      } while ((upstream = next(incoming)) == IterOutcome.OK && incoming.getRecordCount() == 0);
    }
    if (first && upstream == IterOutcome.OK) {
      upstream = IterOutcome.OK_NEW_SCHEMA;
    }
    switch (upstream) {
    case NONE:
      assert !first;
    case NOT_YET:
    case STOP:
      return upstream;
    case OUT_OF_MEMORY:
      return upstream;
    case OK_NEW_SCHEMA:
      first = false;
      try {
        stats.startSetup();
        setupNewSchema();
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
      assert !first : "First batch should be OK_NEW_SCHEMA";
      doWork();
      if (outOfMemory) {
        outOfMemory = false;
        return IterOutcome.OUT_OF_MEMORY;
      }
      return upstream; // change if upstream changed, otherwise normal.
    default:
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public void cleanup() {
//    logger.debug("Cleaning up.");
    super.cleanup();
    incoming.cleanup();
  }

  @Override
  public BatchSchema getSchema() {
    return container.getSchema();
  }

  protected abstract void setupNewSchema() throws SchemaChangeException;
  protected abstract IterOutcome doWork();
}
