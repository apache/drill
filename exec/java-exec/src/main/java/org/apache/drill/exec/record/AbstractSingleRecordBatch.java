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
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.base.PhysicalOperator;

public abstract class AbstractSingleRecordBatch<T extends PhysicalOperator> extends AbstractRecordBatch<T> {
  final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(this.getClass());
  
  protected final RecordBatch incoming;
  private boolean first = true;
  
  public AbstractSingleRecordBatch(T popConfig, FragmentContext context, RecordBatch incoming) {
    super(popConfig, context);
    this.incoming = incoming;
  }

  @Override
  protected void killIncoming() {
    incoming.kill();
  }

  @Override
  public IterOutcome next() {
    IterOutcome upstream = incoming.next();
    if(first && upstream == IterOutcome.OK) upstream = IterOutcome.OK_NEW_SCHEMA;
    first = false;
    switch(upstream){
    case NONE:
    case NOT_YET:
    case STOP:
      cleanup();
      return upstream;
    case OK_NEW_SCHEMA:
      try{
        setupNewSchema();
      }catch(SchemaChangeException ex){
        kill();
        logger.error("Failure during query", ex);
        context.fail(ex);
        return IterOutcome.STOP;
      }
      // fall through.
    case OK:
      doWork();
      return upstream; // change if upstream changed, otherwise normal.
    default:
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public void cleanup() {
//    logger.debug("Cleaning up.");
    incoming.cleanup();
    super.cleanup();
  }

  protected abstract void setupNewSchema() throws SchemaChangeException;
  protected abstract void doWork();
}
