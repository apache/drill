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
package org.apache.drill.exec.ref.rops;

import java.io.IOException;
import java.util.concurrent.locks.ReadWriteLock;

import org.apache.drill.common.logical.data.SinkOperator;
import org.apache.drill.exec.ref.RunOutcome;
import org.apache.drill.exec.ref.RecordIterator;
import org.apache.drill.exec.ref.RecordPointer;
import org.apache.drill.exec.ref.RunOutcome.OutcomeType;
import org.apache.drill.exec.ref.RecordIterator.NextOutcome;
import org.apache.drill.exec.ref.eval.EvaluatorFactory;
import org.apache.drill.exec.ref.exceptions.SetupException;

public abstract class BaseSinkROP<T extends SinkOperator> extends SingleInputROPBase<T> implements SinkROP{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BaseSinkROP.class);
  
  protected RecordIterator iter;
  protected RecordPointer record;
  
  public BaseSinkROP(T config) {
    super(config);
  }
  
  @Override
  protected void setupEvals(EvaluatorFactory builder) throws SetupException {
    try {
      setupSink();
    } catch (IOException e) {
      throw new SetupException(String.format("failure setting up %s sink rop.", this.getClass()), e);
    }
  }
  
  @Override
  protected void setInput(RecordIterator incoming) {
    iter = incoming;
    record = incoming.getRecordPointer();
  }

  @Override
  public RecordIterator getIteratorInternal() {
    throw new UnsupportedOperationException("A ReferenceSink");
  }
  
  @Override
  public RunOutcome run(StatusHandle handle) {
    Throwable exception = null;
    final int runsize = 1000;
    int recordCount = 0;
    OutcomeType outcome = OutcomeType.FAILED;
    long pos = -1; 
    try{
    while(true){
      boolean more = true;
      for(;recordCount < runsize; recordCount++){
        NextOutcome r = iter.next();
        if(r == NextOutcome.NONE_LEFT){
          more = false;
          break;
        }else{
          pos = sinkRecord(record);
        }
      }
      handle.progress(pos, recordCount);
      if(!handle.okToContinue()){
        logger.debug("Told to cancel, breaking run.");
        outcome = OutcomeType.CANCELED;
        break;
      }else if(!more){
        outcome = OutcomeType.SUCCESS;
        logger.debug("Breaking because no more records were found.");
        break;
      }else{
        logger.debug("No problems, doing next progress iteration.");
      }
      
    }
    }catch(Exception e){
      exception = e ;
    }
    
    cleanup(outcome);
    return new RunOutcome(outcome, pos, recordCount, exception);
    
  }

  /**
   * 
   * @param r RecordPointer to record
   * @return The approximate amount of bytes written.
   * @throws IOException
   */
  public abstract long sinkRecord(RecordPointer r) throws IOException;
  protected abstract void setupSink() throws IOException;

}
