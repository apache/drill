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
package org.apache.drill.exec.ref;

import java.util.Iterator;

import org.apache.drill.exec.ref.rops.ROP;


public interface RecordIterator{
  
  public enum NextOutcome {NONE_LEFT, INCREMENTED_SCHEMA_UNCHANGED, INCREMENTED_SCHEMA_CHANGED}
  public RecordPointer getRecordPointer(); // called once
  public NextOutcome next();
  public ROP getParent();
  
  
  public static class IteratorWrapper implements RecordIterator{
    final Iterator<RecordPointer> iter;
    final RecordPointer outputRecord;
    final ROP parent;
    public IteratorWrapper(ROP rop, Iterator<RecordPointer> iter, RecordPointer outputRecord) {
      this.iter = iter;
      this.parent = rop;
      this.outputRecord = outputRecord;
    }
    
    @Override
    public NextOutcome next() {
      if(iter.hasNext()) {
        outputRecord.copyFrom(iter.next());
        return NextOutcome.INCREMENTED_SCHEMA_CHANGED;
      }
      
      return NextOutcome.NONE_LEFT;
    }

    @Override
    public ROP getParent() {
      return parent;
    }

    @Override
    public RecordPointer getRecordPointer() {
      return outputRecord;
    }

    
    
    
  }
}
