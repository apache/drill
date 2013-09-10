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

import java.util.List;

import org.apache.drill.common.logical.data.SingleInputOperator;
import org.apache.drill.exec.ref.IteratorRegistry;
import org.apache.drill.exec.ref.RecordIterator;
import org.apache.drill.exec.ref.RecordPointer;
import org.apache.drill.exec.ref.exceptions.SetupException;

public abstract class SingleInputROPBase<T extends SingleInputOperator> extends ROPBase<T>{

  protected RecordPointer record;

  public SingleInputROPBase(T config) {
    super(config);
  }

  @Override
  protected void setupIterators(IteratorRegistry registry) {
    List<RecordIterator> iters = registry.getOperator(config.getInput());
    if(iters.size() != 1) throw new IllegalArgumentException(String.format("Expected one input iterator for class %s.  Received %d", this.getClass().getCanonicalName(), iters.size()));
    RecordIterator i = iters.get(0);
    this.record = i.getRecordPointer();
    if(record == null) throw new SetupException(String.format("The %s op iterator return a null record pointer.", i.getParent()));
    setInput(i);
  }

  protected abstract void setInput(RecordIterator incoming);
  
  
}
