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

import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.exec.ref.*;
import org.apache.drill.exec.ref.eval.EvaluatorFactory;
import org.apache.drill.exec.ref.exceptions.SetupException;

public abstract class ROPBase<T extends LogicalOperator> implements ROP{

  private boolean alreadyUsed = false;
  protected final T config;

  
  public ROPBase(T config) {
    if(config == null) throw new IllegalArgumentException("Config must be defined.");
    this.config = config;
  }

  protected void setupEvals(EvaluatorFactory builder) throws SetupException{};
  protected void setupIterators(IteratorRegistry registry) throws SetupException{};
  protected abstract RecordIterator getIteratorInternal();
  
  @Override
  public void init(IteratorRegistry registry, EvaluatorFactory builder) throws SetupException {
    if(config != null) registry.register(config, this);
    setupIterators(registry);
    setupEvals(builder);
  }

  @Override
  public final RecordIterator getOutput() {
    if(alreadyUsed) throw new IllegalStateException("You can only request the ouput of a reference operator once.");
    alreadyUsed = true;
    return getIteratorInternal();
  }
  
  @Override
  public void cleanup(RunOutcome.OutcomeType outcome) {
  }


  

}
