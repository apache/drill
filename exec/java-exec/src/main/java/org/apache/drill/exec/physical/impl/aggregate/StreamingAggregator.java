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
package org.apache.drill.exec.physical.impl.aggregate;

import org.apache.drill.exec.compile.TemplateClassDefinition;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatch.IterOutcome;

public interface StreamingAggregator {

  public static TemplateClassDefinition<StreamingAggregator> TEMPLATE_DEFINITION = new TemplateClassDefinition<StreamingAggregator>(StreamingAggregator.class, StreamingAggTemplate.class);

  public static enum AggOutcome {
    RETURN_OUTCOME, CLEANUP_AND_RETURN, UPDATE_AGGREGATOR;
  }

  public abstract void setup(FragmentContext context, RecordBatch incoming, StreamingAggBatch outgoing) throws SchemaChangeException;

  public abstract IterOutcome getOutcome();

  public abstract int getOutputCount();

  public abstract AggOutcome doWork();

  public abstract void cleanup();

}
