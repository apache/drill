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

package org.apache.drill.exec.physical.impl.window;

import org.apache.drill.exec.compile.TemplateClassDefinition;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.RecordBatch;

public interface StreamingWindowFramer {
  public static TemplateClassDefinition<StreamingWindowFramer> TEMPLATE_DEFINITION = new TemplateClassDefinition<>(StreamingWindowFramer.class, StreamingWindowFrameTemplate.class);


  public static enum AggOutcome {
    RETURN_OUTCOME, UPDATE_AGGREGATOR, RETURN_AND_COMPLETE;
  }

  public abstract void setup(FragmentContext context, RecordBatch incoming, RecordBatch outgoing,
                             int precedingConfig, int followingConfig) throws SchemaChangeException;

  public abstract RecordBatch.IterOutcome getOutcome();

  public abstract int getOutputCount();

  public abstract AggOutcome doWork();

  public abstract void cleanup();
}
