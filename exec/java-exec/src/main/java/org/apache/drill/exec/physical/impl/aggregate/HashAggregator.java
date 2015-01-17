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

import java.io.IOException;
import java.util.List;

import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.exec.compile.TemplateClassDefinition;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorStats;
import org.apache.drill.exec.physical.config.HashAggregate;
import org.apache.drill.exec.physical.impl.common.HashTableConfig;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatch.IterOutcome;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorContainer;

public interface HashAggregator {

  public static TemplateClassDefinition<HashAggregator> TEMPLATE_DEFINITION =
      new TemplateClassDefinition<HashAggregator>(HashAggregator.class, HashAggTemplate.class);

  public static enum AggOutcome {
    RETURN_OUTCOME, CLEANUP_AND_RETURN, UPDATE_AGGREGATOR
  }

  public abstract void setup(HashAggregate hashAggrConfig, HashTableConfig htConfig, FragmentContext context,
      OperatorStats stats, BufferAllocator allocator, RecordBatch incoming, HashAggBatch outgoing,
      LogicalExpression[] valueExprs, List<TypedFieldId> valueFieldIds, TypedFieldId[] keyFieldIds,
      VectorContainer outContainer) throws SchemaChangeException, IOException, ClassTransformationException;

  public abstract IterOutcome getOutcome();

  public abstract int getOutputCount();

  public abstract AggOutcome doWork();

  public abstract void cleanup();

  public abstract boolean allFlushed();

  public abstract boolean buildComplete();

  public abstract IterOutcome outputCurrentBatch();

}
