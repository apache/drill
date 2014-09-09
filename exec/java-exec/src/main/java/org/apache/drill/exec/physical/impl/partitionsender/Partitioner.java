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
package org.apache.drill.exec.physical.impl.partitionsender;

import java.io.IOException;
import java.util.List;

import org.apache.drill.exec.compile.TemplateClassDefinition;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.ops.OperatorStats;
import org.apache.drill.exec.physical.config.HashPartitionSender;
import org.apache.drill.exec.physical.impl.SendingAccountor;
import org.apache.drill.exec.record.RecordBatch;

public interface Partitioner {

  public abstract void setup(FragmentContext context,
                          RecordBatch incoming,
                          HashPartitionSender popConfig,
                          OperatorStats stats,
                          SendingAccountor sendingAccountor,
                          OperatorContext oContext,
                          StatusHandler statusHandler) throws SchemaChangeException;

  public abstract void partitionBatch(RecordBatch incoming) throws IOException;
  public abstract void flushOutgoingBatches(boolean isLastBatch, boolean schemaChanged) throws IOException;
  public abstract void initialize();
  public abstract void clear();
  public abstract List<? extends PartitionOutgoingBatch> getOutgoingBatches();

  public static TemplateClassDefinition<Partitioner> TEMPLATE_DEFINITION = new TemplateClassDefinition<>(Partitioner.class, PartitionerTemplate.class);
}