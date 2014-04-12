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
package org.apache.drill.exec.physical.impl.TopN;

import org.apache.drill.exec.compile.TemplateClassDefinition;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.sort.RecordBatchData;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.selection.SelectionVector4;

public interface PriorityQueue {
  public void add(FragmentContext context, RecordBatchData batch) throws SchemaChangeException;
  public void init(int limit, FragmentContext context, BufferAllocator allocator, boolean hasSv2) throws SchemaChangeException;
  public void generate() throws SchemaChangeException;
  public VectorContainer getHyperBatch();
  public SelectionVector4 getHeapSv4();
  public SelectionVector4 getFinalSv4();
  public boolean validate();
  public void resetQueue(VectorContainer container, SelectionVector4 vector4) throws SchemaChangeException;
  public void cleanup();

  public static TemplateClassDefinition<PriorityQueue> TEMPLATE_DEFINITION = new TemplateClassDefinition<PriorityQueue>(PriorityQueue.class, PriorityQueueTemplate.class);

}
