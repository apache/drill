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

import org.apache.drill.common.exceptions.DrillException;
import org.apache.drill.exec.compile.TemplateClassDefinition;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.record.VectorContainer;

import java.util.List;

public interface WindowFramer {
  TemplateClassDefinition<WindowFramer> TEMPLATE_DEFINITION = new TemplateClassDefinition<>(WindowFramer.class, DefaultFrameTemplate.class);

  void setup(final List<WindowDataBatch> batches, final VectorContainer container, final OperatorContext operatorContext)
    throws SchemaChangeException;

  /**
   * process the inner batch and write the aggregated values in the container
   * @throws DrillException
   */
  void doWork() throws DrillException;

  /**
   * check if current batch can be processed:
   * <ol>
   *   <li>we have at least 2 saved batches</li>
   *   <li>last partition of current batch ended</li>
   * </ol>
   * @return true if current batch can be processed, false otherwise
   */
  boolean canDoWork();

  /**
   * @return number rows processed in last batch
   */
  int getOutputCount();

  void cleanup();
}
