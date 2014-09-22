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

import com.google.common.collect.Iterables;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.WindowPOP;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.record.RecordBatch;

import java.util.List;

public class StreamingWindowFrameBatchCreator implements BatchCreator<WindowPOP> {

  @Override
  public RecordBatch getBatch(FragmentContext context, WindowPOP config, List<RecordBatch> children) throws ExecutionSetupException {
    return new StreamingWindowFrameRecordBatch(config, context, Iterables.getOnlyElement(children));
  }
}
