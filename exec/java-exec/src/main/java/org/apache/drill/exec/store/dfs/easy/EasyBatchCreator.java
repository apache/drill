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
package org.apache.drill.exec.store.dfs.easy;

import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.record.RecordBatch;

public class EasyBatchCreator implements BatchCreator<EasySubScan>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(EasyBatchCreator.class);

  @Override
  public RecordBatch getBatch(FragmentContext context, EasySubScan config, List<RecordBatch> children)
      throws ExecutionSetupException {
    assert children == null || children.isEmpty();
    return config.getFormatPlugin().getBatch(context, config);
  }
  
  
}
