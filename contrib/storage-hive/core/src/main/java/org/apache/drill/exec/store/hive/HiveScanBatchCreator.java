/*
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
package org.apache.drill.exec.store.hive;

import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.ExecutorFragmentContext;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.store.hive.readers.initilializers.AbstractReadersInitializer;
import org.apache.drill.exec.store.hive.readers.initilializers.ReadersInitializer;

@SuppressWarnings("unused")
public class HiveScanBatchCreator implements BatchCreator<HiveSubScan> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HiveScanBatchCreator.class);

  @Override
  public ScanBatch getBatch(ExecutorFragmentContext context, HiveSubScan config, List<RecordBatch> children)
      throws ExecutionSetupException {
    AbstractReadersInitializer readersInitializer = ReadersInitializer.getInitializer(context, config);
    return new ScanBatch(config, context, readersInitializer.init());
  }
}
