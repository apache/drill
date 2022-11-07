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
package org.apache.drill.exec.store.delta.read;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.ExecutorFragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.record.CloseableRecordBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.ColumnExplorer;
import org.apache.drill.exec.store.delta.DeltaRowGroupScan;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.parquet.AbstractParquetRowGroupScan;
import org.apache.drill.exec.store.parquet.AbstractParquetScanBatchCreator;
import org.apache.drill.exec.store.parquet.ParquetScanBatchCreator;
import org.apache.drill.exec.store.parquet.RowGroupReadEntry;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;

import java.util.List;
import java.util.Map;

@SuppressWarnings("unused")
public class DeltaScanBatchCreator extends AbstractParquetScanBatchCreator
  implements BatchCreator<DeltaRowGroupScan> {

  @Override
  public CloseableRecordBatch getBatch(ExecutorFragmentContext context, DeltaRowGroupScan rowGroupScan,
    List<RecordBatch> children) throws ExecutionSetupException {
    Preconditions.checkArgument(children.isEmpty());
    OperatorContext oContext = context.newOperatorContext(rowGroupScan);
    return getBatch(context, rowGroupScan, oContext);
  }

  @Override
  protected AbstractDrillFileSystemManager getDrillFileSystemCreator(
    OperatorContext operatorContext, OptionManager optionManager) {
    return new ParquetScanBatchCreator.ParquetDrillFileSystemManager(operatorContext,
      optionManager.getOption(ExecConstants.PARQUET_PAGEREADER_ASYNC).bool_val);
  }

  @Override
  protected Map<String, String> getImplicitValues(AbstractParquetRowGroupScan rowGroupScan,
    ColumnExplorer columnExplorer, RowGroupReadEntry rowGroup, DrillFileSystem fs) {
    return ((DeltaRowGroupScan) rowGroupScan).getPartitions(rowGroup);
  }

}
