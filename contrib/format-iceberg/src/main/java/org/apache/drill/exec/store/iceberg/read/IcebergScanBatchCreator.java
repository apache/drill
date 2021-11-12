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
package org.apache.drill.exec.store.iceberg.read;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.ops.ExecutorFragmentContext;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.physical.impl.scan.framework.BasicScanFactory;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedScanFramework;
import org.apache.drill.exec.physical.impl.scan.framework.SchemaNegotiator;
import org.apache.drill.exec.record.CloseableRecordBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.store.iceberg.IcebergSubScan;
import org.apache.drill.exec.store.iceberg.IcebergWork;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.iceberg.TableScan;

import java.util.List;

@SuppressWarnings("unused")
public class IcebergScanBatchCreator implements BatchCreator<IcebergSubScan> {

  @Override
  public CloseableRecordBatch getBatch(ExecutorFragmentContext context,
    IcebergSubScan subScan, List<RecordBatch> children) throws ExecutionSetupException {
    Preconditions.checkArgument(children.isEmpty());

    try {
      ManagedScanFramework.ScanFrameworkBuilder builder = createBuilder(subScan);
      return builder.buildScanOperator(context, subScan);
    } catch (UserException e) {
      // Rethrow user exceptions directly
      throw e;
    } catch (Throwable e) {
      // Wrap all others
      throw new ExecutionSetupException(e);
    }
  }

  private ManagedScanFramework.ScanFrameworkBuilder createBuilder(IcebergSubScan subScan) {
    ManagedScanFramework.ScanFrameworkBuilder builder = new ManagedScanFramework.ScanFrameworkBuilder();
    builder.projection(subScan.getColumns());
    builder.setUserName(subScan.getUserName());
    builder.providedSchema(subScan.getSchema());

    ManagedScanFramework.ReaderFactory readerFactory = new BasicScanFactory(
      subScan.getWorkList().stream()
          .map(icebergWork -> getRecordReader(subScan.getTableScan(), icebergWork, subScan.getMaxRecords()))
          .iterator());
    builder.setReaderFactory(readerFactory);
    builder.nullType(Types.optional(TypeProtos.MinorType.VARCHAR));
    return builder;
  }

  private static ManagedReader<SchemaNegotiator> getRecordReader(TableScan tableScan, IcebergWork icebergWork, int maxRecords) {
    return new IcebergRecordReader(tableScan, icebergWork, maxRecords);
  }
}
