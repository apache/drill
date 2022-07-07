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
package org.apache.drill.exec.store.druid;

import org.apache.drill.common.exceptions.ChildErrorContext;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.ops.ExecutorFragmentContext;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedScanFramework;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedScanFramework.ReaderFactory;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedScanFramework.ScanFrameworkBuilder;
import org.apache.drill.exec.physical.impl.scan.framework.SchemaNegotiator;
import org.apache.drill.exec.record.CloseableRecordBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.RecordReader;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.drill.exec.store.druid.DruidSubScan.DruidSubScanSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;

public class DruidScanBatchCreator implements BatchCreator<DruidSubScan> {

  private static final Logger logger = LoggerFactory.getLogger(DruidScanBatchCreator.class);

  /*
    @Override
  public CloseableRecordBatch getBatch(ExecutorFragmentContext context,
                                       SplunkSubScan subScan, List<RecordBatch> children) throws ExecutionSetupException {
    Preconditions.checkArgument(children.isEmpty());

    try {
      ScanFrameworkBuilder builder = createBuilder(context.getOptions(), subScan);
      return builder.buildScanOperator(context, subScan);
    } catch (UserException e) {
      // Rethrow user exceptions directly
      throw e;
    } catch (Throwable e) {
      // Wrap all others
      throw new ExecutionSetupException(e);
    }
  }
   */


  @Override
  public CloseableRecordBatch getBatch(ExecutorFragmentContext context, DruidSubScan subScan, List<RecordBatch> children) throws ExecutionSetupException {
    Preconditions.checkArgument(children.isEmpty());
    List<RecordReader> readers = Lists.newArrayList();
    List<SchemaPath> columns;

    for (DruidSubScan.DruidSubScanSpec scanSpec : subScan.getScanSpec()) {
      try {
        ScanFrameworkBuilder builder = createBuilder(context.getOptions(), subScan, context);
        columns = subScan.getColumns();
        readers.add(new DruidRecordReader(scanSpec, columns, subScan.getMaxRecordsToRead(), context, subScan.getStorageEngine()));
      } catch (Exception ex) {
        throw new ExecutionSetupException(ex);
      }
    }
    logger.debug("Number of record readers initialized - {}", readers.size());
    return new ScanBatch(subScan, context, readers);
  }

  private ScanFrameworkBuilder createBuilder(OptionManager options,
                                             DruidSubScan subScan,
                                             ExecutorFragmentContext context) {
    ScanFrameworkBuilder builder = new ScanFrameworkBuilder();
    builder.projection(subScan.getColumns());
    builder.providedSchema(subScan.getSchema());
    builder.setUserName(subScan.getUserName());
    // Provide custom error context
    builder.errorContext(new ChildErrorContext(builder.errorContext()) {});

    // Reader
    ReaderFactory readerFactory = new DruidReaderFactory(subScan, context);
    builder.setReaderFactory(readerFactory);
    builder.nullType(Types.optional(MinorType.VARCHAR));

    return builder;
  }

  private static class DruidReaderFactory implements ReaderFactory {
    private final DruidSubScan subScan;
    private final ExecutorFragmentContext context;
    private final Iterator<DruidSubScanSpec> subScanSpecIterator;
    public DruidReaderFactory(DruidSubScan subScan, ExecutorFragmentContext context) {
      this.subScan = subScan;
      this.context = context;
      this.subScanSpecIterator = subScan.getScanSpec().iterator();
    }

    @Override
    public void bind(ManagedScanFramework framework) { }

    @Override
    public ManagedReader<SchemaNegotiator> next() {
      return new DruidBatchRecordReader(subScan,
        subScanSpecIterator.next(),
        subScan.getColumns(),
        subScan.getMaxRecordsToRead(),
        context,
        subScan.getStorageEngine());
    }
  }
}
