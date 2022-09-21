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

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.ops.ExecutorFragmentContext;
import org.apache.drill.exec.physical.impl.BatchCreator;
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


  @Override
  public CloseableRecordBatch getBatch(ExecutorFragmentContext context,
                                       DruidSubScan subScan,
                                       List<RecordBatch> children) throws ExecutionSetupException {
    try {
      ScanFrameworkBuilder builder = createBuilder(context.getOptions(), subScan);
      return builder.buildScanOperator(context, subScan);
    } catch (UserException e) {
      throw e;
    } catch (Throwable e) {
      throw e;
    }
  }

  private ScanFrameworkBuilder createBuilder(OptionManager options, DruidSubScan subScan) {
    ScanFrameworkBuilder builder = new ScanFrameworkBuilder();
    builder.projection(subScan.getColumns());
    builder.providedSchema(subScan.getSchema());
    builder.setUserName(subScan.getUserName());

    ReaderFactory readerFactory = new DruidReaderFactory(subScan);
    builder.setReaderFactory(readerFactory);
    builder.nullType(Types.optional(MinorType.VARCHAR));
    return builder;
  }

  private static class DruidReaderFactory implements ReaderFactory {
    private final DruidSubScan subScan;

    private final Iterator<DruidSubScanSpec> scanSpecIterator;

    public DruidReaderFactory(DruidSubScan subScan) {
      this.subScan = subScan;
      this.scanSpecIterator = subScan.getScanSpec().listIterator();
    }

    @Override
    public void bind(ManagedScanFramework framework) {

    }

    @Override
    public ManagedReader<? extends SchemaNegotiator> next() {
      if (scanSpecIterator.hasNext()) {
        DruidSubScanSpec scanSpec = scanSpecIterator.next();
        return new DruidBatchRecordReader(subScan, scanSpec, subScan.getColumns(), subScan.getMaxRecordsToRead(), subScan.getStorageEngine());
      }
      return null;
    }
  }
}
