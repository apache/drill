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
package org.apache.drill.exec.store.paimon.read;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.ops.ExecutorFragmentContext;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.physical.impl.scan.v3.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.v3.ReaderFactory;
import org.apache.drill.exec.physical.impl.scan.v3.ScanLifecycleBuilder;
import org.apache.drill.exec.physical.impl.scan.v3.SchemaNegotiator;
import org.apache.drill.exec.record.CloseableRecordBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.store.paimon.PaimonSubScan;
import org.apache.drill.exec.store.paimon.PaimonWork;
import com.google.common.base.Preconditions;

import java.util.Iterator;
import java.util.List;

@SuppressWarnings("unused")
public class PaimonScanBatchCreator implements BatchCreator<PaimonSubScan> {

  @Override
  public CloseableRecordBatch getBatch(ExecutorFragmentContext context,
    PaimonSubScan subScan, List<RecordBatch> children) throws ExecutionSetupException {
    Preconditions.checkArgument(children.isEmpty());

    try {
      ScanLifecycleBuilder builder = createBuilder(subScan);
      return builder.buildScanOperator(context, subScan);
    } catch (UserException e) {
      throw e;
    } catch (Throwable e) {
      throw new ExecutionSetupException(e);
    }
  }

  private ScanLifecycleBuilder createBuilder(PaimonSubScan subScan) {
    ScanLifecycleBuilder builder = new ScanLifecycleBuilder();
    builder.projection(subScan.getColumns());
    builder.userName(subScan.getUserName());
    builder.providedSchema(subScan.getSchema());
    builder.readerFactory(new PaimonReaderFactory(subScan));
    builder.nullType(Types.optional(TypeProtos.MinorType.VARCHAR));
    return builder;
  }

  private static class PaimonReaderFactory implements ReaderFactory<SchemaNegotiator> {
    private final PaimonSubScan subScan;
    private final Iterator<PaimonWork> workIterator;

    private PaimonReaderFactory(PaimonSubScan subScan) {
      this.subScan = subScan;
      this.workIterator = subScan.getWorkList().iterator();
    }

    @Override
    public boolean hasNext() {
      return workIterator.hasNext();
    }

    @Override
    public ManagedReader next(SchemaNegotiator negotiator) {
      return new PaimonRecordReader(subScan.getFormatPlugin(), subScan.getPath(),
        subScan.getColumns(), subScan.getCondition(), workIterator.next(), subScan.getMaxRecords(),
        negotiator);
    }
  }
}
