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


package org.apache.drill.exec.store.ipfs;

import io.ipfs.multihash.Multihash;
import org.apache.drill.common.exceptions.ChildErrorContext;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos;
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
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class IPFSScanBatchCreator implements BatchCreator<IPFSSubScan> {
  private static final Logger logger = LoggerFactory.getLogger(IPFSScanBatchCreator.class);

  @Override
  public CloseableRecordBatch getBatch(ExecutorFragmentContext context, IPFSSubScan subScan, List<RecordBatch> children)
      throws ExecutionSetupException {
    Preconditions.checkArgument(children.isEmpty());
    logger.debug(String.format("subScanSpecList.size = %d", subScan.getIPFSSubScanSpecList().size()));

    try {
      ScanFrameworkBuilder builder = createBuilder(context.getOptions(), subScan);
      return builder.buildScanOperator(context, subScan);
    } catch (Throwable e) {
      // Wrap all others
      throw new ExecutionSetupException(e);
    }
  }

  private ScanFrameworkBuilder createBuilder(OptionManager options, IPFSSubScan subScan) {
    ManagedScanFramework.ScanFrameworkBuilder builder = new ManagedScanFramework.ScanFrameworkBuilder();
    builder.projection(subScan.getColumns());
    builder.setUserName(subScan.getUserName());

    // Provide custom error context
    builder.errorContext(
        new ChildErrorContext(builder.errorContext()) {
          @Override
          public void addContext(UserException.Builder builder) {
            builder.addContext("Plugin", subScan.getIPFSContext().getStoragePlugin().getName());
          }
        });

    // Reader
    ManagedScanFramework.ReaderFactory readerFactory = new IPFSJSONReaderFactory(subScan);
    builder.setReaderFactory(readerFactory);
    builder.nullType(Types.optional(TypeProtos.MinorType.VARCHAR));
    return builder;
  }

  private static class IPFSJSONReaderFactory implements ReaderFactory {

    private final IPFSSubScan subScan;
    private int count;

    public IPFSJSONReaderFactory(IPFSSubScan subScan) {
      this.subScan = subScan;
      this.count = 0;
    }

    @Override
    public void bind(ManagedScanFramework framework) {
    }

    @Override
    public ManagedReader<SchemaNegotiator> next() {

      List<Multihash> scanSpecList = subScan.getIPFSSubScanSpecList();
      if (count < scanSpecList.size()) {
        Multihash block = scanSpecList.get(count);
        count++;
        return new IPFSJSONReader(subScan.getIPFSContext(), block);
      } else {
        return null;
      }
    }
  }
}
