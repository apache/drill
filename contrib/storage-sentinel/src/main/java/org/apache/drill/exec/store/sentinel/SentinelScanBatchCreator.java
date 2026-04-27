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

package org.apache.drill.exec.store.sentinel;

import org.apache.drill.common.exceptions.ChildErrorContext;
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
import org.apache.drill.exec.store.sentinel.auth.SentinelTokenManager;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class SentinelScanBatchCreator implements BatchCreator<SentinelSubScan> {
  private static final Logger logger = LoggerFactory.getLogger(SentinelScanBatchCreator.class);

  @Override
  public CloseableRecordBatch getBatch(ExecutorFragmentContext context,
                                       SentinelSubScan subScan,
                                       List<RecordBatch> children) throws ExecutionSetupException {
    Preconditions.checkArgument(children.isEmpty());
    try {
      ScanFrameworkBuilder builder = createBuilder(context, subScan);
      return builder.buildScanOperator(context, subScan);
    } catch (UserException e) {
      throw e;
    } catch (Throwable e) {
      throw new ExecutionSetupException(e);
    }
  }

  private ScanFrameworkBuilder createBuilder(ExecutorFragmentContext context, SentinelSubScan subScan) {
    ScanFrameworkBuilder builder = new ScanFrameworkBuilder();
    builder.projection(subScan.getColumns());
    builder.setUserName(subScan.getUserName());

    builder.errorContext(
        new ChildErrorContext(builder.errorContext()) {
          @Override
          public void addContext(UserException.Builder builder) {
            builder.addContext("Plugin", "sentinel");
            builder.addContext("Table", subScan.getScanSpec().getTableName());
          }
        });

    ReaderFactory readerFactory = new SentinelReaderFactory(context, subScan);
    builder.setReaderFactory(readerFactory);
    builder.nullType(Types.optional(MinorType.VARCHAR));
    return builder;
  }

  private static class SentinelReaderFactory implements ReaderFactory {
    private final ExecutorFragmentContext context;
    private final SentinelSubScan subScan;
    private int count;

    public SentinelReaderFactory(ExecutorFragmentContext context, SentinelSubScan subScan) {
      this.context = context;
      this.subScan = subScan;
    }

    @Override
    public void bind(ManagedScanFramework framework) {
      // Binding hook - tokenManager will be created per-batch
    }

    @Override
    public ManagedReader<SchemaNegotiator> next() {
      if (count++ > 0) {
        return null;
      }
      SentinelStoragePluginConfig config = subScan.getConfig();
      SentinelScanSpec scanSpec = subScan.getScanSpec();
      String username = context.getQueryUserName();

      SentinelTokenManager tokenManager = new SentinelTokenManager(
          config.getTenantId(),
          config.getClientId(),
          config.getClientSecret(),
          config.getAuthMode(),
          config.getCredentialsProvider(),
          config.getTokenEndpoint());

      return new SentinelBatchReader(config, scanSpec, tokenManager, username);
    }
  }
}
