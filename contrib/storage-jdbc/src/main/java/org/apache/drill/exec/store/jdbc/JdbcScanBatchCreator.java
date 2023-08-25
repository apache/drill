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
package org.apache.drill.exec.store.jdbc;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.ops.ExecutorFragmentContext;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.physical.impl.scan.framework.BasicScanFactory;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedScanFramework;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedScanFramework.ScanFrameworkBuilder;
import org.apache.drill.exec.proto.UserBitShared.UserCredentials;
import org.apache.drill.exec.physical.impl.scan.framework.SchemaNegotiator;
import org.apache.drill.exec.record.CloseableRecordBatch;
import org.apache.drill.exec.record.RecordBatch;
import com.google.common.base.Preconditions;

import java.util.Collections;
import java.util.List;

import javax.sql.DataSource;

public class JdbcScanBatchCreator implements BatchCreator<JdbcSubScan> {

  @Override
  public CloseableRecordBatch getBatch(ExecutorFragmentContext context,
                                       JdbcSubScan subScan, List<RecordBatch> children) throws ExecutionSetupException {
    Preconditions.checkArgument(children.isEmpty());

    try {
      ScanFrameworkBuilder builder = createBuilder(context, subScan);
      return builder.buildScanOperator(context, subScan);
    } catch (UserException e) {
      // Rethrow user exceptions directly
      throw e;
    } catch (Throwable e) {
      // Wrap all others
      throw new ExecutionSetupException(e);
    }
  }

  private ScanFrameworkBuilder createBuilder(ExecutorFragmentContext context, JdbcSubScan subScan) {
    ScanFrameworkBuilder builder = new ScanFrameworkBuilder();
    builder.projection(subScan.getColumns());
    builder.setUserName(subScan.getUserName());
    JdbcStoragePlugin plugin = subScan.getPlugin();
    UserCredentials userCreds = context.getContextInformation().getQueryUserCredentials();
    DataSource ds = plugin.getDataSource(userCreds)
      .orElseThrow(() -> UserException.permissionError().message(
        "Query user %s could not obtain a connection to %s, missing credentials?",
        userCreds.getUserName(),
        plugin.getName()
      ).build(JdbcStoragePlugin.logger));

    List<ManagedReader<SchemaNegotiator>> readers = Collections.singletonList(
      new JdbcBatchReader(ds, subScan.getSql(), subScan.getColumns())
    );

    ManagedScanFramework.ReaderFactory readerFactory = new BasicScanFactory(readers.iterator());
    builder.setReaderFactory(readerFactory);
    builder.nullType(Types.optional(MinorType.VARCHAR));
    return builder;
  }
}
