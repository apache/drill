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
package org.apache.drill.exec.store.http;

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
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;

import java.util.List;

public class HttpScanBatchCreator implements BatchCreator<HttpSubScan> {

  @Override
  public CloseableRecordBatch getBatch(ExecutorFragmentContext context, HttpSubScan subScan, List<RecordBatch> children) throws ExecutionSetupException {
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

  private ScanFrameworkBuilder createBuilder(OptionManager options,
      HttpSubScan subScan) {
    ScanFrameworkBuilder builder = new ScanFrameworkBuilder();
    builder.projection(subScan.columns());
    builder.setUserName(subScan.getUserName());

    // Provide custom error context
    builder.errorContext(
        new ChildErrorContext(builder.errorContext()) {
          @Override
          public void addContext(UserException.Builder builder) {
            builder.addContext("Connection", subScan.tableSpec().connection());
            builder.addContext("Plugin", subScan.tableSpec().pluginName());
          }
        });

    // Reader
    ReaderFactory readerFactory = new HttpReaderFactory(subScan);
    builder.setReaderFactory(readerFactory);
    builder.nullType(Types.optional(MinorType.VARCHAR));
    return builder;
  }

  private static class HttpReaderFactory implements ReaderFactory {

    private final HttpSubScan subScan;
    private int count;

    public HttpReaderFactory(HttpSubScan subScan) {
      this.subScan = subScan;
    }

    @Override
    public void bind(ManagedScanFramework framework) { }

    @Override
    public ManagedReader<SchemaNegotiator> next() {

      // Get the expected input type
      String inputType = subScan.tableSpec().connectionConfig().inputType();

      // Only a single scan (in a single thread)
      if (count++ == 0) {
        if (inputType.equalsIgnoreCase("csv")) {
          return new HttpCSVBatchReader(subScan);
        } else {
          return new HttpBatchReader(subScan);
        }
      }
      return null;
    }
  }
}
