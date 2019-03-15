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
package org.apache.drill.exec.physical.impl.scan.columns;

import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.impl.scan.file.BaseFileScanFramework;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework.FileSchemaNegotiatorImpl;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.framework.ShimBatchReader;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.easy.FileWork;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.FileSplit;

/**
 * Scan framework for a file that supports the special "columns" column.
 * <p>
 * The logic here is a bit tricky. The CSV (really, "compliant text") reader
 * operates in multiple modes. If a header is required, then the columns
 * array is not allowed. If the header is not loaded, then the columns
 * array must be used.
 * <p>
 * This class combines the above semantics with the projection list from the
 * query. It handles things like two appearances of the unadorned columns
 * identifier, the use of the columns identifier when it is not allowed, etc.
 */

public class ColumnsScanFramework extends BaseFileScanFramework<ColumnsSchemaNegotiator> {

  public interface FileReaderCreator {
    ManagedReader<ColumnsSchemaNegotiator> makeBatchReader(
        DrillFileSystem dfs,
        FileSplit split) throws ExecutionSetupException;
  }

  /**
   * Implementation of the columns array schema negotiator.
   */

  public static class ColumnsSchemaNegotiatorImpl extends FileSchemaNegotiatorImpl
          implements ColumnsSchemaNegotiator {

    public ColumnsSchemaNegotiatorImpl(ColumnsScanFramework framework,
        ShimBatchReader<ColumnsSchemaNegotiator> shim) {
      super(framework, shim);
    }

    private ColumnsScanFramework framework() {
      return (ColumnsScanFramework) basicFramework;
    }

    @Override
    public boolean columnsArrayProjected() {
      return framework().columnsArrayManager.hasColumnsArrayColumn();
    }

    @Override
    public boolean[] projectedIndexes() {
      return framework().columnsArrayManager.elementProjection();
    }
  }

  /**
   * Creates (or iterates over) the readers for this scan.
   */
  private final FileReaderCreator readerCreator;
  private boolean requireColumnsArray;
  protected ColumnsArrayManager columnsArrayManager;

  public ColumnsScanFramework(List<SchemaPath> projection,
      List<? extends FileWork> files,
      Configuration fsConf,
      FileReaderCreator readerCreator) {
    super(projection, files, fsConf);
    this.readerCreator = readerCreator;
  }

  public void requireColumnsArray(boolean flag) {
    requireColumnsArray = flag;
  }

  @Override
  protected void configure() {
    super.configure();
    columnsArrayManager = new ColumnsArrayManager(requireColumnsArray);
    scanOrchestrator.addParser(columnsArrayManager.projectionParser());
    scanOrchestrator.addResolver(columnsArrayManager.resolver());

    // This framework is (at present) used only for the text readers
    // which use required Varchar columns to represent null columns.

    scanOrchestrator.allowRequiredNullColumns(true);
  }

  @Override
  protected ManagedReader<ColumnsSchemaNegotiator> newReader(FileSplit split) throws ExecutionSetupException {
    return readerCreator.makeBatchReader(dfs, split);
  }

  @Override
  public boolean openReader(ShimBatchReader<ColumnsSchemaNegotiator> shim, ManagedReader<ColumnsSchemaNegotiator> reader) {
    return reader.open(
        new ColumnsSchemaNegotiatorImpl(this, shim));
  }
}
