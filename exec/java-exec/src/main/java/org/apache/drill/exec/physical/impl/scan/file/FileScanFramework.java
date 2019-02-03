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
package org.apache.drill.exec.physical.impl.scan.file;

import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.impl.scan.file.BaseFileScanFramework.FileSchemaNegotiator;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.framework.SchemaNegotiatorImpl;
import org.apache.drill.exec.physical.impl.scan.framework.ShimBatchReader;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.easy.FileWork;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.FileSplit;

/**
 * The file scan framework adds into the scan framework support for implicit
 * file metadata columns.
 */

public class FileScanFramework extends BaseFileScanFramework<FileSchemaNegotiator> {

  public interface FileReaderCreator {
    ManagedReader<FileSchemaNegotiator> makeBatchReader(
        DrillFileSystem dfs,
        FileSplit split) throws ExecutionSetupException;
  }

  /**
   * Implementation of the file-level schema negotiator.
   */

  public static class FileSchemaNegotiatorImpl extends SchemaNegotiatorImpl
      implements FileSchemaNegotiator {

    public FileSchemaNegotiatorImpl(BaseFileScanFramework<?> framework, ShimBatchReader<? extends FileSchemaNegotiator> shim) {
      super(framework, shim);
    }
  }

  private final FileReaderCreator readerCreator;

  public FileScanFramework(List<SchemaPath> projection,
      List<? extends FileWork> files,
      Configuration fsConf,
      FileReaderCreator readerCreator) {
    super(projection, files, fsConf);
    this.readerCreator = readerCreator;
  }

  @Override
  protected ManagedReader<FileSchemaNegotiator> newReader(FileSplit split) throws ExecutionSetupException {
    return readerCreator.makeBatchReader(dfs, split);
  }

  @Override
  public boolean openReader(ShimBatchReader<FileSchemaNegotiator> shim, ManagedReader<FileSchemaNegotiator> reader) {
    return reader.open(
        new FileSchemaNegotiatorImpl(this, shim));
  }
}
