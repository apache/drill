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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.impl.scan.RowBatchReader;
import org.apache.drill.exec.physical.impl.scan.framework.AbstractScanFramework;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.framework.SchemaNegotiator;
import org.apache.drill.exec.physical.impl.scan.framework.ShimBatchReader;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.easy.FileWork;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;

/**
 * Scan framework for a file that implements metadata columns (AKA "implicit"
 * columns and partition columns.)
 * <p>
 * Framework iterators over file descriptions, creating readers at the
 * moment they are needed. This allows simpler logic because, at the point of
 * reader creation, we have a file system, context and so on.
 */

public abstract class BaseFileScanFramework<T extends BaseFileScanFramework.FileSchemaNegotiator>
    extends AbstractScanFramework<T> {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BaseFileScanFramework.class);

  /**
   * The file schema negotiator adds no behavior at present, but is
   * created as a placeholder anticipating the need for file-specific
   * behavior later. Readers are expected to use an instance of this
   * class so that their code need not change later if/when we add new
   * methods. For example, perhaps we want to specify an assumed block
   * size for S3 files, or want to specify behavior if the file no longer
   * exists. Those are out of scope of this first round of changes which
   * focus on schema.
   */

  public interface FileSchemaNegotiator extends SchemaNegotiator {
  }

  private final List<? extends FileWork> files;
  private final Configuration fsConfig;
  private List<FileSplit> spilts = new ArrayList<>();
  private Iterator<FileSplit> splitIter;
  private Path scanRootDir;
  private int partitionDepth;
  protected DrillFileSystem dfs;
  private FileMetadataManager metadataManager;

  public BaseFileScanFramework(List<SchemaPath> projection,
      List<? extends FileWork> files,
      Configuration fsConf) {
    super(projection);
    this.files = files;
    this.fsConfig = fsConf;
  }

  /**
   * Specify the selection root for a directory scan, if any.
   * Used to populate partition columns. Also, specify the maximum
   * partition depth.
   *
   * @param rootPath Hadoop file path for the directory
   * @param partitionDepth maximum partition depth across all files
   * within this logical scan operator (files in this scan may be
   * shallower)
   */

  public void setSelectionRoot(Path rootPath, int partitionDepth) {
    this.scanRootDir = rootPath;
    this.partitionDepth = partitionDepth;
  }

  @Override
  protected void configure() {
    super.configure();

    // Create the Drill file system.

    try {
      dfs = context.newFileSystem(fsConfig);
    } catch (IOException e) {
      throw UserException.dataReadError(e)
        .addContext("Failed to create FileSystem")
        .build(logger);
    }

    // Prepare the list of files. We need the list of paths up
    // front to compute the maximum partition. Then, we need to
    // iterate over the splits to create readers on demand.

    List<Path> paths = new ArrayList<>();
    for (FileWork work : files) {
      Path path = dfs.makeQualified(work.getPath());
      paths.add(path);
      FileSplit split = new FileSplit(path, work.getStart(), work.getLength(), new String[]{""});
      spilts.add(split);
    }
    splitIter = spilts.iterator();

    // Create the metadata manager to handle file metadata columns
    // (so-called implicit columns and partition columns.)

    metadataManager = new FileMetadataManager(
        context.getFragmentContext().getOptions(),
        true, // Expand partition columns with wildcard
        false, // Put partition columns after table columns
        scanRootDir,
        partitionDepth,
        paths);
    scanOrchestrator.withMetadata(metadataManager);
  }

  @Override
  public RowBatchReader nextReader() {

    // Create a reader on demand for the next split.

    if (! splitIter.hasNext()) {
      return null;
    }
    FileSplit split = splitIter.next();

    // Alert the framework that a new file is starting.

    startFile(split);
    try {

      // Create a per-framework reader wrapped in a standard
      // "shim" reader. Allows app-specific readers to be very focused;
      // the shim handles standard boilerplate.

      return new ShimBatchReader<T>(this, newReader(split));
    } catch (ExecutionSetupException e) {
      throw UserException.executionError(e)
        .addContext("File", split.getPath().toString())
        .build(logger);
    }
  }

  protected abstract ManagedReader<T> newReader(FileSplit split) throws ExecutionSetupException;

  protected void startFile(FileSplit split) {

    // Tell the metadata manager about the current file so it can
    // populate the metadata columns, if requested.

    metadataManager.startFile(split.getPath());
  }
}
