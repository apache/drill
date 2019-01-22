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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.directory.api.util.Strings;
import org.apache.drill.common.map.CaseInsensitiveMap;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.physical.impl.scan.project.ColumnProjection;
import org.apache.drill.exec.physical.impl.scan.project.ConstantColumnLoader;
import org.apache.drill.exec.physical.impl.scan.project.MetadataManager;
import org.apache.drill.exec.physical.impl.scan.project.ResolvedTuple;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection.ScanProjectionParser;
import org.apache.drill.exec.physical.impl.scan.project.SchemaLevelProjection.SchemaProjectionResolver;
import org.apache.drill.exec.physical.impl.scan.project.VectorSource;
import org.apache.drill.exec.physical.rowSet.ResultVectorCache;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.server.options.OptionSet;
import org.apache.drill.exec.store.ColumnExplorer.ImplicitFileColumns;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.hadoop.fs.Path;

import org.apache.drill.shaded.guava.com.google.common.annotations.VisibleForTesting;

public class FileMetadataManager implements MetadataManager, SchemaProjectionResolver, VectorSource {

  // Input

  private Path scanRootDir;
  private FileMetadata currentFile;

  // Config

  protected final String partitionDesignator;
  protected List<FileMetadataColumnDefn> implicitColDefns = new ArrayList<>();
  protected Map<String, FileMetadataColumnDefn> fileMetadataColIndex = CaseInsensitiveMap.newHashMap();
  private final FileMetadataColumnsParser parser;

  // Internal state

  private ResultVectorCache vectorCache;
  private final List<MetadataColumn> metadataColumns = new ArrayList<>();
  private ConstantColumnLoader loader;
  private VectorContainer outputContainer;
  private final int partitionCount;

  /**
   * Specifies whether to plan based on the legacy meaning of "*". See
   * <a href="https://issues.apache.org/jira/browse/DRILL-5542">DRILL-5542</a>.
   * If true, then the star column <i>includes</i> implicit and partition
   * columns. If false, then star matches <i>only</i> table columns.
   *
   * @param optionManager access to the options for this query; used
   * too look up custom names for the metadata columns
   * @param useLegacyWildcardExpansion true to use the legacy plan, false to use the revised
   * semantics
   * @param rootDir when scanning multiple files, the root directory for
   * the file set. Unfortunately, the planner is ambiguous on this one; if the
   * query is against a single file, then this variable holds the name of that
   * one file, rather than a directory
   * @param files the set of files to scan. Used to compute the maximum partition
   * depth across all readers in this fragment
   *
   * @return this builder
   */

  public FileMetadataManager(OptionSet optionManager,
      Path rootDir, List<Path> files) {
    scanRootDir = rootDir;

    partitionDesignator = optionManager.getString(ExecConstants.FILESYSTEM_PARTITION_COLUMN_LABEL);
    for (ImplicitFileColumns e : ImplicitFileColumns.values()) {
      String colName = optionManager.getString(e.optionName());
      if (! Strings.isEmpty(colName)) {
        FileMetadataColumnDefn defn = new FileMetadataColumnDefn(colName, e);
        implicitColDefns.add(defn);
        fileMetadataColIndex.put(defn.colName, defn);
      }
    }
    parser = new FileMetadataColumnsParser(this);

    // The files and root dir are optional.

    if (scanRootDir == null || files == null) {
      partitionCount = 0;

    // Special case in which the file is the same as the
    // root directory (occurs for a query with only one file.)

    } else if (files.size() == 1 && scanRootDir.equals(files.get(0))) {
      scanRootDir = null;
      partitionCount = 0;
    } else {

      // Compute the partitions.

      partitionCount = computeMaxPartition(files);
    }
  }

  private int computeMaxPartition(List<Path> files) {
    int maxLen = 0;
    for (Path filePath : files) {
      FileMetadata info = fileMetadata(filePath);
      maxLen = Math.max(maxLen, info.dirPathLength());
    }
    return maxLen;
  }

  @Override
  public void bind(ResultVectorCache vectorCache) {
    this.vectorCache = vectorCache;
  }

  @Override
  public ScanProjectionParser projectionParser() { return parser; }

  public FileMetadata fileMetadata(Path filePath) {
    return new FileMetadata(filePath, scanRootDir);
  }

  public boolean hasImplicitCols() { return parser.hasImplicitCols(); }

  public String partitionName(int partition) {
    return partitionDesignator + partition;
  }

  public List<FileMetadataColumnDefn> fileMetadataColDefns() { return implicitColDefns; }

  public void startFile(Path filePath) {
    currentFile = fileMetadata(filePath);
  }

  @Override
  public SchemaProjectionResolver resolver() { return this; }

  @Override
  public void define() {
    assert loader == null;
    if (metadataColumns.isEmpty()) {
      return;
    }
    loader = new ConstantColumnLoader(vectorCache, metadataColumns);
  }

  @Override
  public void load(int rowCount) {
    if (loader == null) {
      return;
    }
    outputContainer = loader.load(rowCount);
  }

  @Override
  public void close() {
    metadataColumns.clear();
    if (loader != null) {
      loader.close();
      loader = null;
    }
  }

  @Override
  public void startResolution() {
    close();
  }

  @Override
  public void endFile() {
    currentFile = null;
  }

  @Override
  public boolean resolveColumn(ColumnProjection col, ResolvedTuple tuple,
      TupleMetadata tableSchema) {
    MetadataColumn outputCol = null;
    switch (col.nodeType()) {
    case PartitionColumn.ID:
      outputCol = ((PartitionColumn) col).resolve(currentFile, this, metadataColumns.size());
      break;

    case FileMetadataColumn.ID:
      outputCol = ((FileMetadataColumn) col).resolve(currentFile, this, metadataColumns.size());
      break;

    default:
      return false;
    }

    tuple.add(outputCol);
    metadataColumns.add(outputCol);
    return true;
  }

  @Override
  public ValueVector vector(int index) {
    return outputContainer.getValueVector(index).getValueVector();
  }

  public int partitionCount() { return partitionCount; }

  @VisibleForTesting
  public List<MetadataColumn> metadataColumns() { return metadataColumns; }
}
