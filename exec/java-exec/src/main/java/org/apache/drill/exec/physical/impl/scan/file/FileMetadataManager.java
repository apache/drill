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

/**
 * Manages the insertion of file metadata (AKA "implicit" and partition) columns.
 * Parses the file metadata columns from the projection list. Creates and loads
 * the vectors that hold the data. If running in legacy mode, inserts partition
 * columns when the query contains a wildcard. Supports renaming the columns via
 * session options.
 * <p>
 * The lifecycle is that the manager is given the set of files for this scan
 * operator so it can determine the partition depth. (Note that different scans
 * may not agree on the depth. This is a known issue with Drill's implementation.)
 * <p>
 * Then, at the start of the scan, all projection columns are parsed. This class
 * picks out the file metadata columns.
 * <p>
 * On each file (on each reader), the columns are "resolved." Here, that means
 * that the columns are filled in with actual values based on the present file.
 * <p>
 * This is the successor to {@link ColumnExplorer}.
 */

public class FileMetadataManager implements MetadataManager, SchemaProjectionResolver, VectorSource {

  /**
   * Automatically compute partition depth from files. Use only
   * for testing!
   */

  public static final int AUTO_PARTITION_DEPTH = -1;

  // Input

  private final Path scanRootDir;
  private final int partitionCount;
  private FileMetadata currentFile;

  // Config

  protected final String partitionDesignator;
  protected final List<FileMetadataColumnDefn> implicitColDefns = new ArrayList<>();
  protected final Map<String, FileMetadataColumnDefn> fileMetadataColIndex = CaseInsensitiveMap.newHashMap();

  /**
   * Indicates whether to expand partition columns when the query contains a wildcard.
   * Supports queries such as the following:<code><pre>
   * select * from dfs.`partitioned-dir`</pre></code>
   * In which the output columns will be (columns, dir0) if the partitioned directory
   * has one level of nesting.
   *
   * See {@link TestImplicitFileColumns#testImplicitColumns}
   */
  protected final boolean useLegacyWildcardExpansion;

  /**
   * In legacy mode, above, Drill expands partition columns whenever the
   * wildcard appears. Drill 1.1 - 1.11 put expanded partition columns after
   * data columns. This is actually a better position as it minimizes changes
   * the row layout for files at different depths. Drill 1.12 moved them before
   * data columns: at the location of the wildcard.
   * <p>
   * This flag, when set, uses the Drill 1.12 position. Later enhancements
   * can unset this flag to go back to the future: use the preferred location
   * after other columns.
   */
  protected final boolean useLegacyExpansionLocation;
  private final FileMetadataColumnsParser parser;

  // Internal state

  private ResultVectorCache vectorCache;
  private final List<MetadataColumn> metadataColumns = new ArrayList<>();
  private ConstantColumnLoader loader;
  private VectorContainer outputContainer;

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
      boolean useLegacyWildcardExpansion,
      boolean useLegacyExpansionLocation,
      Path rootDir,
      int partitionCount,
      List<Path> files) {
    this.useLegacyWildcardExpansion = useLegacyWildcardExpansion;
    this.useLegacyExpansionLocation = useLegacyExpansionLocation;

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

    if (rootDir == null || files == null) {
      scanRootDir = null;
      this.partitionCount = 0;

    // Special case in which the file is the same as the
    // root directory (occurs for a query with only one file.)

    } else if (files.size() == 1 && rootDir.equals(files.get(0))) {
      scanRootDir = null;
      this.partitionCount = 0;
    } else {
      scanRootDir = rootDir;

      // Compute the partitions. Normally the count is passed in.
      // But, handle the case where the count is unknown. Note: use this
      // convenience only in testing since, in production, it can result
      // in different scans reporting different numbers of partitions.

      if (partitionCount == -1) {
        this.partitionCount = computeMaxPartition(files);
      } else {
        this.partitionCount = partitionCount;
      }
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

  /**
   * Returns the file metadata column parser that:
   * <ul>
   * <li>Picks out the file metadata and partition columns,</li>
   * <li>Inserts partition columns for a wildcard query, if the
   * option to do so is set.</li>
   * </ul>
   *
   * @see {@link #useLegacyWildcardExpansion}
   */

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

  /**
   * Resolves metadata columns to concrete, materialized columns with the
   * proper value for the present file.
   */
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
