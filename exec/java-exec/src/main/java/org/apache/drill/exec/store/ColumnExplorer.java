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
package org.apache.drill.exec.store;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.map.CaseInsensitiveMap;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.server.options.OptionValue;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.util.Utilities;
import org.apache.hadoop.fs.Path;

import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.drill.shaded.guava.com.google.common.io.Files;

public class ColumnExplorer {

  private final String partitionDesignator;
  private final List<SchemaPath> columns;
  private final boolean isStarQuery;
  private final List<Integer> selectedPartitionColumns;
  private final List<SchemaPath> tableColumns;
  private final Map<String, ImplicitFileColumns> allImplicitColumns;
  private final Map<String, ImplicitFileColumns> selectedImplicitColumns;

  /**
   * Helper class that encapsulates logic for sorting out columns
   * between actual table columns, partition columns and implicit file columns.
   * Also populates map with implicit columns names as keys and their values
   */
  public ColumnExplorer(OptionManager optionManager, List<SchemaPath> columns) {
    this.partitionDesignator = optionManager.getString(ExecConstants.FILESYSTEM_PARTITION_COLUMN_LABEL);
    this.columns = columns;
    this.isStarQuery = columns != null && Utilities.isStarQuery(columns);
    this.selectedPartitionColumns = Lists.newArrayList();
    this.tableColumns = Lists.newArrayList();
    this.allImplicitColumns = initImplicitFileColumns(optionManager);
    this.selectedImplicitColumns = CaseInsensitiveMap.newHashMap();

    init();
  }

  /**
   * Creates case insensitive map with implicit file columns as keys and
   * appropriate ImplicitFileColumns enum as values
   */
  public static Map<String, ImplicitFileColumns> initImplicitFileColumns(OptionManager optionManager) {
    Map<String, ImplicitFileColumns> map = CaseInsensitiveMap.newHashMap();
    for (ImplicitFileColumns e : ImplicitFileColumns.values()) {
      OptionValue optionValue;
      if ((optionValue = optionManager.getOption(e.name)) != null) {
        map.put(optionValue.string_val, e);
      }
    }
    return map;
  }

  /**
   * Returns list with implicit column names taken from specified {@link SchemaConfig}.
   *
   * @param schemaConfig the source of session options values.
   * @return list with implicit column names.
   */
  public static List<String> getImplicitColumnsNames(SchemaConfig schemaConfig) {
    List<String> implicitColumns = Lists.newArrayList();
    for (ImplicitFileColumns e : ImplicitFileColumns.values()) {
      OptionValue optionValue;
      if ((optionValue = schemaConfig.getOption(e.name)) != null) {
        implicitColumns.add(optionValue.string_val);
      }
    }
    return implicitColumns;
  }

  /**
   * Checks if given column is partition or not.
   *
   * @param optionManager options
   * @param column column
   * @return true if given column is partition, false otherwise
   */
  public static boolean isPartitionColumn(OptionManager optionManager, SchemaPath column) {
    String partitionDesignator = optionManager.getString(ExecConstants.FILESYSTEM_PARTITION_COLUMN_LABEL);
    String path = column.getRootSegmentPath();
    return isPartitionColumn(partitionDesignator, path);
  }

  /**
   * Checks if given column is partition or not.
   *
   * @param partitionDesignator partition designator
   * @param path column path
   * @return true if given column is partition, false otherwise
   */
  public static boolean isPartitionColumn(String partitionDesignator, String path){
    Pattern pattern = Pattern.compile(String.format("%s[0-9]+", partitionDesignator));
    Matcher matcher = pattern.matcher(path);
    return matcher.matches();
  }

  /**
   * Returns list with partition column names.
   * For the case when table has several levels of nesting, max level is chosen.
   *
   * @param selection    the source of file paths
   * @param schemaConfig the source of session option value for partition column label
   * @return list with partition column names.
   */
  public static List<String> getPartitionColumnNames(FileSelection selection, SchemaConfig schemaConfig) {
    int partitionsCount = 0;
    // a depth of table root path
    int rootDepth = new Path(selection.getSelectionRoot()).depth();

    for (String file : selection.getFiles()) {
      // Calculates partitions count for the concrete file:
      // depth of file path - depth of table root path - 1.
      // The depth of file path includes file itself,
      // so we should subtract 1 to consider only directories.
      int currentPartitionsCount = new Path(file).depth() - rootDepth - 1;
      // max depth of files path should be used to handle all partitions
      partitionsCount = Math.max(partitionsCount, currentPartitionsCount);
    }

    String partitionColumnLabel = schemaConfig.getOption(ExecConstants.FILESYSTEM_PARTITION_COLUMN_LABEL).string_val;
    List<String> partitions = Lists.newArrayList();

    // generates partition column names: dir0, dir1 etc.
    for (int i = 0; i < partitionsCount; i++) {
      partitions.add(partitionColumnLabel + i);
    }
    return partitions;
  }

  /**
   * Creates map with implicit columns where key is column name, value is columns actual value.
   * This map contains partition and implicit file columns (if requested).
   * Partition columns names are formed based in partition designator and value index.
   *
   * @param filePath file path, used to populate file implicit columns
   * @param partitionValues list of partition values
   * @param includeFileImplicitColumns if file implicit columns should be included into the result
   * @return implicit columns map
   */
  public Map<String, String> populateImplicitColumns(String filePath,
                                                     List<String> partitionValues,
                                                     boolean includeFileImplicitColumns) {
    Map<String, String> implicitValues = new LinkedHashMap<>();

    for (int i = 0; i < partitionValues.size(); i++) {
      if (isStarQuery || selectedPartitionColumns.contains(i)) {
        implicitValues.put(partitionDesignator + i, partitionValues.get(i));
      }
    }

    if (includeFileImplicitColumns) {
      Path path = Path.getPathWithoutSchemeAndAuthority(new Path(filePath));
      for (Map.Entry<String, ImplicitFileColumns> entry : selectedImplicitColumns.entrySet()) {
        implicitValues.put(entry.getKey(), entry.getValue().getValue(path));
      }
    }

    return implicitValues;
  }

  /**
   * Compares root and file path to determine directories
   * that are present in the file path but absent in root.
   * Example: root - a/b/c, filePath - a/b/c/d/e/0_0_0.parquet, result - d/e.
   * Stores different directory names in the list in successive order.
   *
   *
   * @param filePath file path
   * @param root root directory
   * @return list of directory names
   */
  public static List<String> listPartitionValues(String filePath, String root) {
    if (filePath == null || root == null) {
      return Collections.emptyList();
    }

    int rootDepth = new Path(root).depth();
    Path path = new Path(filePath);
    int parentDepth = path.getParent().depth();

    int diffCount = parentDepth - rootDepth;

    if (diffCount < 1) {
      return Collections.emptyList();
    }

    String[] diffDirectoryNames = new String[diffCount];

    // start filling in array from the end
    for (int i = rootDepth; parentDepth > i; i++) {
      path = path.getParent();
      // place in the end of array
      diffDirectoryNames[parentDepth - i - 1] = path.getName();
    }

    return Arrays.asList(diffDirectoryNames);
  }

  public boolean isStarQuery() {
    return isStarQuery;
  }

  public List<SchemaPath> getTableColumns() {
    return tableColumns;
  }

  /**
   * Checks if current column selection contains partition columns.
   *
   * @return true if partition columns are present, false otherwise
   */
  public boolean containsPartitionColumns() {
    return !selectedPartitionColumns.isEmpty();
  }

  /**
   * Checks if current column selection contains implicit columns.
   *
   * @return true if implicit columns are present, false otherwise
   */
  public boolean containsImplicitColumns() {
    return !selectedImplicitColumns.isEmpty();
  }

  /**
   * If it is not star query, sorts out columns into three categories:
   * 1. table columns
   * 2. partition columns
   * 3. implicit file columns
   * If it is a star query, then only includes implicit columns that were
   * explicitly selected (e.g., SELECT *, FILENAME FROM ..)
   */
  private void init() {
    for (SchemaPath column : columns) {
      final String path = column.getRootSegmentPath();
      if (isStarQuery) {
        if (allImplicitColumns.get(path) != null) {
          selectedImplicitColumns.put(path, allImplicitColumns.get(path));
        }
      } else {
        if (isPartitionColumn(partitionDesignator, path)) {
          selectedPartitionColumns.add(Integer.parseInt(path.substring(partitionDesignator.length())));
        } else if (allImplicitColumns.get(path) != null) {
          selectedImplicitColumns.put(path, allImplicitColumns.get(path));
        } else {
          tableColumns.add(column);
        }
      }
    }
  }

  /**
   * Columns that give information from where file data comes from.
   * Columns are implicit, so should be called explicitly in query
   */
  public enum ImplicitFileColumns {

    /**
     * Fully qualified name, contains full path to file and file name
     */
    FQN (ExecConstants.IMPLICIT_FQN_COLUMN_LABEL) {
      @Override
      public String getValue(Path path) {
        return path.toUri().getPath();
      }
    },

    /**
     * Full path to file without file name
     */
    FILEPATH (ExecConstants.IMPLICIT_FILEPATH_COLUMN_LABEL) {
      @Override
      public String getValue(Path path) {
        return path.getParent().toUri().getPath();
      }
    },

    /**
     * File name with extension without path
     */
    FILENAME (ExecConstants.IMPLICIT_FILENAME_COLUMN_LABEL) {
      @Override
      public String getValue(Path path) {
        return path.getName();
      }
    },

    /**
     * File suffix (without dot at the beginning)
     */
    SUFFIX (ExecConstants.IMPLICIT_SUFFIX_COLUMN_LABEL) {
      @Override
      public String getValue(Path path) {
        return Files.getFileExtension(path.getName());
      }
    };

    String name;

    ImplicitFileColumns(String name) {
      this.name = name;
    }

    public String optionName() { return name; }

    /**
     * Using file path calculates value for each implicit file column
     */
    public abstract String getValue(Path path);
  }
}
