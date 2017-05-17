/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.map.CaseInsensitiveMap;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.server.options.OptionValue;
import org.apache.drill.exec.store.dfs.easy.FileWork;
import org.apache.drill.exec.util.Utilities;
import org.apache.hadoop.fs.Path;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
  public ColumnExplorer(FragmentContext context, List<SchemaPath> columns) {
    this(context.getOptions(), columns);
  }

  /**
   * Helper class that encapsulates logic for sorting out columns
   * between actual table columns, partition columns and implicit file columns.
   * Also populates map with implicit columns names as keys and their values
   */
  public ColumnExplorer(OptionManager optionManager, List<SchemaPath> columns) {
    this.partitionDesignator = optionManager.getOption(ExecConstants.FILESYSTEM_PARTITION_COLUMN_LABEL).string_val;
    this.columns = columns;
    this.isStarQuery = columns != null && Utilities.isStarQuery(columns);
    this.selectedPartitionColumns = Lists.newArrayList();
    this.tableColumns = Lists.newArrayList();
    this.allImplicitColumns = initImplicitFileColumns(optionManager);
    this.selectedImplicitColumns = CaseInsensitiveMap.newHashMap();

    init();
  }

  /**
   * Creates case insensitive map with implicit file columns as keys and appropriate ImplicitFileColumns enum as values
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
   * Checks if given column is partition or not.
   *
   * @param optionManager options
   * @param column column
   * @return true if given column is partition, false otherwise
   */
  public static boolean isPartitionColumn(OptionManager optionManager, SchemaPath column){
    String partitionDesignator = optionManager.getOption(ExecConstants.FILESYSTEM_PARTITION_COLUMN_LABEL).string_val;
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
   * Compares selection root and actual file path to determine partition columns values.
   * Adds implicit file columns according to columns list.
   *
   * @return map with columns names as keys and their values
   */
  public Map<String, String> populateImplicitColumns(FileWork work, String selectionRoot) {
    return populateImplicitColumns(work.getPath(), selectionRoot);
  }

  /**
   * Compares selection root and actual file path to determine partition columns values.
   * Adds implicit file columns according to columns list.
   *
   * @return map with columns names as keys and their values
   */
  public Map<String, String> populateImplicitColumns(String filePath, String selectionRoot) {
    Map<String, String> implicitValues = Maps.newLinkedHashMap();
    if (selectionRoot != null) {
      String[] r = Path.getPathWithoutSchemeAndAuthority(new Path(selectionRoot)).toString().split("/");
      Path path = Path.getPathWithoutSchemeAndAuthority(new Path(filePath));
      String[] p = path.toString().split("/");
      if (p.length > r.length) {
        String[] q = ArrayUtils.subarray(p, r.length, p.length - 1);
        for (int a = 0; a < q.length; a++) {
          if (isStarQuery || selectedPartitionColumns.contains(a)) {
            implicitValues.put(partitionDesignator + a, q[a]);
          }
        }
      }
      //add implicit file columns
      for (Map.Entry<String, ImplicitFileColumns> entry : selectedImplicitColumns.entrySet()) {
        implicitValues.put(entry.getKey(), entry.getValue().getValue(path));
      }
    }
    return implicitValues;
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
   */
  private void init() {
    if (isStarQuery) {
      selectedImplicitColumns.putAll(allImplicitColumns);
    } else {
      for (SchemaPath column : columns) {
        String path = column.getRootSegmentPath();
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

    /**
     * Using file path calculates value for each implicit file column
     */
    public abstract String getValue(Path path);

  }

}
