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
package org.apache.drill.exec.store.hive;

import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.drill.shaded.guava.com.google.common.collect.Maps;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.List;
import java.util.Map;

/**
 * The class represents "cache" for partition and table columns.
 * Used to reduce physical plan for Hive tables.
 * Only unique partition lists of columns stored in the column lists cache.
 * Table columns should be stored at index 0.
 */
public class ColumnListsCache {
  // contains immutable column lists
  private final List<List<FieldSchema>> fields;

  // keys of the map are column lists and values are them positions in list fields
  private final Map<List<FieldSchema>, Integer> keys;

  public ColumnListsCache(Table table) {
    this();
    // table columns stored at index 0.
    addOrGet(table.getSd().getCols());
  }

  public ColumnListsCache() {
    this.fields = Lists.newArrayList();
    this.keys = Maps.newHashMap();
  }

  /**
   * Checks if column list has been added before and returns position of column list.
   * If list is unique, adds list to the fields list and returns it position.
   * Returns -1, if {@param columns} equals null.
   *
   * @param columns list of columns
   * @return index of {@param columns} or -1, if {@param columns} equals null
   */
  public int addOrGet(List<FieldSchema> columns) {
    if (columns == null) {
      return -1;
    }
    Integer index = keys.get(columns);
    if (index != null) {
      return index;
    } else {
      index = fields.size();
      final List<FieldSchema> immutableList = ImmutableList.copyOf(columns);
      fields.add(immutableList);
      keys.put(immutableList, index);
      return index;
    }
  }

  /**
   * Returns list of columns at the specified position in fields list,
   * or null if index is negative or greater than fields list size.
   *
   * @param index index of column list to return
   * @return list of columns at the specified position in fields list
   * or null if index is negative or greater than fields list size
   */
  public List<FieldSchema> getColumns(int index) {
    if (index >= 0 && index < fields.size()) {
      return fields.get(index);
    } else {
      return null;
    }
  }

  public List<List<FieldSchema>> getFields() {
    return Lists.newArrayList(fields);
  }
}
