/**
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
package org.apache.drill.exec.store.hbase;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.util.Utilities;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.ParseFilter;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.FilterProtos;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.Lists;

public class HBaseUtils {
  static final ParseFilter FILTER_PARSEER = new ParseFilter();

  static final int FIRST_FILTER = 0;
  static final int LAST_FILTER = -1;

  public static byte[] getBytes(String str) {
    return str == null ? HConstants.EMPTY_BYTE_ARRAY : Bytes.toBytes(str);
  }

  static Filter parseFilterString(String filterString) {
    if (filterString == null) {
      return null;
    }
    try {
      return FILTER_PARSEER.parseFilterString(filterString);
    } catch (CharacterCodingException e) {
      throw new DrillRuntimeException("Error parsing filter string: " + filterString, e);
    }
  }

  public static byte[] serializeFilter(Filter filter) {
    if (filter == null) {
      return null;
    }
    try {
      FilterProtos.Filter pbFilter = ProtobufUtil.toFilter(filter);
      return pbFilter.toByteArray();
    } catch (IOException e) {
      throw new DrillRuntimeException("Error serializing filter: " + filter, e);
    }
  }

  public static Filter deserializeFilter(byte[] filterBytes) {
    if (filterBytes == null) {
      return null;
    }
    try {
      FilterProtos.Filter pbFilter = FilterProtos.Filter.parseFrom(filterBytes);
      return ProtobufUtil.toFilter(pbFilter);
    } catch (Exception e) {
      throw new DrillRuntimeException("Error deserializing filter: " + filterBytes, e);
    }
  }

  public static Filter andFilterAtIndex(Filter currentFilter, int index, Filter newFilter) {
    if (currentFilter == null) {
      return newFilter;
    } else if (newFilter == null) {
      return currentFilter;
    }

    List<Filter> allFilters = Lists.newArrayList();
    if (currentFilter instanceof FilterList && ((FilterList)currentFilter).getOperator() == FilterList.Operator.MUST_PASS_ALL) {
      allFilters.addAll(((FilterList)currentFilter).getFilters());
    } else {
      allFilters.add(currentFilter);
    }
    allFilters.add((index == LAST_FILTER ? allFilters.size() : index), newFilter);
    return new FilterList(FilterList.Operator.MUST_PASS_ALL, allFilters);
  }

  public static Filter orFilterAtIndex(Filter currentFilter, int index, Filter newFilter) {
    if (currentFilter == null) {
      return newFilter;
    } else if (newFilter == null) {
      return currentFilter;
    }

    List<Filter> allFilters = Lists.newArrayList();
    if (currentFilter instanceof FilterList && ((FilterList)currentFilter).getOperator() == FilterList.Operator.MUST_PASS_ONE) {
      allFilters.addAll(((FilterList)currentFilter).getFilters());
    } else {
      allFilters.add(currentFilter);
    }
    allFilters.add((index == LAST_FILTER ? allFilters.size() : index), newFilter);
    return new FilterList(FilterList.Operator.MUST_PASS_ONE, allFilters);
  }

  public static byte[] maxOfStartRows(byte[] left, byte[] right) {
    if (left == null || left.length == 0 || right == null || right.length == 0) {
      return (left == null || left.length == 0) ? right : left;
    }
    return Bytes.compareTo(left, right) > 0 ? left : right;
  }

  public static byte[] minOfStartRows(byte[] left, byte[] right) {
    if (left == null || left.length == 0 || right == null || right.length == 0) {
      return HConstants.EMPTY_BYTE_ARRAY;
    }
    return Bytes.compareTo(left, right) < 0 ? left : right;
  }

  public static byte[] maxOfStopRows(byte[] left, byte[] right) {
    if (left == null || left.length == 0 || right == null || right.length == 0) {
      return HConstants.EMPTY_BYTE_ARRAY;
    }
    return Bytes.compareTo(left, right) > 0 ? left : right;
  }

  public static byte[] minOfStopRows(byte[] left, byte[] right) {
    if (left == null || left.length == 0 || right == null || right.length == 0) {
      return (left == null || left.length == 0) ? right : left;
    }
    return Bytes.compareTo(left, right) < 0 ? left : right;
  }


  /**
   * Verify column family in schema path exists in the hbase table, or schema path refers to a row_key column.
   * Convert * column into list of column family defined in HTableDescriptor. Return converted list if
   * star column conversion happens.
   *
   * @param columns
   * @param hTableDesc
   * @return
   * @throws DrillRuntimeException if column family does not exist, or is not row_key column.
   */
  public static List<SchemaPath> verifyColumnsAndConvertStar(List<SchemaPath> columns, HTableDescriptor hTableDesc) {
    boolean hasStarCol = false;
    LinkedHashSet<SchemaPath> requestedColumns = new LinkedHashSet<>();

    for (SchemaPath column : columns) {
      // convert * into [row_key, cf1, cf2, ..., cf_n].
      if (column.equals(Utilities.STAR_COLUMN)) {
        hasStarCol = true;
        Set<byte[]> families = hTableDesc.getFamiliesKeys();
        requestedColumns.add(DrillHBaseConstants.ROW_KEY_PATH);
        for (byte[] family : families) {
          SchemaPath colFamily = SchemaPath.getSimplePath(Bytes.toString(family));
          requestedColumns.add(colFamily);
        }
      } else {
        if (!(column.equals(DrillHBaseConstants.ROW_KEY_PATH) ||
            hTableDesc.hasFamily(getBytes(column.getRootSegment().getPath())))) {
          DrillRuntimeException.format("The column family '%s' does not exist in HBase table: %s .",
              column.getRootSegment().getPath(), hTableDesc.getNameAsString());
        }
        requestedColumns.add(column);
      }
    }

    // since star column has been converted, reset.
    if (hasStarCol) {
      return new ArrayList<>(requestedColumns);
    } else {
      return columns;
    }
  }
}
