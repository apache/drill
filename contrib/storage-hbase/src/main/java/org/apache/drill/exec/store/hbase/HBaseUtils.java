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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.List;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.ParseFilter;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
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
    try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream(); DataOutputStream out = new DataOutputStream(byteStream)) {
      HbaseObjectWritable.writeObject(out, filter, filter.getClass(), null);
      return byteStream.toByteArray();
    } catch (IOException e) {
      throw new DrillRuntimeException("Error serializing filter: " + filter, e);
    }
  }

  public static Filter deserializeFilter(byte[] filterBytes) {
    if (filterBytes == null) {
      return null;
    }
    try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(filterBytes));) {
      return (Filter) HbaseObjectWritable.readObject(dis, null);
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

}
