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

package org.apache.drill.test.rowSet.file;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.accessor.ColumnAccessor;
import org.apache.drill.exec.vector.accessor.ColumnReader;
import org.apache.drill.test.rowSet.RowSet;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class JsonFileBuilder
{
  public static final String DEFAULT_DOUBLE_FORMATTER = "%f";
  public static final String DEFAULT_INTEGER_FORMATTER = "%d";
  public static final String DEFAULT_LONG_FORMATTER = "%d";
  public static final String DEFAULT_STRING_FORMATTER = "\"%s\"";
  public static final String DEFAULT_DECIMAL_FORMATTER = "%s";
  public static final String DEFAULT_PERIOD_FORMATTER = "%s";

  public static final Map<String, String> DEFAULT_FORMATTERS = new ImmutableMap.Builder()
    .put(ColumnAccessor.ValueType.DOUBLE, DEFAULT_DOUBLE_FORMATTER)
    .put(ColumnAccessor.ValueType.INTEGER, DEFAULT_INTEGER_FORMATTER)
    .put(ColumnAccessor.ValueType.LONG, DEFAULT_LONG_FORMATTER)
    .put(ColumnAccessor.ValueType.STRING, DEFAULT_STRING_FORMATTER)
    .put(ColumnAccessor.ValueType.DECIMAL, DEFAULT_DECIMAL_FORMATTER)
    .put(ColumnAccessor.ValueType.PERIOD, DEFAULT_PERIOD_FORMATTER)
    .build();

  private final RowSet rowSet;
  private final Map<String, String> customFormatters = Maps.newHashMap();

  public JsonFileBuilder(RowSet rowSet) {
    this.rowSet = Preconditions.checkNotNull(rowSet);
    Preconditions.checkArgument(rowSet.rowCount() > 0, "The given rowset is empty.");
  }

  public JsonFileBuilder setCustomFormatter(final String columnName, final String columnFormatter) {
    Preconditions.checkNotNull(columnName);
    Preconditions.checkNotNull(columnFormatter);

    Iterator<MaterializedField> fields = rowSet
      .schema()
      .batch()
      .iterator();

    boolean hasColumn = false;

    while (!hasColumn && fields.hasNext()) {
      hasColumn = fields.next()
        .getName()
        .equals(columnName);
    }

    final String message = String.format("(%s) is not a valid column", columnName);
    Preconditions.checkArgument(hasColumn, message);

    customFormatters.put(columnName, columnFormatter);

    return this;
  }

  public void build(File tableFile) throws IOException {
    tableFile.getParentFile().mkdirs();

    try (BufferedOutputStream os = new BufferedOutputStream(new FileOutputStream(tableFile))) {
      final RowSet.RowSetReader reader = rowSet.reader();
      final int numCols = rowSet
        .schema()
        .batch()
        .getFieldCount();
      final Iterator<MaterializedField> fieldIterator = rowSet
        .schema()
        .batch()
        .iterator();
      final List<String> columnNames = Lists.newArrayList();
      final List<String> columnFormatters = Lists.newArrayList();

      // Build formatters from first row.
      while (fieldIterator.hasNext()) {
        final String columnName = fieldIterator.next().getName();
        final ColumnReader columnReader = reader.column(columnName);
        final ColumnAccessor.ValueType valueType = columnReader.valueType();
        final String columnFormatter;

        if (customFormatters.containsKey(columnName)) {
          columnFormatter = customFormatters.get(columnName);
        } else if (DEFAULT_FORMATTERS.containsKey(valueType)) {
          columnFormatter = DEFAULT_FORMATTERS.get(valueType);
        } else {
          final String message = String.format("Unsupported column type %s", valueType);
          throw new UnsupportedOperationException(message);
        }

        columnNames.add(columnName);
        columnFormatters.add(columnFormatter);
      }

      final StringBuilder sb = new StringBuilder();
      String lineSeparator = "";

      for (int index = 0; index < rowSet.rowCount(); index++) {
        reader.next();
        sb.append(lineSeparator);
        sb.append('{');
        String separator = "";

        for (int columnIndex = 0; columnIndex < numCols; columnIndex++) {
          sb.append(separator);

          final String columnName = columnNames.get(columnIndex);
          final ColumnReader columnReader = reader.column(columnIndex);
          final String columnFormatter = columnFormatters.get(columnIndex);
          final Object columnObject = columnReader.getObject();
          final String columnString = String.format(columnFormatter, columnObject);

          sb.append('"')
            .append(columnName)
            .append('"')
            .append(':')
            .append(columnString);

          separator = ",";
        }

        sb.append('}');
        lineSeparator = "\n";
        os.write(sb.toString().getBytes());
        sb.delete(0, sb.length());
      }
    }
  }
}
