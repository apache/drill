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
package org.apache.drill.exec.util;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.drill.common.util.DrillStringUtils;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.record.VectorWrapper;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

public class VectorUtil {

  public static final int DEFAULT_COLUMN_WIDTH = 15;

  public static void showVectorAccessibleContent(VectorAccessible va, final String delimiter) {

    int rows = va.getRecordCount();
    System.out.println(rows + " row(s):");
    List<String> columns = Lists.newArrayList();
    for (VectorWrapper<?> vw : va) {
      columns.add(vw.getValueVector().getField().getPath());
    }

    int width = columns.size();
    for (String column : columns) {
      System.out.printf("%s%s",column, column == columns.get(width - 1) ? "\n" : delimiter);
    }
    for (int row = 0; row < rows; row++) {
      int columnCounter = 0;
      for (VectorWrapper<?> vw : va) {
        boolean lastColumn = columnCounter == width - 1;
        Object o ;
        try{
          o = vw.getValueVector().getAccessor().getObject(row);
        }catch(Exception e){
          throw new RuntimeException("failure while trying to read column " + vw.getField().getPath());
        }
        if (o == null) {
          //null value
          String value = "null";
          System.out.printf("%s%s", value, lastColumn ? "\n" : delimiter);
        }
        else if (o instanceof byte[]) {
          String value = new String((byte[]) o);
          System.out.printf("%s%s", value, lastColumn ? "\n" : delimiter);
        } else {
          String value = o.toString();
          System.out.printf("%s%s", value, lastColumn ? "\n" : delimiter);
        }
        columnCounter++;
      }
    }

    for (VectorWrapper<?> vw : va) {
      vw.clear();
    }
  }

  public static void appendVectorAccessibleContent(VectorAccessible va, StringBuilder formattedResults,
      final String delimiter, boolean includeHeader) {
    if (includeHeader) {
      List<String> columns = Lists.newArrayList();
      for (VectorWrapper<?> vw : va) {
        columns.add(vw.getValueVector().getField().getPath());
      }

      formattedResults.append(Joiner.on(delimiter).join(columns));
      formattedResults.append("\n");
    }

    int rows = va.getRecordCount();
    for (int row = 0; row < rows; row++) {
      List<String> rowValues = Lists.newArrayList();
      for (VectorWrapper<?> vw : va) {
        Object o = vw.getValueVector().getAccessor().getObject(row);
        if (o == null) {
          rowValues.add("null");
        } else if (o instanceof byte[]) {
          rowValues.add(new String((byte[]) o));
        } else if (o instanceof DateTime) {
          // TODO(DRILL-3882) - remove this once the datetime is not returned in an
          // object needlessly holding a timezone
          rowValues.add(DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS").print((DateTime) o));
        } else {
          rowValues.add(o.toString());
        }
      }
      formattedResults.append(Joiner.on(delimiter).join(rowValues));
      formattedResults.append("\n");
    }

    for (VectorWrapper<?> vw : va) {
      vw.clear();
    }
  }

  public static void showVectorAccessibleContent(VectorAccessible va) {
    showVectorAccessibleContent(va, DEFAULT_COLUMN_WIDTH);
  }

  public static void showVectorAccessibleContent(VectorAccessible va, int columnWidth) {
    showVectorAccessibleContent(va, new int[]{ columnWidth });
  }

  public static void showVectorAccessibleContent(VectorAccessible va, int[] columnWidths) {
    int width = 0;
    int columnIndex = 0;
    List<String> columns = Lists.newArrayList();
    List<String> formats = Lists.newArrayList();
    for (VectorWrapper<?> vw : va) {
      int columnWidth = getColumnWidth(columnWidths, columnIndex);
      width += columnWidth + 2;
      formats.add("| %-" + columnWidth + "s");
      MaterializedField field = vw.getValueVector().getField();
      columns.add(field.getPath() + "<" + field.getType().getMinorType() + "(" + field.getType().getMode() + ")" + ">");
      columnIndex++;
    }

    int rows = va.getRecordCount();
    System.out.println(rows + " row(s):");
    for (int row = 0; row < rows; row++) {
      // header, every 50 rows.
      if (row%50 == 0) {
        System.out.println(StringUtils.repeat("-", width + 1));
        columnIndex = 0;
        for (String column : columns) {
          int columnWidth = getColumnWidth(columnWidths, columnIndex);
          System.out.printf(formats.get(columnIndex), column.length() <= columnWidth ? column : column.substring(0, columnWidth - 1));
          columnIndex++;
        }
        System.out.printf("|\n");
        System.out.println(StringUtils.repeat("-", width + 1));
      }
      // column values
      columnIndex = 0;
      for (VectorWrapper<?> vw : va) {
        int columnWidth = getColumnWidth(columnWidths, columnIndex);
        Object o = vw.getValueVector().getAccessor().getObject(row);
        String cellString;
        if (o instanceof byte[]) {
          cellString = DrillStringUtils.toBinaryString((byte[]) o);
        } else {
          cellString = DrillStringUtils.escapeNewLines(String.valueOf(o));
        }
        System.out.printf(formats.get(columnIndex), cellString.length() <= columnWidth ? cellString : cellString.substring(0, columnWidth - 1));
        columnIndex++;
      }
      System.out.printf("|\n");
    }
    if (rows > 0) {
      System.out.println(StringUtils.repeat("-", width + 1));
    }

    for (VectorWrapper<?> vw : va) {
      vw.clear();
    }
  }

  private static int getColumnWidth(int[] columnWidths, int columnIndex) {
    return (columnWidths == null) ? DEFAULT_COLUMN_WIDTH
        : (columnWidths.length > columnIndex) ? columnWidths[columnIndex] : columnWidths[0];
  }

}
