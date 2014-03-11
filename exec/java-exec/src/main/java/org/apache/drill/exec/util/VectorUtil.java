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

import org.apache.commons.lang.StringUtils;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.rpc.RpcException;

import com.beust.jcommander.internal.Lists;

public class VectorUtil {

  public static void showVectorAccessibleContent(VectorAccessible va, final String delimiter) {

    int rows = va.getRecordCount();
    List<String> columns = Lists.newArrayList();
    for (VectorWrapper vw : va) {
      columns.add(vw.getValueVector().getField().getName());
    }

    int width = columns.size();
    for (String column : columns) {
      System.out.printf("%s%s",column, column == columns.get(width - 1) ? "\n" : delimiter);
    }
    for (int row = 0; row < rows; row++) {
      int columnCounter = 0;
      for (VectorWrapper vw : va) {
        boolean lastColumn = columnCounter == width - 1;
        Object o = vw.getValueVector().getAccessor().getObject(row);
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
  }

  public static void showVectorAccessibleContent(VectorAccessible va) {

    int rows = va.getRecordCount();
    List<String> columns = Lists.newArrayList();
    for (VectorWrapper vw : va) {
      columns.add(vw.getValueVector().getField().getName());
    }

    int width = columns.size();
    for (int row = 0; row < rows; row++) {
      if (row%50 == 0) {
        System.out.println(StringUtils.repeat("-", width*17 + 1));
        for (String column : columns) {
          System.out.printf("| %-15s", column.length() <= 15 ? column : column.substring(0, 14));
        }
        System.out.printf("|\n");
        System.out.println(StringUtils.repeat("-", width*17 + 1));
      }
      for (VectorWrapper vw : va) {
        Object o = vw.getValueVector().getAccessor().getObject(row);
        if (o == null) {
          //null value
          System.out.printf("| %-15s", "");
        }
        else if (o instanceof byte[]) {
          String value = new String((byte[]) o);
          System.out.printf("| %-15s",value.length() <= 15 ? value : value.substring(0, 14));
        } else {
          String value = o.toString();
          System.out.printf("| %-15s",value.length() <= 15 ? value : value.substring(0,14));
        }
      }
      System.out.printf("|\n");
    }

    if (rows > 0 )
      System.out.println(StringUtils.repeat("-", width*17 + 1));
  }


}
