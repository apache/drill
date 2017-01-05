/**
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
package org.apache.drill.exec.store.indexr;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Util;
import org.apache.commons.lang3.StringUtils;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;
import org.apache.drill.exec.store.AbstractRecordReader;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import io.indexr.segment.ColumnSchema;
import io.indexr.segment.ColumnType;
import io.indexr.segment.SegmentSchema;
import io.indexr.server.HybridTable;
import io.indexr.util.Pair;

public class DrillIndexRTable extends DynamicDrillTable {
  private final SegmentSchema schema;

  public DrillIndexRTable(IndexRStoragePlugin plugin, IndexRScanSpec spec, SegmentSchema schema) {
    super(plugin, plugin.pluginName(), spec);
    this.schema = schema;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    List<String> names = new ArrayList<>();
    List<RelDataType> types = new ArrayList<>();
    for (ColumnSchema cs : schema.columns) {
      names.add(cs.name);
      types.add(parseDataType(typeFactory, cs.dataType));
    }
    return typeFactory.createStructType(types, names);
  }

  public static RelDataType parseDataType(RelDataTypeFactory typeFactory, byte type) {
    switch (type) {
      case ColumnType.INT:
        return typeFactory.createSqlType(SqlTypeName.INTEGER);
      case ColumnType.LONG:
        return typeFactory.createSqlType(SqlTypeName.BIGINT);
      case ColumnType.FLOAT:
        return typeFactory.createSqlType(SqlTypeName.FLOAT);
      case ColumnType.DOUBLE:
        return typeFactory.createSqlType(SqlTypeName.DOUBLE);
      case ColumnType.STRING:
        return typeFactory.createTypeWithCharsetAndCollation(
            typeFactory.createSqlType(SqlTypeName.VARCHAR, ColumnType.MAX_STRING_UTF8_SIZE),
            Util.getDefaultCharset(),
            SqlCollation.IMPLICIT
        );
      default:
        throw new UnsupportedOperationException(String.format("Unsupported type [%s]", type));
    }
  }

  public static MinorType parseMinorType(byte type) {
    switch (type) {
      case ColumnType.INT:
        return MinorType.INT;
      case ColumnType.LONG:
        return MinorType.BIGINT;
      case ColumnType.FLOAT:
        return MinorType.FLOAT4;
      case ColumnType.DOUBLE:
        return MinorType.FLOAT8;
      case ColumnType.STRING:
        return MinorType.VARCHAR;
      default:
        throw new UnsupportedOperationException(String.format("Unsupported type [%s]", type));
    }
  }

  public static Pair<ColumnSchema, Integer> mapColumn(String tableName, SegmentSchema segmentSchema, SchemaPath schemaPath) {
    String colName = toColName(tableName, schemaPath);
    return mapColumn(segmentSchema, colName);
  }

  public static String toColName(String tableName, SchemaPath schemaPath) {
    return StringUtils.removeStart(//
        schemaPath.getAsUnescapedPath().toLowerCase(),//
        tableName + "."); // remove the table name.
  }

  private static Pair<ColumnSchema, Integer> mapColumn(SegmentSchema segmentSchema, String colName) {
    int[] ordinal = new int[]{-1};
    ColumnSchema cs = segmentSchema.columns.stream().filter(
        new Predicate<ColumnSchema>() {
          @Override
          public boolean test(ColumnSchema schema) {
            ordinal[0]++;
            return schema.name.equalsIgnoreCase(colName);
          }
        }).findFirst().get();
    return cs == null ? null : Pair.of(cs, ordinal[0]);
  }

  public static Integer mapColumn(ColumnSchema columnSchema, SegmentSchema segmentSchema) {
    int index = 0;
    for (ColumnSchema cs : segmentSchema.columns) {
      if (StringUtils.equals(cs.name, columnSchema.name) && cs.dataType == columnSchema.dataType) {
        return index;
      }
      index++;
    }
    return null;
  }

  public static double bytCostPerField(ColumnSchema cs, boolean isCompress) {
    switch (cs.dataType) {
      case ColumnType.INT:
        return isCompress ? 4 : 4 * 0.2;
      case ColumnType.LONG:
        return isCompress ? 8 : 8 * 0.2;
      case ColumnType.FLOAT:
        return isCompress ? 4 : 4 * 0.5;
      case ColumnType.DOUBLE:
        return isCompress ? 8 : 8 * 0.5;
      case ColumnType.STRING:
        return isCompress ? 100 : 100 * 0.75;
      default:
        throw new UnsupportedOperationException(String.format("Unsupported type [%s]", cs.dataType));
    }
  }


  public static double byteCostPerRow(HybridTable table, List<SchemaPath> columns, boolean isCompress) {
    if (columns == null) {
      return 8 * 20;
    } else if (AbstractRecordReader.isStarQuery(columns)) {
      SegmentSchema schema = table.schema().schema;
      double b = 0;
      for (ColumnSchema cs : schema.columns) {
        b += DrillIndexRTable.bytCostPerField(cs, isCompress);
      }
      return b;
    } else {
      SegmentSchema schema = table.schema().schema;
      double b = 0;
      for (SchemaPath f : columns) {
        ColumnSchema cs = DrillIndexRTable.mapColumn(table.name(), schema, f).first;
        b += DrillIndexRTable.bytCostPerField(cs, isCompress);
      }
      return b;
    }
  }
}
