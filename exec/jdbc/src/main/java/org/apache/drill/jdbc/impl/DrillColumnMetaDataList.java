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
package org.apache.drill.jdbc.impl;

import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import net.hydromatic.avatica.ColumnMetaData;
import net.hydromatic.avatica.ColumnMetaData.AvaticaType;
import net.hydromatic.avatica.ColumnMetaData.Rep;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;

public class DrillColumnMetaDataList extends BasicList<ColumnMetaData>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillColumnMetaDataList.class);

  private List<ColumnMetaData> columns = new ArrayList<ColumnMetaData>();

  @Override
  public int size() {
    return columns.size();
  }

  @Override
  public ColumnMetaData get(int index) {
    return columns.get(index);
  }

  /**
   * Gets AvaticaType carrying both JDBC {@code java.sql.Type.*} type code
   * and SQL type name for given RPC-level type (from batch schema).
   */
  private static AvaticaType getAvaticaType( MajorType rpcDateType ) {
    final String sqlTypeName = Types.getSqlTypeName( rpcDateType );
    final int jdbcTypeId = Types.getJdbcTypeCode( rpcDateType );
    return ColumnMetaData.scalar( jdbcTypeId, sqlTypeName,
                                  Rep.BOOLEAN /* dummy value, unused */ );
  }

  public void updateColumnMetaData(String catalogName, String schemaName,
                                   String tableName, BatchSchema schema,
                                   List<Class<?>> getObjectClasses ) {
    final List<ColumnMetaData> newColumns =
        new ArrayList<ColumnMetaData>(schema.getFieldCount());
    for (int colOffset = 0; colOffset < schema.getFieldCount(); colOffset++) {
      final MaterializedField field = schema.getColumn(colOffset);
      Class<?> objectClass = getObjectClasses.get( colOffset );

      final String columnName = field.getPath();

      final MajorType rpcDataType = field.getType();
      final AvaticaType bundledSqlDataType = getAvaticaType(rpcDataType);
      final String columnClassName = objectClass.getName();

      final int nullability;
      switch ( field.getDataMode() ) {
        case OPTIONAL: nullability = ResultSetMetaData.columnNullable; break;
        case REQUIRED: nullability = ResultSetMetaData.columnNoNulls;  break;
        // Should REPEATED still map to columnNoNulls? or to columnNullable?
        case REPEATED: nullability = ResultSetMetaData.columnNoNulls;  break;
        default:
          throw new AssertionError( "Unexpected new DataMode value '"
                                    + field.getDataMode().name() + "'" );
      }
      final boolean isSigned = Types.isJdbcSignedType( rpcDataType );

      // TODO(DRILL-3355):  TODO(DRILL-3356):  When string lengths, precisions,
      // interval kinds, etc., are available from RPC-level data, implement:
      // - precision for ResultSetMetadata.getPrecision(...) (like
      //   getColumns()'s COLUMN_SIZE)
      // - scale for getScale(...), and
      // - and displaySize for getColumnDisplaySize(...).
      final int precision =
          rpcDataType.hasPrecision() ? rpcDataType.getPrecision() : 0;
      final int scale = rpcDataType.hasScale() ? rpcDataType.getScale() : 0;
      final int displaySize = 10;

      ColumnMetaData col = new ColumnMetaData(
          colOffset,    // (zero-based ordinal (for Java arrays/lists).)
          false,        /* autoIncrement */
          false,        /* caseSensitive */
          true,         /* searchable */
          false,        /* currency */
          nullability,
          isSigned,
          displaySize,
          columnName,   /* label */
          columnName,   /* columnName */
          schemaName,
          precision,
          scale,
          tableName,
          catalogName,
          bundledSqlDataType,
          true,         /* readOnly */
          false,        /* writable */
          false,        /* definitelyWritable */
          columnClassName
         );
      newColumns.add(col);
    }
    columns = newColumns;
  }

  @Override
  public boolean contains(Object o) {
    return columns.contains(o);
  }

  @Override
  public Iterator<ColumnMetaData> iterator() {
    return columns.iterator();
  }

  @Override
  public Object[] toArray() {
    return columns.toArray();
  }

  @Override
  public <T> T[] toArray(T[] a) {
    return columns.toArray(a);
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    return columns.containsAll(c);
  }

  @Override
  public int indexOf(Object o) {
    return columns.indexOf(o);
  }

  @Override
  public int lastIndexOf(Object o) {
    return columns.lastIndexOf(o);
  }

  @Override
  public ListIterator<ColumnMetaData> listIterator() {
    return columns.listIterator();
  }

  @Override
  public ListIterator<ColumnMetaData> listIterator(int index) {
    return columns.listIterator(index);
  }

  @Override
  public List<ColumnMetaData> subList(int fromIndex, int toIndex) {
    return columns.subList(fromIndex, toIndex);
  }
}
