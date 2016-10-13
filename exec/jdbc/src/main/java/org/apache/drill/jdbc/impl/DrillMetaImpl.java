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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import javax.validation.constraints.NotNull;

import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.AvaticaUtils;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.ColumnMetaData.StructType;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.proto.UserBitShared.DrillPBError;
import org.apache.drill.exec.proto.UserProtos.CatalogMetadata;
import org.apache.drill.exec.proto.UserProtos.ColumnMetadata;
import org.apache.drill.exec.proto.UserProtos.GetCatalogsResp;
import org.apache.drill.exec.proto.UserProtos.GetColumnsResp;
import org.apache.drill.exec.proto.UserProtos.GetSchemasResp;
import org.apache.drill.exec.proto.UserProtos.GetTablesResp;
import org.apache.drill.exec.proto.UserProtos.LikeFilter;
import org.apache.drill.exec.proto.UserProtos.RequestStatus;
import org.apache.drill.exec.proto.UserProtos.SchemaMetadata;
import org.apache.drill.exec.proto.UserProtos.TableMetadata;
import org.apache.drill.exec.rpc.DrillRpcFuture;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;



class DrillMetaImpl extends MetaImpl {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillMetaImpl.class);

  // TODO:  Use more central version of these constants if available.

  /** JDBC conventional(?) number of fractional decimal digits for REAL. */
  private static final int DECIMAL_DIGITS_REAL = 7;
  /** JDBC conventional(?) number of fractional decimal digits for FLOAT. */
  private static final int DECIMAL_DIGITS_FLOAT = DECIMAL_DIGITS_REAL;
  /** JDBC conventional(?) number of fractional decimal digits for DOUBLE. */
  private static final int DECIMAL_DIGITS_DOUBLE = 15;

  /** Radix used to report precisions of "datetime" types. */
  private static final int RADIX_DATETIME = 10;
  /** Radix used to report precisions of interval types. */
  private static final int RADIX_INTERVAL = 10;


  final DrillConnectionImpl connection;

  DrillMetaImpl(DrillConnectionImpl connection) {
    super(connection);
    this.connection = connection;
  }

  private static Signature newSignature(String sql) {
    return new Signature(
        new DrillColumnMetaDataList(),
        sql,
        Collections.<AvaticaParameter> emptyList(),
        Collections.<String, Object>emptyMap(),
        null // CursorFactory set to null, as SQL requests use DrillCursor
        );
  }


  /** Information about type mapping. */
  private static class TypeInfo {
    private static Map<Class<?>, TypeInfo> MAPPING = ImmutableMap.<Class<?>, TypeInfo> builder()
        .put(boolean.class, of(Types.BOOLEAN, "BOOLEAN"))
        .put(Boolean.class, of(Types.BOOLEAN, "BOOLEAN"))
        .put(Byte.TYPE, of(Types.TINYINT, "TINYINT"))
        .put(Byte.class, of(Types.TINYINT, "TINYINT"))
        .put(Short.TYPE, of(Types.SMALLINT, "SMALLINT"))
        .put(Short.class, of(Types.SMALLINT, "SMALLINT"))
        .put(Integer.TYPE, of(Types.INTEGER, "INTEGER"))
        .put(Integer.class, of(Types.INTEGER, "INTEGER"))
        .put(Long.TYPE,  of(Types.BIGINT, "BIGINT"))
        .put(Long.class, of(Types.BIGINT, "BIGINT"))
        .put(Float.TYPE, of(Types.FLOAT, "FLOAT"))
        .put(Float.class,  of(Types.FLOAT, "FLOAT"))
        .put(Double.TYPE,  of(Types.DOUBLE, "DOUBLE"))
        .put(Double.class, of(Types.DOUBLE, "DOUBLE"))
        .put(String.class, of(Types.VARCHAR, "CHARACTER VARYING"))
        .put(java.sql.Date.class, of(Types.DATE, "DATE"))
        .put(Time.class, of(Types.TIME, "TIME"))
        .put(Timestamp.class, of(Types.TIMESTAMP, "TIMESTAMP"))
        .build();

    private final int sqlType;
    private final String sqlTypeName;

    public TypeInfo(int sqlType, String sqlTypeName) {
      this.sqlType = sqlType;
      this.sqlTypeName = sqlTypeName;
    }

    private static TypeInfo of(int sqlType, String sqlTypeName) {
      return new TypeInfo(sqlType, sqlTypeName);
    }

    public static TypeInfo get(Class<?> clazz) {
      return MAPPING.get(clazz);
    }
  }

  /** Metadata describing a column.
   * Copied from Avatica with several fixes
   * */
  public static class MetaColumn implements Named {
    public final String tableCat;
    public final String tableSchem;
    public final String tableName;
    public final String columnName;
    public final int dataType;
    public final String typeName;
    public final Integer columnSize;
    public final Integer bufferLength = null;
    public final Integer decimalDigits;
    public final Integer numPrecRadix;
    public final int nullable;
    public final String remarks = null;
    public final String columnDef = null;
    public final Integer sqlDataType = null;
    public final Integer sqlDatetimeSub = null;
    public final Integer charOctetLength;
    public final int ordinalPosition;
    @NotNull
    public final String isNullable;
    public final String scopeCatalog = null;
    public final String scopeSchema = null;
    public final String scopeTable = null;
    public final Short sourceDataType = null;
    @NotNull
    public final String isAutoincrement = "";
    @NotNull
    public final String isGeneratedcolumn = "";

    public MetaColumn(
        String tableCat,
        String tableSchem,
        String tableName,
        String columnName,
        int dataType,
        String typeName,
        Integer columnSize,
        Integer decimalDigits,
        Integer numPrecRadix,
        int nullable,
        Integer charOctetLength,
        int ordinalPosition,
        String isNullable) {
      this.tableCat = tableCat;
      this.tableSchem = tableSchem;
      this.tableName = tableName;
      this.columnName = columnName;
      this.dataType = dataType;
      this.typeName = typeName;
      this.columnSize = columnSize;
      this.decimalDigits = decimalDigits;
      this.numPrecRadix = numPrecRadix;
      this.nullable = nullable;
      this.charOctetLength = charOctetLength;
      this.ordinalPosition = ordinalPosition;
      this.isNullable = isNullable;
    }

    @Override
    public String getName() {
      return columnName;
    }
  }

  private static LikeFilter newLikeFilter(final Pat pattern) {
    if (pattern == null || pattern.s == null) {
      return null;
    }

    return LikeFilter.newBuilder().setPattern(pattern.s).build();
  }

  /**
   * Quote the provided string as a LIKE pattern
   *
   * @param v the value to quote
   * @return a LIKE pattern matching exactly v, or {@code null} if v is {@code null}
   */
  private static Pat quote(String v) {
    if (v == null) {
      return null;
    }

    StringBuilder sb = new StringBuilder(v.length());
    for(int index = 0; index<v.length(); index++) {
      char c = v.charAt(index);
      switch(c) {
      case '%':
      case '_':
      case '\\':
        sb.append('\\').append(c);
        break;

      default:
        sb.append(c);
      }
    }

    return Pat.of(sb.toString());
  }

  // Overriding fieldMetaData as Calcite version create ColumnMetaData with invalid offset
  protected static ColumnMetaData.StructType drillFieldMetaData(Class<?> clazz) {
    final List<ColumnMetaData> list = new ArrayList<>();
    for (Field field : clazz.getFields()) {
      if (Modifier.isPublic(field.getModifiers())
          && !Modifier.isStatic(field.getModifiers())) {
        NotNull notNull = field.getAnnotation(NotNull.class);
        boolean notNullable = (notNull != null || field.getType().isPrimitive());
        list.add(
            drillColumnMetaData(
                AvaticaUtils.camelToUpper(field.getName()),
                list.size(), field.getType(), notNullable));
      }
    }
    return ColumnMetaData.struct(list);
  }


  protected static ColumnMetaData drillColumnMetaData(String name, int index,
      Class<?> type, boolean notNullable) {
    TypeInfo pair = TypeInfo.get(type);
    ColumnMetaData.Rep rep =
        ColumnMetaData.Rep.VALUE_MAP.get(type);
    ColumnMetaData.AvaticaType scalarType =
        ColumnMetaData.scalar(pair.sqlType, pair.sqlTypeName, rep);
    return new ColumnMetaData(
        index, false, true, false, false,
        notNullable
            ? DatabaseMetaData.columnNoNulls
            : DatabaseMetaData.columnNullable,
        true, -1, name, name, null,
        0, 0, null, null, scalarType, true, false, false,
        scalarType.columnClassName());
  }

  abstract private class MetadataAdapter<CalciteMetaType, Response, ResponseValue> {
    private final Class<? extends CalciteMetaType> clazz;

    public MetadataAdapter(Class<? extends CalciteMetaType> clazz) {
      this.clazz = clazz;
    }

    MetaResultSet getMeta(DrillRpcFuture<Response> future) {
      final Response response;
      try {
        response = future.get();
      } catch (InterruptedException e) {
        throw Throwables.propagate(e);
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause != null) {
          throw Throwables.propagate(cause);
        }
        throw Throwables.propagate(e);
      }

      try {
        List<Object> tables = Lists.transform(getResult(response), new Function<ResponseValue, Object>() {
          @Override
          public Object apply(ResponseValue input) {
            return adapt(input);
          }
        });

        Meta.Frame frame = Meta.Frame.create(0, true, tables);
        StructType fieldMetaData = drillFieldMetaData(clazz);
        Meta.Signature signature = Meta.Signature.create(
            fieldMetaData.columns, "",
            Collections.<AvaticaParameter>emptyList(), CursorFactory.record(clazz));

        AvaticaStatement statement = connection.createStatement();
        return MetaResultSet.create(connection.id, statement.getId(), true,
            signature, frame);
      } catch (Exception e) {
        // Wrap in RuntimeException because Avatica's abstract method declarations
        // didn't allow for SQLException!
        throw new DrillRuntimeException("Failure while attempting to get DatabaseMetadata.", e);
      }
    }

    abstract protected RequestStatus getStatus(Response response);
    abstract protected DrillPBError getError(Response response);
    abstract protected List<ResponseValue> getResult(Response response);
    abstract protected CalciteMetaType adapt(ResponseValue protoValue);
  }

  @Override
  public MetaResultSet getTables(String catalog, final Pat schemaPattern, final Pat tableNamePattern,
      final List<String> typeList) {
    // Catalog is not a pattern
    final LikeFilter catalogNameFilter = newLikeFilter(quote(catalog));
    final LikeFilter schemaNameFilter = newLikeFilter(schemaPattern);
    final LikeFilter tableNameFilter = newLikeFilter(tableNamePattern);

    return new MetadataAdapter<MetaImpl.MetaTable, GetTablesResp, TableMetadata>(MetaTable.class) {

      @Override
      protected RequestStatus getStatus(GetTablesResp response) {
        return response.getStatus();
      };

      @Override
      protected DrillPBError getError(GetTablesResp response) {
        return response.getError();
      };

      @Override
      protected List<TableMetadata> getResult(GetTablesResp response) {
        return response.getTablesList();
      }

      @Override
      protected MetaImpl.MetaTable adapt(TableMetadata protoValue) {
        return new MetaImpl.MetaTable(protoValue.getCatalogName(), protoValue.getSchemaName(), protoValue.getTableName(), protoValue.getType());
      };
    }.getMeta(connection.getClient().getTables(catalogNameFilter, schemaNameFilter, tableNameFilter, typeList));
  }

  /**
   * Implements {@link DatabaseMetaData#getColumns}.
   */
  @Override
  public MetaResultSet getColumns(String catalog, Pat schemaPattern,
                              Pat tableNamePattern, Pat columnNamePattern) {
    final LikeFilter catalogNameFilter = newLikeFilter(quote(catalog));
    final LikeFilter schemaNameFilter = newLikeFilter(schemaPattern);
    final LikeFilter tableNameFilter = newLikeFilter(tableNamePattern);
    final LikeFilter columnNameFilter = newLikeFilter(columnNamePattern);

    return new MetadataAdapter<MetaColumn, GetColumnsResp, ColumnMetadata>(MetaColumn.class) {
      @Override
      protected RequestStatus getStatus(GetColumnsResp response) {
        return response.getStatus();
      }

      @Override
      protected DrillPBError getError(GetColumnsResp response) {
        return response.getError();
      }

      @Override
      protected List<ColumnMetadata> getResult(GetColumnsResp response) {
        return response.getColumnsList();
      };

      private Integer getColumnSize(ColumnMetadata value) {
        /* "... COLUMN_SIZE ....
         * For numeric data, this is the maximum precision (in base 10).
         * For character data, this is the length in characters.
         * For datetime datatypes, this is the length in characters of the String
         *   representation (assuming the maximum allowed precision of the
         *   fractional seconds component).
         * For binary data, this is the length in bytes.
         * For the ROWID datatype, this is the length in bytes.
         * Null is returned for data types where the column size is not applicable."
         *
         * Note:  "Maximum precision" seems to mean the maximum number of
         * significant digits that can appear (not the number of decimal digits
         * that can be counted on, and not the maximum number of (decimal)
         * characters needed to display a value).
         */
        switch(value.getDataType()) {
        // 1. For numeric data, the maximum precision in base 10:
        case "TINYINT":
        case "SMALLINT":
        case "INTEGER":
        case "BIGINT":
        case "DECIMAL":
        case "NUMERIC":
        case "REAL":
        case "FLOAT":
        case "DOUBLE":
        case "DOUBLE PRECISION":
          return value.getNumericPrecision();

        // 2. For character data, the length in characters
        case "CHARACTER":
        case "CHARACTER VARYING":
          return value.getCharMaxLength();

        // 3. For datetime datatypes ... length ... String representation
        //    (assuming the maximum ... precision of ... fractional seconds ...)
        // SQL datetime types:
        case "DATE":
          return 10; // YYYY-MM-DD
        case "TIME":
          if (value.getDateTimePrecision() > 0) {
            return 8 + 1 + value.getDateTimePrecision(); // HH:MM:SS.sss
          } else {
            return 8; // HH:MM:SS
          }

        case "TIMESTAMP":
          if (value.getDateTimePrecision() > 0) {
            return 10 + 1 + 8 + 1 + value.getDateTimePrecision(); // date + "T" + time
          } else {
            return 10 + 1 + 8;
          }
        // SQL interval types:
        // Note:  Not addressed by JDBC 4.1; providing length of current string
        // representation (not length of, say, interval literal).
        case "INTERVAL":
          int intervalPrecision = value.getIntervalPrecision();
          int extraSecondIntervalSize = value.getDateTimePrecision();
          if (extraSecondIntervalSize > 0) {
            // If frac. digits, also add 1 for decimal point.
            extraSecondIntervalSize++;
          }

          switch(value.getIntervalType()) {
          // a. Single field, not SECOND:
          case "YEAR":
          case "MONTH":
          case "DAY":
            return intervalPrecision + 2; // P...Y
          case "HOUR":
          case "MINUTE":
            return intervalPrecision + 3; // PT...M

          // b. Two adjacent fields, no SECOND:
          case "YEAR TO MONTH":
            return intervalPrecision + 5; // P..Y12M
          case "DAY TO HOUR":
            return intervalPrecision + 6; // P...DT12H
          case "HOUR TO MINUTE":
            return intervalPrecision + 6; // PT..H12M
          // c. Three contiguous fields, no SECOND:
          case "DAY TO MINUTE":
            return intervalPrecision + 9; // P...DT12H12M

          // d. With SECOND field:
          // - For INTERVAL ... TO SECOND(0): "P...DT12H12M12S"
          case "DAY TO SECOND":
            return intervalPrecision + extraSecondIntervalSize + 12; // P...DT12H12M12...S
          case "HOUR TO SECOND":
            return intervalPrecision + extraSecondIntervalSize + 9; // PT...H12M12...S
          case "MINUTE TO SECOND":
            return intervalPrecision + extraSecondIntervalSize + 6; // PT...M12...S
          case "SECOND":
            return intervalPrecision + extraSecondIntervalSize + 3; // PT......S
          default:
            return -1; // Make net result be -1:
          }

        // 4. "For binary data, ... the length in bytes":
        case "BINARY":
        case "BINARY VARYING":
          return value.getCharMaxLength();

        // 5. "For ... ROWID datatype...": Not in Drill?
        // 6. "unknown ... for data types [for which] ... not applicable.":
        default:
          return null;
        }
      }

      private int getDataType(ColumnMetadata value) {
        switch (value.getDataType()) {
        case "ARRAY":
          return Types.ARRAY;

        case "BIGINT":
          return Types.BIGINT;
        case "BINARY":
          return Types.BINARY;
        case "BINARY LARGE OBJECT":
          return Types.BLOB;
        case "BINARY VARYING":
          return Types.VARBINARY;
        case "BIT":
          return Types.BIT;
        case "BOOLEAN":
          return Types.BOOLEAN;
        case "CHARACTER":
          return Types.CHAR;
        // Resolve: Not seen in Drill yet. Can it appear?:
        case "CHARACTER LARGE OBJECT":
          return Types.CLOB;
        case "CHARACTER VARYING":
          return Types.VARCHAR;

        // Resolve: Not seen in Drill yet. Can it appear?:
        case "DATALINK":
          return Types.DATALINK;
        case "DATE":
          return Types.DATE;
        case "DECIMAL":
          return Types.DECIMAL;
        // Resolve: Not seen in Drill yet. Can it appear?:
        case "DISTINCT":
          return Types.DISTINCT;
        case "DOUBLE":
        case "DOUBLE PRECISION":
          return Types.DOUBLE;

        case "FLOAT":
          return Types.FLOAT;

        case "INTEGER":
          return Types.INTEGER;
        case "INTERVAL":
          return Types.OTHER;

        // Resolve: Not seen in Drill yet. Can it ever appear?:
        case "JAVA_OBJECT":
          return Types.JAVA_OBJECT;

        // Resolve: Not seen in Drill yet. Can it appear?:
        case "LONGNVARCHAR":
          return Types.LONGNVARCHAR;
        // Resolve: Not seen in Drill yet. Can it appear?:
        case "LONGVARBINARY":
          return Types.LONGVARBINARY;
        // Resolve: Not seen in Drill yet. Can it appear?:
        case "LONGVARCHAR":
          return Types.LONGVARCHAR;

        case "MAP":
          return Types.OTHER;

        // Resolve: Not seen in Drill yet. Can it appear?:
        case "NATIONAL CHARACTER":
          return Types.NCHAR;
        // Resolve: Not seen in Drill yet. Can it appear?:
        case "NATIONAL CHARACTER LARGE OBJECT":
          return Types.NCLOB;
        // Resolve: Not seen in Drill yet. Can it appear?:
        case "NATIONAL CHARACTER VARYING":
          return Types.NVARCHAR;

        // TODO: Resolve following about NULL (and then update comment and
        // code):
        // It is not clear whether Types.NULL can represent a type (perhaps the
        // type of the literal NULL when no further type information is known?)
        // or
        // whether 'NULL' can appear in INFORMATION_SCHEMA.COLUMNS.DATA_TYPE.
        // For now, since it shouldn't hurt, include 'NULL'/Types.NULL in
        // mapping.
        case "NULL":
          return Types.NULL;
        // (No NUMERIC--Drill seems to map any to DECIMAL currently.)
        case "NUMERIC":
          return Types.NUMERIC;

        // Resolve: Unexpectedly, has appeared in Drill. Should it?
        case "OTHER":
          return Types.OTHER;

        case "REAL":
          return Types.REAL;
        // Resolve: Not seen in Drill yet. Can it appear?:
        case "REF":
          return Types.REF;
        // Resolve: Not seen in Drill yet. Can it appear?:
        case "ROWID":
          return Types.ROWID;

        case "SMALLINT":
          return Types.SMALLINT;
        // Resolve: Not seen in Drill yet. Can it appear?:
        case "SQLXML":
          return Types.SQLXML;
        case "STRUCT":
          return Types.STRUCT;

        case "TIME":
          return Types.TIME;
        case "TIMESTAMP":
          return Types.TIMESTAMP;
        case "TINYINT":
          return Types.TINYINT;

        default:
          return Types.OTHER;
        }
      }

      Integer getDecimalDigits(ColumnMetadata value) {
        switch(value.getDataType()) {
        case "TINYINT":
        case "SMALLINT":
        case "INTEGER":
        case "BIGINT":
        case "DECIMAL":
        case "NUMERIC":
          return value.hasNumericScale() ? value.getNumericScale() : null;

        case "REAL":
          return DECIMAL_DIGITS_REAL;

        case "FLOAT":
          return DECIMAL_DIGITS_FLOAT;

        case "DOUBLE":
          return DECIMAL_DIGITS_DOUBLE;

        case "DATE":
        case "TIME":
        case "TIMESTAMP":
        case "INTERVAL":
          return value.getDateTimePrecision();

        default:
          return null;
        }
      }

      private Integer getNumPrecRadix(ColumnMetadata value) {
        switch(value.getDataType()) {
        case "TINYINT":
        case "SMALLINT":
        case "INTEGER":
        case "BIGINT":
        case "DECIMAL":
        case "NUMERIC":
        case "REAL":
        case "FLOAT":
        case "DOUBLE":
          return value.getNumericPrecisionRadix();

        case "INTERVAL":
          return RADIX_INTERVAL;

        case "DATE":
        case "TIME":
        case "TIMESTAMP":
          return RADIX_DATETIME;

        default:
          return null;
        }
      }

      private int getNullable(ColumnMetadata value) {
        if (!value.hasIsNullable()) {
          return DatabaseMetaData.columnNullableUnknown;
        }
        return  value.getIsNullable() ? DatabaseMetaData.columnNullable : DatabaseMetaData.columnNoNulls;
      }

      private String getIsNullable(ColumnMetadata value) {
        if (!value.hasIsNullable()) {
          return "";
        }
        return  value.getIsNullable() ? "YES" : "NO";
      }

      private Integer getCharOctetLength(ColumnMetadata value) {
        if (!value.hasCharMaxLength()) {
          return null;
        }

        switch(value.getDataType()) {
        case "CHARACTER":
        case "CHARACTER LARGE OBJECT":
        case "CHARACTER VARYING":
        case "LONGVARCHAR":
        case "LONGNVARCHAR":
        case "NATIONAL CHARACTER":
        case "NATIONAL CHARACTER LARGE OBJECT":
        case "NATIONAL CHARACTER VARYING":
          return value.getCharOctetLength();

        default:
          return null;
        }
      }

      @Override
      protected MetaColumn adapt(ColumnMetadata value) {
        return new MetaColumn(
            value.getCatalogName(),
            value.getSchemaName(),
            value.getTableName(),
            value.getColumnName(),
            getDataType(value), // It might require the full SQL type
            value.getDataType(),
            getColumnSize(value),
            getDecimalDigits(value),
            getNumPrecRadix(value),
            getNullable(value),
            getCharOctetLength(value),
            value.getOrdinalPosition(),
            getIsNullable(value));
      }
    }.getMeta(connection.getClient().getColumns(catalogNameFilter, schemaNameFilter, tableNameFilter, columnNameFilter));
  }

  @Override
  public MetaResultSet getSchemas(String catalog, Pat schemaPattern) {
    final LikeFilter catalogNameFilter = newLikeFilter(quote(catalog));
    final LikeFilter schemaNameFilter = newLikeFilter(schemaPattern);

    return new MetadataAdapter<MetaImpl.MetaSchema, GetSchemasResp, SchemaMetadata>(MetaImpl.MetaSchema.class) {
      @Override
      protected RequestStatus getStatus(GetSchemasResp response) {
        return response.getStatus();
      }

      @Override
      protected List<SchemaMetadata> getResult(GetSchemasResp response) {
        return response.getSchemasList();
      }

      @Override
      protected DrillPBError getError(GetSchemasResp response) {
        return response.getError();
      }

      @Override
      protected MetaSchema adapt(SchemaMetadata value) {
        return new MetaImpl.MetaSchema(value.getCatalogName(), value.getSchemaName());
      }
    }.getMeta(connection.getClient().getSchemas(catalogNameFilter, schemaNameFilter));
  }

  @Override
  public MetaResultSet getCatalogs() {
    return new MetadataAdapter<MetaImpl.MetaCatalog, GetCatalogsResp, CatalogMetadata>(MetaImpl.MetaCatalog.class) {
      @Override
      protected RequestStatus getStatus(GetCatalogsResp response) {
        return response.getStatus();
      }

      @Override
      protected List<CatalogMetadata> getResult(GetCatalogsResp response) {
        return response.getCatalogsList();
      }

      @Override
      protected DrillPBError getError(GetCatalogsResp response) {
        return response.getError();
      }

      @Override
      protected MetaImpl.MetaCatalog adapt(CatalogMetadata protoValue) {
        return new MetaImpl.MetaCatalog(protoValue.getCatalogName());
      }
    }.getMeta(connection.getClient().getCatalogs(null));
  }


  interface Named {
    String getName();
  }

  @Override
  public StatementHandle prepare(ConnectionHandle ch, String sql, long maxRowCount) {
    StatementHandle result = super.createStatement(ch);
    result.signature = newSignature(sql);

    return result;
  }

  @Override
  public ExecuteResult prepareAndExecute(StatementHandle h, String sql, long maxRowCount, PrepareCallback callback) {
    final Signature signature = newSignature(sql);
    try {
      synchronized (callback.getMonitor()) {
        callback.clear();
        callback.assign(signature, null, -1);
      }
      callback.execute();
      final MetaResultSet metaResultSet = MetaResultSet.create(h.connectionId, h.id, false, signature, null);
      return new ExecuteResult(Collections.singletonList(metaResultSet));
    } catch(SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void closeStatement(StatementHandle h) {
    // Nothing
  }
}
