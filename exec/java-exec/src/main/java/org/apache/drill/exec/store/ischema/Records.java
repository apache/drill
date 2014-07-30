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

package org.apache.drill.exec.store.ischema;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.sql.type.SqlTypeName;

public class Records {

  /** Pojo object for a record in INFORMATION_SCHEMA.TABLES */
  public static class Table {
    public final String TABLE_CATALOG;
    public final String TABLE_SCHEMA;
    public final String TABLE_NAME;
    public final String TABLE_TYPE;

    public Table(String catalog, String schema, String name, String type) {
      this.TABLE_CATALOG = catalog;
      this.TABLE_SCHEMA = schema;
      this.TABLE_NAME = name;
      this.TABLE_TYPE = type;
    }
  }

  /** Pojo object for a record in INFORMATION_SCHEMA.COLUMNS */
  public static class Column {
    public final String TABLE_CATALOG;
    public final String TABLE_SCHEMA;
    public final String TABLE_NAME;
    public final String COLUMN_NAME;
    public final int ORDINAL_POSITION;
    public final String IS_NULLABLE;
    public final String DATA_TYPE;
    public final int CHARACTER_MAXIMUM_LENGTH;
    public final int NUMERIC_PRECISION_RADIX;
    public final int NUMERIC_SCALE;
    public final int NUMERIC_PRECISION;

    public Column(String catalog, String schemaName, String tableName, RelDataTypeField field) {
      this.TABLE_CATALOG = catalog;
      this.TABLE_SCHEMA = schemaName;
      this.TABLE_NAME = tableName;

      this.COLUMN_NAME = field.getName();
      RelDataType type = field.getType();
      SqlTypeName sqlType = type.getSqlTypeName();

      this.ORDINAL_POSITION = field.getIndex();
      this.IS_NULLABLE = type.isNullable() ? "YES" : "NO";

      if (sqlType == SqlTypeName.ARRAY || sqlType == SqlTypeName.MAP || sqlType == SqlTypeName.ROW) {
        // For complex types use the toString method to display the inside elements
        String typeString = type.toString();

        // RelDataType.toString prints "RecordType" for "STRUCT".
        this.DATA_TYPE = type.toString().replace("RecordType", "STRUCT");
      } else {
        this.DATA_TYPE = sqlType.toString();
      }

      this.NUMERIC_PRECISION_RADIX = (sqlType == SqlTypeName.DECIMAL) ? 10 : -1; // TODO: where do we get radix?

      if (sqlType == SqlTypeName.VARCHAR) {
        // Max length is stored as precision in Optiq.
        this.CHARACTER_MAXIMUM_LENGTH = (sqlType.allowsPrec()) ? type.getPrecision() : -1;
        this.NUMERIC_PRECISION = -1;
      } else {
        this.CHARACTER_MAXIMUM_LENGTH = -1;
        this.NUMERIC_PRECISION = (sqlType.allowsPrec()) ? type.getPrecision() : -1;
      }

      this.NUMERIC_SCALE = (sqlType.allowsScale())?type.getScale(): -1;
    }
  }

  /** Pojo object for a record in INFORMATION_SCHEMA.VIEWS */
  public static class View {
    public final String TABLE_CATALOG;
    public final String TABLE_SCHEMA;
    public final String TABLE_NAME;
    public final String VIEW_DEFINITION;

    public View(String catalog, String schema, String name, String definition) {
      this.TABLE_CATALOG = catalog;
      this.TABLE_SCHEMA = schema;
      this.TABLE_NAME = name;
      this.VIEW_DEFINITION = definition;
    }
  }

  /** Pojo object for a record in INFORMATION_SCHEMA.CATALOGS */
  public static class Catalog {
    public final String CATALOG_NAME;
    public final String CATALOG_DESCRIPTION;
    public final String CATALOG_CONNECT;

    public Catalog(String name, String description, String connect) {
      this.CATALOG_NAME = name;
      this.CATALOG_DESCRIPTION = description;
      this.CATALOG_CONNECT = connect;
    }
  }

  /** Pojo object for a record in INFORMATION_SCHEMA.SCHEMATA */
  public static class Schema {
    public final String CATALOG_NAME;
    public final String SCHEMA_NAME;
    public final String SCHEMA_OWNER;
    public final String TYPE;
    public final String IS_MUTABLE;

    public Schema(String catalog, String name, String owner, String type, boolean isMutable) {
      this.CATALOG_NAME = catalog;
      this.SCHEMA_NAME = name;
      this.SCHEMA_OWNER = owner;
      this.TYPE = type;
      this.IS_MUTABLE = isMutable ? "YES" : "NO";
    }
  }
}
