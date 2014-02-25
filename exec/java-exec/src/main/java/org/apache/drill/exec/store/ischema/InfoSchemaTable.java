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

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.VarCharVector;

/**
 * InfoSchemaTable defines the various Information Schema tables.
 * <p>
 * All the information schema tables are grouped together for convenience.
 * For each specific table, the corresponding class:
 * <p>Declares the table name.
 * <p>Declares the field names and types.
 * <p>Optionally defines a typed method to write a row of data to the vectors.
 *    If not defined here, FixedTable will kick in and do the job using
 *    a slower, generic method.
 */
public class InfoSchemaTable{

  /**
   * Layout for the SCHEMATA table.
   */
  public static class Schemata extends FixedTable {
    static final String tableName = "SCHEMATA";
    static final String[] fieldNames = {"CATALOG_NAME", "SCHEMA_NAME", "SCHEMA_OWNER"};
    static final MajorType[] fieldTypes = {VARCHAR,         VARCHAR,       VARCHAR};

    public Schemata() {
      super(tableName, fieldNames, fieldTypes);
    }

    // Optional ...
    public boolean writeRowToVectors(int index, Object[] row) {
      return 
          setSafe((VarCharVector)vectors.get(0), index, (String)row[0])  &&
          setSafe((VarCharVector)vectors.get(1), index,  (String)row[1]) &&
          setSafe((VarCharVector)vectors.get(2), index,  (String)row[2]);
    }
  }

  /**
   * Layout for the TABLES table.
   */
  public static class Tables extends FixedTable {
    static final String tableName = "TABLES";
    static final String[] fieldNames = {"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "TABLE_TYPE"};
    static final MajorType[] fieldTypes = {VARCHAR,          VARCHAR,        VARCHAR,      VARCHAR};

    public Tables() {
      super(tableName, fieldNames, fieldTypes);
    }  

    // Optional ...
    public boolean writeRowToVectors(int index, Object[] row) {
      return
          setSafe((VarCharVector)vectors.get(0), index, (String)row[0]) &&
          setSafe((VarCharVector)vectors.get(1), index, (String)row[1]) &&
          setSafe((VarCharVector)vectors.get(2), index, (String)row[2]) &&
          setSafe((VarCharVector)vectors.get(3), index, (String)row[3]);
    }
  }


  /**
   * Layout for the COLUMNS table.
   */
  public static class Columns extends FixedTable {
    static final String tableName = "COLUMNS";
    static final String[] fieldNames = {"TABLE_CATALOG",     "TABLE_SCHEMA",     "TABLE_NAME",    "COLUMN_NAME",
      "ORDINAL_POSITION",   "IS_NULLABLE",      "DATA_TYPE",     "CHARACTER_MAXIMUM_LENGTH",
      "NUMERIC_PRECISION_RADIX", "NUMERIC_SCALE", "NUMERIC_PRECISION"};
    static final MajorType[] fieldTypes= { VARCHAR,         VARCHAR,       VARCHAR,      VARCHAR,
      INT,             VARCHAR,        VARCHAR,     INT,
      INT,             INT,            INT};
    public Columns() {
      super(tableName, fieldNames, fieldTypes);
    }


    // Optional ...
    public boolean writeRowToVectors(int index, Object[] row) {
      return 
          setSafe((VarCharVector)vectors.get(0), index, (String)row[0]) &&
          setSafe((VarCharVector)vectors.get(1), index, (String)row[1]) &&
          setSafe((VarCharVector)vectors.get(2), index, (String)row[2]) &&
          setSafe((VarCharVector)vectors.get(3), index, (String)row[3]) && 
          setSafe((IntVector)vectors.get(4), index, (int)row[4])        &&
          setSafe((VarCharVector)vectors.get(5), index, (String)row[5])     &&
          setSafe((VarCharVector)vectors.get(6), index, (String)row[6]) &&  
          setSafe((IntVector)vectors.get(7), index, (int)row[7]) &&
          setSafe((IntVector)vectors.get(8), index, (int)row[8])        &&
          setSafe((IntVector)vectors.get(9), index, (int)row[9])        &&
          setSafe((IntVector)vectors.get(10), index, (int)row[10]);
    }
  }


  /**
   * Layout for the VIEWS table.
   */
  static public class Views extends FixedTable {
    static final String tableName = "VIEWS";
    static final String[] fieldNames = {"TABLE_CATALOG", "TABLE_SHEMA", "TABLE_NAME", "VIEW_DEFINITION"};
    static final MajorType[] fieldTypes = {VARCHAR,         VARCHAR,       VARCHAR,      VARCHAR};

    Views() {
      super(tableName, fieldNames, fieldTypes);
    }

    // Optional ...
    public boolean writeRowToVectors(int index, Object[] row) {
      return setSafe((VarCharVector)vectors.get(0), index, (String)row[0]) &&
          setSafe((VarCharVector)vectors.get(1), index, (String)row[1])    &&
          setSafe((VarCharVector)vectors.get(2), index, (String)row[2])    &&
          setSafe((VarCharVector)vectors.get(3), index, (String)row[3]);
    }
  }


  /**
   * Layout for the CATALOGS table.
   */
  static public class Catalogs extends FixedTable {
    static final String tableName = "CATALOGS";
    static final String[] fieldNames = {"CATALOG_NAME", "CATALOG_DESCRIPTION", "CATALOG_CONNECT"};
    static final MajorType[] fieldTypes = {VARCHAR,      VARCHAR,               VARCHAR};

    Catalogs() {
      super(tableName, fieldNames, fieldTypes);
    }
  }
  
  
}
