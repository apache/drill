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

import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.Table;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.sql.type.SqlTypeFactoryImpl;
import org.eigenbase.sql.type.SqlTypeName;

/**
 * OptiqProvider provides data for the various tables in the information schema.
 * Each table has its own nested class, keeping them grouped together.
 * Note "writeRow(...)" must match the values expected by the corresponding table.
 * <p>
 * To keep code concise, each class inherits from both an OptiqScanner
 * and a PipeProvider. Double inheritance is a problem in Java, so this
 * code needs to be cleaned up. For the moment, OptiqScanner artificially
 * inherits from PipeProvider.
 */
public class OptiqProvider  {

  /**
   * Provide data for TABLES table.
   */
  static public class Tables extends Abstract { 
    Tables(SchemaPlus root) {
      super(root);
    }

    @Override
    public boolean visitTableName(String schema, String tableName) {
      return writeRow("DRILL", schema, tableName, Schema.TableType.TABLE.toString());
    }
  }


  /**
   * Provide data for SCHEMATA table.
   */
  static public class Schemata extends Abstract {
    @Override
    public boolean visitSchema(String schemaName, Schema schema) {
      if (schemaName != null && schemaName != "") {
          writeRow("DRILL", schemaName, "<owner>");
      }
      return false;
    }

    Schemata(SchemaPlus root) {
      super(root);
    }
  }

  

  /**
   * Provide data for COLUMNS data.
   */
  static public class Columns extends Abstract {

    public Columns(SchemaPlus root) {
      super(root);
    }

    @Override
    public boolean visitField(String schemaName, String tableName, RelDataTypeField field) {
      String columnName = field.getName();
      RelDataType type = field.getType();
      SqlTypeName sqlType = type.getSqlTypeName();
      
      int position = field.getIndex();
      String nullable;
      if (type.isNullable()) nullable = "YES";
      else                   nullable = "NO";
      String sqlTypeName = sqlType.getName();
      int radix = (sqlType == SqlTypeName.DECIMAL)?10:-1;        // TODO: where do we get radix?
      int charMaxLen = -1;  // TODO: where do we get char length?
      int scale = (sqlType.allowsPrec())?type.getScale(): -1;
      int precision = (sqlType.allowsScale())?type.getPrecision(): -1;

      writeRow("DRILL", schemaName, tableName, columnName, position, nullable, sqlTypeName, charMaxLen, radix, scale, precision);

      return false;
    }
  }



  /**
   * Provide data for VIEWS table
   */
  public static class Views extends Abstract {
    public Views(SchemaPlus root) {
      super(root);
    }
    @Override
    public boolean visitTable(String schemaName, String tableName, Table table) {
      if (table.getJdbcTableType() == Schema.TableType.VIEW) {
        writeRow("DRILL", schemaName, tableName, "TODO: GetViewDefinition");
      }
      return false;
    }
  }

  public static class Catalogs extends Abstract {
    public Catalogs(SchemaPlus root) {
      super(root);
    }
    @Override
    public void generateRows() {
      writeRow("DRILL", "The internal metadata used by Drill", "");
    }
  }


  /**
   * An abstract class which helps generate data. It does the actual scanning of an Optiq schema,
   * but relies on a subclass to provide a "visit" routine to write out data.
   */
  public static class Abstract extends OptiqScanner {
    SchemaPlus root;

    protected Abstract(SchemaPlus root) {
      this.root = root;
    }


    /**
     * Start writing out rows.
     */
    @Override
    public void generateRows() {

      // Scan the root schema for subschema, tables, columns.
      scanSchema(root); 
    }
  }



  /**
   * An OptiqScanner scans the Optiq schema, generating rows for each 
   * schema, table or column. It is intended to be subclassed, where the
   * subclass does what it needs when visiting a Optiq schema structure.
   */
  // We would really prefer multiple inheritance from both OptiqScanner and PipeProvider,
  //   but making one a subclass of the other works for now. 
  //   TODO: Refactor to avoid subclassing of what should be an unrelated class.
  abstract static class OptiqScanner extends PipeProvider {


    /**
     *  Each visitor implements at least one of the the following methods.
     *    If the schema visitor returns true, then visit the tables.
     *    If the table visitor returns true, then visit the fields (columns).
     */
    public boolean visitSchema(String schemaName, Schema schema){return true;}
    public boolean visitTableName(String schemaName, String tableName){return true;}
    public boolean visitTable(String schemaName, String tableName, Table table){return true;}
    public boolean visitField(String schemaName, String tableName, RelDataTypeField field){return true;}



    /**
     * Start scanning an Optiq Schema.
     * @param root - where to start
     */
    protected void scanSchema(Schema root) {
      scanSchema(root.getName(), root);
    }
    
    /**
     * Recursively scan the schema, invoking the visitor as appropriate.
     * @param schemaPath - the path to the current schema, so far,
     * @param schema - the current schema.
     * @param visitor - the methods to invoke at each entity in the schema.
     */
    private void scanSchema(String schemaPath, Schema schema) {
      
      // If we have an empty schema path, then don't insert a leading dot.
      String separator;
      if (schemaPath == "") separator = "";
      else                  separator = ".";

      // Recursively scan the subschema.
      for (String name: schema.getSubSchemaNames()) {
        scanSchema(schemaPath + separator + name, schema.getSubSchema(name));
      }

      // Visit this schema and if requested ...
      if (visitSchema(schemaPath, schema)) {

        // ... do for each of the schema's tables.
        for (String tableName: schema.getTableNames()) {
          if(visitTableName(schemaPath, tableName)){
            Table table = schema.getTable(tableName);
            
            // Visit the table, and if requested ...
            if (visitTable(schemaPath,  tableName, table)) {

              // ... do for each of the table's fields.
              RelDataType tableRow = table.getRowType(new SqlTypeFactoryImpl()); // TODO: Is this correct?
              for (RelDataTypeField field: tableRow.getFieldList()) {

                // Visit the field.
                visitField(schemaPath,  tableName, field);
              }
            }            
          }
        }
      }
    }
  }
}

