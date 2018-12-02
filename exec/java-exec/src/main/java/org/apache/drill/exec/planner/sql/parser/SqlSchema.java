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
package org.apache.drill.exec.planner.sql.parser;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.drill.exec.planner.sql.handlers.AbstractSqlHandler;
import org.apache.drill.exec.planner.sql.handlers.SqlHandlerConfig;
import org.apache.drill.exec.planner.sql.handlers.SchemaHandler;
import org.apache.drill.exec.store.dfs.FileSelection;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Parent class for CREATE and DROP SCHEMA commands.
 * Holds logic common command property: table.
 */
public abstract class SqlSchema extends DrillSqlCall {

  protected final SqlIdentifier table;

  protected SqlSchema(SqlParserPos pos, SqlIdentifier table) {
    super(pos);
    this.table = table;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    if (table != null) {
      writer.keyword("FOR TABLE");
      table.unparse(writer, leftPrec, rightPrec);
    }
  }

  public boolean hasTable() {
    return table != null;
  }

  public SqlIdentifier getTable() {
    return table;
  }

  public List<String> getSchemaPath() {
    if (hasTable()) {
      return table.isSimple() ? Collections.emptyList() : table.names.subList(0, table.names.size() - 1);
    }
    return null;
  }

  public String getTableName() {
    if (hasTable()) {
      String tableName = table.isSimple() ? table.getSimple() : table.names.get(table.names.size() - 1);
      return FileSelection.removeLeadingSlash(tableName);
    }
    return null;
  }

  /**
   * Visits literal and returns bare value (i.e. single quotes).
   */
  private static class LiteralVisitor extends SqlBasicVisitor<String> {

    static final LiteralVisitor INSTANCE = new LiteralVisitor();

    @Override
    public String visit(SqlLiteral literal) {
      return literal.toValue();
    }

  }

  /**
   * CREATE SCHEMA sql call.
   */
  public static class Create extends SqlSchema {

    private final SqlCharStringLiteral schema;
    private final SqlNode load;
    private final SqlNode path;
    private final SqlNodeList properties;
    private final SqlLiteral createType;

    public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("CREATE_SCHEMA", SqlKind.OTHER_DDL) {
      @Override
      public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
        return new Create(pos, (SqlCharStringLiteral) operands[0], operands[1],
          (SqlIdentifier) operands[2], operands[3], (SqlNodeList) operands[4], (SqlLiteral) operands[5]);
      }
    };

    public Create(SqlParserPos pos,
                  SqlCharStringLiteral schema,
                  SqlNode load,
                  SqlIdentifier table,
                  SqlNode path,
                  SqlNodeList properties,
                  SqlLiteral createType) {
      super(pos, table);
      this.schema = schema;
      this.load = load;
      this.path = path;
      this.properties = properties;
      this.createType = createType;
    }

    @Override
    public SqlOperator getOperator() {
      return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
      return Arrays.asList(schema, load, table, path, properties, createType);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
      writer.keyword("CREATE");

      if (SqlCreateType.OR_REPLACE == getSqlCreateType()) {
        writer.keyword("OR");
        writer.keyword("REPLACE");
      }

      writer.keyword("SCHEMA");
      writer.literal(getSchema());

      super.unparse(writer, leftPrec, rightPrec);

      if (load != null) {
        writer.keyword("LOAD");
        load.unparse(writer, leftPrec, rightPrec);
      }

      if (path != null) {
        writer.keyword("PATH");
        path.unparse(writer, leftPrec, rightPrec);
      }

      if (properties != null) {
        writer.keyword("PROPERTIES");
        writer.keyword("(");

        for (int i = 1; i < properties.size(); i += 2) {
          if (i != 1) {
            writer.keyword(",");
          }
          properties.get(i - 1).unparse(writer, leftPrec, rightPrec);
          writer.keyword("=");
          properties.get(i).unparse(writer, leftPrec, rightPrec);
        }

        writer.keyword(")");
      }
    }

    @Override
    public AbstractSqlHandler getSqlHandler(SqlHandlerConfig config) {
      return new SchemaHandler.Create(config);
    }

    public boolean hasSchema() {
      return schema != null;
    }

    public String getSchema() {
      return hasSchema() ? schema.toValue() : null;
    }

    public String getLoad() {
      return load == null ? null : load.accept(LiteralVisitor.INSTANCE);
    }

    public String getPath() {
      return path == null ? null : path.accept(LiteralVisitor.INSTANCE);
    }

    public Map<String, String> getProperties() {
      if (properties == null) {
        return null;
      }

      // preserve properties order
      Map<String, String> map = new LinkedHashMap<>();
      for (int i = 1; i < properties.size(); i += 2) {
        map.put(properties.get(i - 1).accept(LiteralVisitor.INSTANCE),
          properties.get(i).accept(LiteralVisitor.INSTANCE));
      }
      return map;
    }

    public SqlCreateType getSqlCreateType() {
      return SqlCreateType.valueOf(createType.toValue());
    }

  }

  /**
   * DROP SCHEMA sql call.
   */
  public static class Drop extends SqlSchema {

    private final SqlLiteral existenceCheck;

    public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("DROP_SCHEMA", SqlKind.OTHER_DDL) {
      @Override
      public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
        return new Drop(pos, (SqlIdentifier) operands[0], (SqlLiteral) operands[1]);
      }
    };

    public Drop(SqlParserPos pos, SqlIdentifier table, SqlLiteral existenceCheck) {
      super(pos, table);
      this.existenceCheck = existenceCheck;
    }

    @Override
    public SqlOperator getOperator() {
      return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
      return Arrays.asList(table, existenceCheck);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
      writer.keyword("DROP");
      writer.keyword("SCHEMA");

      if (ifExists()) {
        writer.keyword("IF");
        writer.keyword("EXISTS");
      }

      super.unparse(writer, leftPrec, rightPrec);
    }

    @Override
    public AbstractSqlHandler getSqlHandler(SqlHandlerConfig config) {
      return new SchemaHandler.Drop(config);
    }

    public boolean ifExists() {
      return existenceCheck.booleanValue();
    }

  }

}
