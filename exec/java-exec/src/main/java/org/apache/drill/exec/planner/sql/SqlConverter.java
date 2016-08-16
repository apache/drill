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
package org.apache.drill.exec.planner.sql;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Sets;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.jdbc.CalciteSchemaImpl;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql.validate.AggregatingSelectScope;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.ops.UdfUtilities;
import org.apache.drill.exec.planner.cost.DrillCostBase;
import org.apache.drill.exec.planner.logical.DrillConstExecutor;
import org.apache.drill.exec.planner.physical.DrillDistributionTraitDef;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.drill.exec.planner.sql.parser.impl.DrillParserWithCompoundIdConverter;

import com.google.common.base.Joiner;

/**
 * Class responsible for managing parsing, validation and toRel conversion for sql statements.
 */
public class SqlConverter {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SqlConverter.class);

  private static DrillTypeSystem DRILL_TYPE_SYSTEM = new DrillTypeSystem();

  private final JavaTypeFactory typeFactory;
  private final SqlParser.Config parserConfig;
  // Allow the default config to be modified using immutable configs
  private SqlToRelConverter.Config sqlToRelConverterConfig;
  private final CalciteCatalogReader catalog;
  private final PlannerSettings settings;
  private final SchemaPlus rootSchema;
  private final SchemaPlus defaultSchema;
  private final SqlOperatorTable opTab;
  private final RelOptCostFactory costFactory;
  private final DrillValidator validator;
  private final boolean isInnerQuery;
  private final UdfUtilities util;
  private final FunctionImplementationRegistry functions;

  private String sql;
  private VolcanoPlanner planner;


  public SqlConverter(PlannerSettings settings, SchemaPlus defaultSchema,
      final SqlOperatorTable operatorTable, UdfUtilities util, FunctionImplementationRegistry functions) {
    this.settings = settings;
    this.util = util;
    this.functions = functions;
    this.parserConfig = new ParserConfig();
    this.sqlToRelConverterConfig = new SqlToRelConverterConfig();
    this.isInnerQuery = false;
    this.typeFactory = new JavaTypeFactoryImpl(DRILL_TYPE_SYSTEM);
    this.defaultSchema = defaultSchema;
    this.rootSchema = rootSchema(defaultSchema);
    this.catalog = new CalciteCatalogReader(
        CalciteSchemaImpl.from(rootSchema),
        parserConfig.caseSensitive(),
        CalciteSchemaImpl.from(defaultSchema).path(null),
        typeFactory);
    this.opTab = new ChainedSqlOperatorTable(Arrays.asList(operatorTable, catalog));
    this.costFactory = (settings.useDefaultCosting()) ? null : new DrillCostBase.DrillCostFactory();
    this.validator = new DrillValidator(opTab, catalog, typeFactory, SqlConformance.DEFAULT);
    validator.setIdentifierExpansion(true);
  }

  private SqlConverter(SqlConverter parent, SchemaPlus defaultSchema, SchemaPlus rootSchema,
      CalciteCatalogReader catalog) {
    this.parserConfig = parent.parserConfig;
    this.sqlToRelConverterConfig = parent.sqlToRelConverterConfig;
    this.defaultSchema = defaultSchema;
    this.functions = parent.functions;
    this.util = parent.util;
    this.isInnerQuery = true;
    this.typeFactory = parent.typeFactory;
    this.costFactory = parent.costFactory;
    this.settings = parent.settings;
    this.rootSchema = rootSchema;
    this.catalog = catalog;
    this.opTab = parent.opTab;
    this.planner = parent.planner;
    this.validator = new DrillValidator(opTab, catalog, typeFactory, SqlConformance.DEFAULT);
    validator.setIdentifierExpansion(true);
  }


  public SqlNode parse(String sql) {
    try {
      SqlParser parser = SqlParser.create(sql, parserConfig);
      return parser.parseStmt();
    } catch (SqlParseException e) {
      UserException.Builder builder = UserException
          .parseError(e)
          .addContext("SQL Query", formatSQLParsingError(sql, e.getPos()));
      if (isInnerQuery) {
        builder.message("Failure parsing a view your query is dependent upon.");
      }
      throw builder.build(logger);
    }

  }

  public SqlNode validate(final SqlNode parsedNode) {
    try {
      SqlNode validatedNode = validator.validate(parsedNode);
      return validatedNode;
    } catch (RuntimeException e) {
      UserException.Builder builder = UserException
          .validationError(e)
          .addContext("SQL Query", sql);
      if (isInnerQuery) {
        builder.message("Failure validating a view your query is dependent upon.");
      }
      throw builder.build(logger);
    }
  }

  public RelDataType getOutputType(SqlNode validatedNode) {
    return validator.getValidatedNodeType(validatedNode);
  }

  public JavaTypeFactory getTypeFactory() {
    return typeFactory;
  }

  public SqlOperatorTable getOpTab() {
    return opTab;
  }

  public RelOptCostFactory getCostFactory() {
    return costFactory;
  }

  public SchemaPlus getRootSchema() {
    return rootSchema;
  }

  public SchemaPlus getDefaultSchema() {
    return defaultSchema;
  }

  private class DrillValidator extends SqlValidatorImpl {
    private final Set<SqlValidatorScope> identitySet = Sets.newIdentityHashSet();

    protected DrillValidator(SqlOperatorTable opTab, SqlValidatorCatalogReader catalogReader,
        RelDataTypeFactory typeFactory, SqlConformance conformance) {
      super(opTab, catalogReader, typeFactory, conformance);
    }
  }

  private static class DrillTypeSystem extends RelDataTypeSystemImpl {

    @Override
    public int getDefaultPrecision(SqlTypeName typeName) {
      switch (typeName) {
      case CHAR:
      case BINARY:
      case VARCHAR:
      case VARBINARY:
        return 65536;
      default:
        return super.getDefaultPrecision(typeName);
      }
    }

    @Override
    public int getMaxNumericScale() {
      return 38;
    }

    @Override
    public int getMaxNumericPrecision() {
      return 38;
    }

    @Override
    public boolean isSchemaCaseSensitive() {
      // Drill uses case-insensitive and case-preserve policy
      return false;
    }
  }

  public RelNode toRel(
      final SqlNode validatedNode) {
    final RexBuilder rexBuilder = new DrillRexBuilder(typeFactory);
    if (planner == null) {
      planner = new VolcanoPlanner(costFactory, settings);
      planner.setExecutor(new DrillConstExecutor(functions, util, settings));
      planner.clearRelTraitDefs();
      planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
      planner.addRelTraitDef(DrillDistributionTraitDef.INSTANCE);
      planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
    }

    final RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);
    final SqlToRelConverter sqlToRelConverter =
        new SqlToRelConverter(new Expander(), validator, catalog, cluster, DrillConvertletTable.INSTANCE,
            sqlToRelConverterConfig);
    final RelNode rel = sqlToRelConverter.convertQuery(validatedNode, false, !isInnerQuery);
    final RelNode rel2 = sqlToRelConverter.flattenTypes(rel, true);
    final RelNode rel3 = RelDecorrelator.decorrelateQuery(rel2);
    return rel3;

  }

  private class Expander implements RelOptTable.ViewExpander {

    public Expander() {
    }

    public RelNode expandView(
        RelDataType rowType,
        String queryString,
        List<String> schemaPath) {
      SqlConverter parser = new SqlConverter(SqlConverter.this, defaultSchema, rootSchema,
          catalog.withSchemaPath(schemaPath));
      return expandView(queryString, parser);
    }

    @Override
    public RelNode expandView(
        RelDataType rowType,
        String queryString,
        SchemaPlus rootSchema, // new root schema
        List<String> schemaPath) {
      final CalciteCatalogReader catalogReader = new CalciteCatalogReader(
          CalciteSchemaImpl.from(rootSchema),
          parserConfig.caseSensitive(),
          schemaPath,
          typeFactory);
      SchemaPlus schema = rootSchema;
      for (String s : schemaPath) {
        SchemaPlus newSchema = schema.getSubSchema(s);

        if (newSchema == null) {
          throw UserException
              .validationError()
              .message(
              "Failure while attempting to expand view. Requested schema %s not available in schema %s.", s,
                  schema.getName())
              .addContext("View Context", Joiner.on(", ").join(schemaPath))
              .addContext("View SQL", queryString)
              .build(logger);
        }

        schema = newSchema;
      }
      SqlConverter parser = new SqlConverter(SqlConverter.this, schema, rootSchema, catalogReader);
      return expandView(queryString, parser);
    }

    private RelNode expandView(String queryString, SqlConverter converter) {
      final SqlNode parsedNode = converter.parse(queryString);
      final SqlNode validatedNode = converter.validate(parsedNode);
      return converter.toRel(validatedNode);
    }

  }

  private class ParserConfig implements SqlParser.Config {

    final long identifierMaxLength = settings.getIdentifierMaxLength();

    @Override
    public int identifierMaxLength() {
      return (int) identifierMaxLength;
    }

    @Override
    public Casing quotedCasing() {
      return Casing.UNCHANGED;
    }

    @Override
    public Casing unquotedCasing() {
      return Casing.UNCHANGED;
    }

    @Override
    public Quoting quoting() {
      return Quoting.BACK_TICK;
    }

    @Override
    public boolean caseSensitive() {
      return false;
    }

    @Override
    public SqlParserImplFactory parserFactory() {
      return DrillParserWithCompoundIdConverter.FACTORY;
    }

  }

  private class SqlToRelConverterConfig implements SqlToRelConverter.Config {

    final int inSubqueryThreshold = (int)settings.getInSubqueryThreshold();

    @Override
    public boolean isConvertTableAccess() {
      return false;
    }

    @Override
    public boolean isDecorrelationEnabled() {
      return SqlToRelConverterConfig.DEFAULT.isDecorrelationEnabled();
    }

    @Override
    public boolean isTrimUnusedFields() {
      return false;
    }

    @Override
    public boolean isCreateValuesRel() {
      return SqlToRelConverterConfig.DEFAULT.isCreateValuesRel();
    }

    @Override
    public boolean isExplain() {
      return SqlToRelConverterConfig.DEFAULT.isExplain();
    }

    @Override
    public boolean isExpand() {
      return SqlToRelConverterConfig.DEFAULT.isExpand();
    }

    @Override
    public int getInSubqueryThreshold() {
      return inSubqueryThreshold;
    }
  }

  /**
   *
   * @param sql
   *          the SQL sent to the server
   * @param pos
   *          the position of the error
   * @return The sql with a ^ character under the error
   */
  static String formatSQLParsingError(String sql, SqlParserPos pos) {
    StringBuilder sb = new StringBuilder();
    String[] lines = sql.split("\n");
    for (int i = 0; i < lines.length; i++) {
      String line = lines[i];
      sb.append(line).append("\n");
      if (i == (pos.getLineNum() - 1)) {
        for (int j = 0; j < pos.getColumnNum() - 1; j++) {
          sb.append(" ");
        }
        sb.append("^\n");
      }
    }
    return sb.toString();
  }

  private static SchemaPlus rootSchema(SchemaPlus schema) {
    while (true) {
      if (schema.getParentSchema() == null) {
        return schema;
      }
      schema = schema.getParentSchema();
    }
  }

  private static class DrillRexBuilder extends RexBuilder {
    private DrillRexBuilder(RelDataTypeFactory typeFactory) {
      super(typeFactory);
    }

    /**
     * Since Drill has different mechanism and rules for implicit casting,
     * ensureType() is overridden to avoid conflicting cast functions being added to the expressions.
     */
    @Override
    public RexNode ensureType(
        RelDataType type,
        RexNode node,
        boolean matchNullability) {
      return node;
    }
  }
}
