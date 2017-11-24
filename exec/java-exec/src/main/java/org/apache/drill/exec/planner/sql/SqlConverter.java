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
package org.apache.drill.exec.planner.sql;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import com.google.common.base.Strings;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.util.Util;
import org.apache.commons.collections.ListUtils;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.ops.UdfUtilities;
import org.apache.drill.exec.planner.cost.DrillCostBase;
import org.apache.drill.exec.planner.logical.DrillConstExecutor;
import org.apache.drill.exec.planner.physical.DrillDistributionTraitDef;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.rpc.user.UserSession;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

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
  private final DrillCalciteCatalogReader catalog;
  private final PlannerSettings settings;
  private final SchemaPlus rootSchema;
  private final SchemaPlus defaultSchema;
  private final SqlOperatorTable opTab;
  private final RelOptCostFactory costFactory;
  private final DrillValidator validator;
  private final boolean isInnerQuery;
  private final UdfUtilities util;
  private final FunctionImplementationRegistry functions;
  private final String temporarySchema;
  private final UserSession session;
  private final DrillConfig drillConfig;

  private String sql;
  private VolcanoPlanner planner;


  public SqlConverter(QueryContext context) {
    this.settings = context.getPlannerSettings();
    this.util = context;
    this.functions = context.getFunctionRegistry();
    this.parserConfig = new DrillParserConfig(settings);
    this.sqlToRelConverterConfig = new SqlToRelConverterConfig();
    this.isInnerQuery = false;
    this.typeFactory = new JavaTypeFactoryImpl(DRILL_TYPE_SYSTEM);
    this.defaultSchema =  context.getNewDefaultSchema();
    this.rootSchema = rootSchema(defaultSchema);
    this.temporarySchema = context.getConfig().getString(ExecConstants.DEFAULT_TEMPORARY_WORKSPACE);
    this.session = context.getSession();
    this.drillConfig = context.getConfig();
    this.catalog = new DrillCalciteCatalogReader(
        rootSchema,
        parserConfig.caseSensitive(),
        DynamicSchema.from(defaultSchema).path(null),
        typeFactory,
        drillConfig,
        session);
    this.opTab = new ChainedSqlOperatorTable(Arrays.asList(context.getDrillOperatorTable(), catalog));
    this.costFactory = (settings.useDefaultCosting()) ? null : new DrillCostBase.DrillCostFactory();
    this.validator = new DrillValidator(opTab, catalog, typeFactory, SqlConformance.DEFAULT);
    validator.setIdentifierExpansion(true);
  }

  private SqlConverter(SqlConverter parent, SchemaPlus defaultSchema, SchemaPlus rootSchema,
      DrillCalciteCatalogReader catalog) {
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
    this.temporarySchema = parent.temporarySchema;
    this.session = parent.session;
    this.drillConfig = parent.drillConfig;
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

  /** Disallow temporary tables presence in sql statement (ex: in view definitions) */
  public void disallowTemporaryTables() {
    catalog.disallowTemporaryTables();
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
        return Types.MAX_VARCHAR_LENGTH;
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

    @Override
    public RelNode expandView(RelDataType rowType, String queryString, List<String> schemaPath) {
      final DrillCalciteCatalogReader catalogReader = new DrillCalciteCatalogReader(
          rootSchema,
          parserConfig.caseSensitive(),
          schemaPath,
          typeFactory,
          drillConfig,
          session);
      final SqlConverter parser = new SqlConverter(SqlConverter.this, defaultSchema, rootSchema, catalogReader);
      return expandView(queryString, parser);
    }

    @Override
    public RelNode expandView(RelDataType rowType, String queryString, SchemaPlus rootSchema, List<String> schemaPath) {
      final DrillCalciteCatalogReader catalogReader = new DrillCalciteCatalogReader(
          rootSchema,
          parserConfig.caseSensitive(),
          schemaPath,
          typeFactory,
          drillConfig,
          session);
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
      converter.disallowTemporaryTables();
      final SqlNode parsedNode = converter.parse(queryString);
      final SqlNode validatedNode = converter.validate(parsedNode);
      return converter.toRel(validatedNode);
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
    if (pos == null) {
      return sql;
    }
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

  /**
   * Extension of {@link CalciteCatalogReader} to add ability to check for temporary tables first
   * if schema is not indicated near table name during query parsing
   * or indicated workspace is default temporary workspace.
   */
  private class DrillCalciteCatalogReader extends CalciteCatalogReader {

    private final DrillConfig drillConfig;
    private final UserSession session;
    private boolean allowTemporaryTables;
    private final SchemaPlus rootSchema;


    DrillCalciteCatalogReader(SchemaPlus rootSchema,
                              boolean caseSensitive,
                              List<String> defaultSchema,
                              JavaTypeFactory typeFactory,
                              DrillConfig drillConfig,
                              UserSession session) {
      super(DynamicSchema.from(rootSchema), caseSensitive, defaultSchema, typeFactory);
      this.drillConfig = drillConfig;
      this.session = session;
      this.allowTemporaryTables = true;
      this.rootSchema = rootSchema;
    }

    /**
     * Disallow temporary tables presence in sql statement (ex: in view definitions)
     */
    public void disallowTemporaryTables() {
      this.allowTemporaryTables = false;
    }

    /**
     * If schema is not indicated (only one element in the list) or schema is default temporary workspace,
     * we need to check among session temporary tables in default temporary workspace first.
     * If temporary table is found and temporary tables usage is allowed, its table instance will be returned,
     * otherwise search will be conducted in original workspace.
     *
     * @param names list of schema and table names, table name is always the last element
     * @return table instance, null otherwise
     * @throws UserException if temporary tables usage is disallowed
     */
    @Override
    public RelOptTableImpl getTable(final List<String> names) {
      RelOptTableImpl temporaryTable = null;

      if (mightBeTemporaryTable(names, session.getDefaultSchemaPath(), drillConfig)) {
        String temporaryTableName = session.resolveTemporaryTableName(names.get(names.size() - 1));
        if (temporaryTableName != null) {
          List<String> temporaryNames = Lists.newArrayList(temporarySchema, temporaryTableName);
          temporaryTable = super.getTable(temporaryNames);
        }
      }
      if (temporaryTable != null) {
        if (allowTemporaryTables) {
          return temporaryTable;
        }
        throw UserException
            .validationError()
            .message("Temporary tables usage is disallowed. Used temporary table name: %s.", names)
            .build(logger);
      }

      RelOptTableImpl table = super.getTable(names);

      // Check the schema and throw a valid SchemaNotFound exception instead of TableNotFound exception.
      if (table == null) {
        isValidSchema(names);
      }

      return table;
    }

    /**
     * check if the schema provided is a valid schema:
     * <li>schema is not indicated (only one element in the names list)<li/>
     *
     * @param names             list of schema and table names, table name is always the last element
     * @return throws a userexception if the schema is not valid.
     */
    private void isValidSchema(final List<String> names) throws UserException {
      SchemaPlus defaultSchema = session.getDefaultSchema(this.rootSchema);
      String defaultSchemaCombinedPath = SchemaUtilites.getSchemaPath(defaultSchema);
      List<String> schemaPath = Util.skipLast(names);
      String schemaPathCombined = SchemaUtilites.getSchemaPath(schemaPath);
      String commonPrefix = SchemaUtilites.getPrefixSchemaPath(defaultSchemaCombinedPath,
              schemaPathCombined,
              parserConfig.caseSensitive());
      boolean isPrefixDefaultPath = commonPrefix.length() == defaultSchemaCombinedPath.length();
      List<String> fullSchemaPath = Strings.isNullOrEmpty(defaultSchemaCombinedPath) ? schemaPath :
              isPrefixDefaultPath ? schemaPath : ListUtils.union(SchemaUtilites.getSchemaPathAsList(defaultSchema), schemaPath);
      if (names.size() > 1 && (SchemaUtilites.findSchema(this.rootSchema, fullSchemaPath) == null &&
              SchemaUtilites.findSchema(this.rootSchema, schemaPath) == null)) {
        SchemaUtilites.throwSchemaNotFoundException(defaultSchema, schemaPath);
      }
    }

    /**
     * We should check if passed table is temporary or not if:
     * <li>schema is not indicated (only one element in the names list)<li/>
     * <li>current schema or indicated schema is default temporary workspace<li/>
     *
     * Examples (where dfs.tmp is default temporary workspace):
     * <li>select * from t<li/>
     * <li>select * from dfs.tmp.t<li/>
     * <li>use dfs; select * from tmp.t<li/>
     *
     * @param names             list of schema and table names, table name is always the last element
     * @param defaultSchemaPath current schema path set using USE command
     * @param drillConfig       drill config
     * @return true if check for temporary table should be done, false otherwise
     */
    private boolean mightBeTemporaryTable(List<String> names, String defaultSchemaPath, DrillConfig drillConfig) {
      if (names.size() == 1) {
        return true;
      }

      String schemaPath = SchemaUtilites.getSchemaPath(names.subList(0, names.size() - 1));
      return SchemaUtilites.isTemporaryWorkspace(schemaPath, drillConfig) ||
          SchemaUtilites.isTemporaryWorkspace(
              SchemaUtilites.SCHEMA_PATH_JOINER.join(defaultSchemaPath, schemaPath), drillConfig);
    }
  }
}
