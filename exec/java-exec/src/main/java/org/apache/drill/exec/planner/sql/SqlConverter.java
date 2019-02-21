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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.drill.shaded.guava.com.google.common.base.Strings;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.DynamicSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
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
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.RelBuilderFactory;
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
import org.apache.drill.exec.planner.logical.DrillRelFactories;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.planner.physical.DrillDistributionTraitDef;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.exec.store.dfs.FileSelection;
import static org.apache.calcite.util.Static.RESOURCE;

import org.apache.drill.shaded.guava.com.google.common.base.Joiner;
import org.apache.drill.exec.store.ColumnExplorer;
import org.apache.drill.exec.util.DecimalUtility;

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
  private RelOptCluster cluster;

  private VolcanoPlanner planner;
  private boolean useRootSchema = false;

  static {
    /*
     * Sets value to false to avoid simplifying project expressions
     * during creating new projects since it may cause changing data mode
     * which causes to assertion errors during type validation
     */
    Hook.REL_BUILDER_SIMPLIFY.add(Hook.propertyJ(false));
  }

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
    this.validator = new DrillValidator(opTab, catalog, typeFactory, parserConfig.conformance());
    validator.setIdentifierExpansion(true);
    cluster = null;
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
    this.validator = new DrillValidator(opTab, catalog, typeFactory, parserConfig.conformance());
    this.temporarySchema = parent.temporarySchema;
    this.session = parent.session;
    this.drillConfig = parent.drillConfig;
    validator.setIdentifierExpansion(true);
    this.cluster = parent.cluster;
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
      return validator.validate(parsedNode);
    } catch (RuntimeException e) {
      UserException.Builder builder = UserException
          .validationError(e);
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

  /**
   * Is root schema path should be used as default schema path.
   *
   * @param useRoot flag
   */
  public void useRootSchemaAsDefault(boolean useRoot) {
    useRootSchema = useRoot;
  }

  private class DrillValidator extends SqlValidatorImpl {

    DrillValidator(SqlOperatorTable opTab, SqlValidatorCatalogReader catalogReader,
        RelDataTypeFactory typeFactory, SqlConformance conformance) {
      super(opTab, catalogReader, typeFactory, conformance);
    }

    @Override
    protected void validateFrom(
        SqlNode node,
        RelDataType targetRowType,
        SqlValidatorScope scope) {
      switch (node.getKind()) {
        case AS:
          SqlNode sqlNode = ((SqlCall) node).operand(0);
          switch (sqlNode.getKind()) {
            case IDENTIFIER:
              SqlIdentifier tempNode = (SqlIdentifier) sqlNode;
              DrillCalciteCatalogReader catalogReader = (SqlConverter.DrillCalciteCatalogReader) getCatalogReader();

              changeNamesIfTableIsTemporary(tempNode);

              // Check the schema and throw a valid SchemaNotFound exception instead of TableNotFound exception.
              if (catalogReader.getTable(tempNode.names) == null) {
                catalogReader.isValidSchema(tempNode.names);
              }
              break;
            case UNNEST:
              if (((SqlCall) node).operandCount() < 3) {
                throw RESOURCE.validationError("Alias table and column name are required for UNNEST").ex();
              }
          }
      }
      super.validateFrom(node, targetRowType, scope);
    }

    @Override
    public String deriveAlias(
        SqlNode node,
        int ordinal) {
      if (node instanceof SqlIdentifier) {
        SqlIdentifier tempNode = ((SqlIdentifier) node);
        changeNamesIfTableIsTemporary(tempNode);
      }
      return SqlValidatorUtil.getAlias(node, ordinal);
    }

    /**
     * Checks that specified expression is not implicit column and
     * adds it to a select list, ensuring that its alias does not
     * clash with any existing expressions on the list.
     * <p>
     * This method may be used when {@link RelDataType#isDynamicStruct}
     * method returns false. Each column from table row type except
     * the implicit is added into specified list, aliases and fieldList.
     * In the opposite case when {@link RelDataType#isDynamicStruct}
     * returns true, only dynamic star is added into specified
     * list, aliases and fieldList.
     */
    @Override
    protected void addToSelectList(
        List<SqlNode> list,
        Set<String> aliases,
        List<Map.Entry<String, RelDataType>> fieldList,
        SqlNode exp,
        SqlValidatorScope scope,
        final boolean includeSystemVars) {
      if (!ColumnExplorer.initImplicitFileColumns(session.getOptions())
          .containsKey(SqlValidatorUtil.getAlias(exp, -1))) {
        super.addToSelectList(list, aliases, fieldList, exp, scope, includeSystemVars);
      }
    }

    private void changeNamesIfTableIsTemporary(SqlIdentifier tempNode) {
      List<String> temporaryTableNames = ((SqlConverter.DrillCalciteCatalogReader) getCatalogReader()).getTemporaryNames(tempNode.names);
      if (temporaryTableNames != null) {
        SqlParserPos pos = tempNode.getComponentParserPosition(0);
        List<SqlParserPos> poses = Lists.newArrayList();
        for (int i = 0; i < temporaryTableNames.size(); i++) {
          poses.add(i, pos);
        }
        tempNode.setNames(temporaryTableNames, poses);
      }
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

  public RelRoot toRel(final SqlNode validatedNode) {
    if (planner == null) {
      planner = new VolcanoPlanner(costFactory, settings);
      planner.setExecutor(new DrillConstExecutor(functions, util, settings));
      planner.clearRelTraitDefs();
      planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
      planner.addRelTraitDef(DrillDistributionTraitDef.INSTANCE);
      planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
    }

    if (cluster == null) {
      initCluster();
    }
    final SqlToRelConverter sqlToRelConverter =
        new SqlToRelConverter(new Expander(), validator, catalog, cluster, DrillConvertletTable.INSTANCE,
            sqlToRelConverterConfig);

    //To avoid unexpected column errors set a value of top to false
    final RelRoot rel = sqlToRelConverter.convertQuery(validatedNode, false, false);
    return rel.withRel(sqlToRelConverter.flattenTypes(rel.rel, true));
  }

  private class Expander implements RelOptTable.ViewExpander {

    @Override
    public RelRoot expandView(RelDataType rowType, String queryString, List<String> schemaPath, List<String> viewPath) {
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
    public RelRoot expandView(RelDataType rowType, String queryString, SchemaPlus rootSchema, List<String> schemaPath) {
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

    private RelRoot expandView(String queryString, SqlConverter converter) {
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
      return false;
    }

    @Override
    public int getInSubQueryThreshold() {
      return inSubqueryThreshold;
    }

    @Override
    public RelBuilderFactory getRelBuilderFactory() {
      return DrillRelFactories.LOGICAL_BUILDER;
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

  private void initCluster() {
    cluster = RelOptCluster.create(planner, new DrillRexBuilder(typeFactory));
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

    /**
     * Creates a call to the CAST operator, expanding if possible, and optionally
     * also preserving nullability.
     *
     * <p>Tries to expand the cast, and therefore the result may be something
     * other than a {@link org.apache.calcite.rex.RexCall} to the CAST operator, such as a
     * {@link RexLiteral} if {@code matchNullability} is false.
     *
     * @param type             Type to cast to
     * @param exp              Expression being cast
     * @param matchNullability Whether to ensure the result has the same
     *                         nullability as {@code type}
     * @return Call to CAST operator
     */
    @Override
    public RexNode makeCast(RelDataType type, RexNode exp, boolean matchNullability) {
      if (matchNullability) {
        return makeAbstractCast(type, exp);
      }
      // for the case when BigDecimal literal has a scale or precision
      // that differs from the value from specified RelDataType, cast cannot be removed
      // TODO: remove this code when CALCITE-1468 is fixed
      if (type.getSqlTypeName() == SqlTypeName.DECIMAL && exp instanceof RexLiteral) {
        if (type.getPrecision() < 1) {
          throw UserException.validationError()
              .message("Expected precision greater than 0, but was %s.", type.getPrecision())
              .build(logger);
        }
        if (type.getScale() > type.getPrecision()) {
          throw UserException.validationError()
              .message("Expected scale less than or equal to precision, " +
                  "but was scale %s and precision %s.", type.getScale(), type.getPrecision())
              .build(logger);
        }
        RexLiteral literal = (RexLiteral) exp;
        Comparable value = literal.getValueAs(Comparable.class);
        if (value instanceof BigDecimal) {
          BigDecimal bigDecimal = (BigDecimal) value;
          DecimalUtility.checkValueOverflow(bigDecimal, type.getPrecision(), type.getScale());
          if (bigDecimal.scale() != type.getScale() || bigDecimal.precision() != type.getPrecision()) {
            return makeAbstractCast(type, exp);
          }
        }
      }
      return super.makeCast(type, exp, false);
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
      super(DynamicSchema.from(rootSchema), defaultSchema,
          typeFactory, getConnectionConfig(caseSensitive));
      this.drillConfig = drillConfig;
      this.session = session;
      this.allowTemporaryTables = true;
      this.rootSchema = rootSchema;
    }

    /**
     * Disallow temporary tables presence in sql statement (ex: in view definitions)
     */
    void disallowTemporaryTables() {
      this.allowTemporaryTables = false;
    }

    private List<String> getTemporaryNames(List<String> names) {
      if (mightBeTemporaryTable(names, session.getDefaultSchemaPath(), drillConfig)) {
        String tableName = FileSelection.removeLeadingSlash(names.get(names.size() - 1));
        String temporaryTableName = session.resolveTemporaryTableName(tableName);
        if (temporaryTableName != null) {
          List<String> temporaryNames = new ArrayList<>(SchemaUtilites.getSchemaPathAsList(temporarySchema));
          temporaryNames.add(temporaryTableName);
          return temporaryNames;
        }
      }
      return null;
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
    public Prepare.PreparingTable getTable(final List<String> names) {
      String originalTableName = session.getOriginalTableNameFromTemporaryTable(names.get(names.size() - 1));
      if (originalTableName != null) {
        if (!allowTemporaryTables) {
          throw UserException
              .validationError()
              .message("Temporary tables usage is disallowed. Used temporary table name: [%s].", originalTableName)
              .build(logger);
        }
      }

      Prepare.PreparingTable table = super.getTable(names);
      DrillTable unwrap;
      // add session options if found table is Drill table
      if (table != null && (unwrap = table.unwrap(DrillTable.class)) != null) {
        unwrap.setOptions(session.getOptions());
      }
      return table;
    }

    @Override
    public List<List<String>> getSchemaPaths() {
      if (useRootSchema) {
        return ImmutableList.of(ImmutableList.of());
      }
      return super.getSchemaPaths();
    }

    /**
     * check if the schema provided is a valid schema:
     * <li>schema is not indicated (only one element in the names list)<li/>
     *
     * @param names list of schema and table names, table name is always the last element
     * @throws UserException if the schema is not valid.
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

  /**
   * Creates {@link CalciteConnectionConfigImpl} instance with specified caseSensitive property.
   *
   * @param caseSensitive is case sensitive.
   * @return {@link CalciteConnectionConfigImpl} instance
   */
  private static CalciteConnectionConfigImpl getConnectionConfig(boolean caseSensitive) {
    Properties properties = new Properties();
    properties.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(),
        String.valueOf(caseSensitive));
    return new CalciteConnectionConfigImpl(properties);
  }
}
