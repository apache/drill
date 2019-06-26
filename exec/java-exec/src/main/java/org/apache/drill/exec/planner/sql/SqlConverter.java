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
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.util.Static;
import org.apache.commons.lang3.StringUtils;
import org.apache.drill.exec.planner.sql.parser.DrillParserUtil;
import org.apache.drill.exec.planner.sql.parser.impl.DrillSqlParseException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.metastore.MetadataProviderManager;
import org.apache.drill.exec.planner.types.DrillRelDataTypeSystem;
import org.apache.drill.metastore.metadata.TableMetadataProvider;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.drill.exec.util.Utilities;
import org.apache.drill.shaded.guava.com.google.common.cache.CacheBuilder;
import org.apache.drill.shaded.guava.com.google.common.cache.CacheLoader;
import org.apache.drill.shaded.guava.com.google.common.cache.LoadingCache;
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
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
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
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.UserException;
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

import org.apache.drill.shaded.guava.com.google.common.base.Joiner;
import org.apache.drill.exec.store.ColumnExplorer;
import org.apache.drill.exec.util.DecimalUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class responsible for managing parsing, validation and toRel conversion for sql statements.
 */
public class SqlConverter {
  private static final Logger logger = LoggerFactory.getLogger(SqlConverter.class);

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
  private final boolean isExpandedView;
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
    this.isExpandedView = false;
    this.typeFactory = new JavaTypeFactoryImpl(DrillRelDataTypeSystem.DRILL_REL_DATATYPE_SYSTEM);
    this.defaultSchema = context.getNewDefaultSchema();
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
    this.isExpandedView = true;
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
      DrillSqlParseException dex = new DrillSqlParseException(e);
      UserException.Builder builder = UserException
          .parseError(dex)
          .addContext(formatSQLParsingError(sql, dex));
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
      if (node.getKind() == SqlKind.AS) {
        SqlCall sqlCall = (SqlCall) node;
        SqlNode sqlNode = sqlCall.operand(0);
        switch (sqlNode.getKind()) {
          case IDENTIFIER:
            SqlIdentifier tempNode = (SqlIdentifier) sqlNode;
            DrillCalciteCatalogReader catalogReader = (DrillCalciteCatalogReader) getCatalogReader();

            changeNamesIfTableIsTemporary(tempNode);

            // Check the schema and throw a valid SchemaNotFound exception instead of TableNotFound exception.
            catalogReader.isValidSchema(tempNode.names);
            break;
          case UNNEST:
            if (sqlCall.operandCount() < 3) {
              throw Static.RESOURCE.validationError("Alias table and column name are required for UNNEST").ex();
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

    @Override
    protected void inferUnknownTypes(
        RelDataType inferredType,
        SqlValidatorScope scope,
        SqlNode node) {
      // calls validateQuery() for SqlSelect to be sure that temporary table name will be changed
      // for the case when it is used in sub-select
      if (node.getKind() == SqlKind.SELECT) {
        validateQuery(node, scope, inferredType);
      }
      super.inferUnknownTypes(inferredType, scope, node);
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

    RelRoot rel = sqlToRelConverter.convertQuery(validatedNode, false, !isInnerQuery || isExpandedView);

    // If extra expressions used in ORDER BY were added to the project list,
    // add another project to remove them.
    if ((!isInnerQuery || isExpandedView) && rel.rel.getRowType().getFieldCount() - rel.fields.size() > 0) {
      RexBuilder builder = rel.rel.getCluster().getRexBuilder();

      RelNode relNode = rel.rel;
      List<RexNode> expressions = rel.fields.stream()
          .map(f -> builder.makeInputRef(relNode, f.left))
          .collect(Collectors.toList());

      RelNode project = LogicalProject.create(rel.rel, expressions, rel.validatedRowType);
      rel = RelRoot.of(project, rel.validatedRowType, rel.kind);
    }
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
   * Formats sql exception with context name included and with
   * graphical representation for the {@link DrillSqlParseException}
   *
   * @param sql     the SQL sent to the server
   * @param ex      exception object
   * @return The sql with a ^ character under the error
   */
  static String formatSQLParsingError(String sql, DrillSqlParseException ex) {
    final String sqlErrorMessageHeader = "SQL Query: ";
    final SqlParserPos pos = ex.getPos();

    if (pos != null) {
      int issueLineNumber = pos.getLineNum() - 1;  // recalculates to base 0
      int issueColumnNumber = pos.getColumnNum() - 1;  // recalculates to base 0
      int messageHeaderLength = sqlErrorMessageHeader.length();

      // If the issue happens on the first line, header width should be calculated alongside with the sql query
      int shiftLength = (issueLineNumber == 0) ? issueColumnNumber + messageHeaderLength : issueColumnNumber;

      StringBuilder sb = new StringBuilder();
      String[] lines = sql.split(DrillParserUtil.EOL);

      for (int i = 0; i < lines.length; i++) {
        sb.append(lines[i]);

        if (i == issueLineNumber) {
          sb
              .append(DrillParserUtil.EOL)
              .append(StringUtils.repeat(' ', shiftLength))
              .append("^");
        }
        if (i < lines.length - 1) {
          sb.append(DrillParserUtil.EOL);
        }
      }
      sql = sb.toString();
    }
    return sqlErrorMessageHeader + sql;
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
    JaninoRelMetadataProvider relMetadataProvider = Utilities.registerJaninoRelMetadataProvider();

    cluster.setMetadataProvider(relMetadataProvider);
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
                  "but was precision %s and scale %s.", type.getPrecision(), type.getScale())
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
   * Key for storing / obtaining {@link TableMetadataProvider} instance from {@link LoadingCache}.
   */
  private static class DrillTableKey {
    private final SchemaPath key;
    private final DrillTable drillTable;

    public DrillTableKey(SchemaPath key, DrillTable drillTable) {
      this.key = key;
      this.drillTable = drillTable;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }

      DrillTableKey that = (DrillTableKey) obj;

      return Objects.equals(key, that.key);
    }

    @Override
    public int hashCode() {
      return key != null ? key.hashCode() : 0;
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

    private final LoadingCache<DrillTableKey, MetadataProviderManager> tableCache;

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
      this.tableCache =
          CacheBuilder.newBuilder()
            .build(new CacheLoader<DrillTableKey, MetadataProviderManager>() {
              @Override
              public MetadataProviderManager load(DrillTableKey key) {
                return key.drillTable.getMetadataProviderManager();
              }
            });
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
    public Prepare.PreparingTable getTable(List<String> names) {
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
      DrillTable drillTable;
      // add session options if found table is Drill table
      if (table != null && (drillTable = table.unwrap(DrillTable.class)) != null) {
        drillTable.setOptions(session.getOptions());

        drillTable.setTableMetadataProviderManager(tableCache.getUnchecked(
            new DrillTableKey(SchemaPath.getCompoundPath(names.toArray(new String[0])), drillTable)));
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
     * Checks if the schema provided is a valid schema:
     * <li>schema is not indicated (only one element in the names list)<li/>
     *
     * @param names list of schema and table names, table name is always the last element
     * @throws UserException if the schema is not valid.
     */
    private void isValidSchema(List<String> names) throws UserException {
      List<String> schemaPath = Util.skipLast(names);

      for (List<String> currentSchema : getSchemaPaths()) {
        List<String> fullSchemaPath = new ArrayList<>(currentSchema);
        fullSchemaPath.addAll(schemaPath);
        CalciteSchema schema = SqlValidatorUtil.getSchema(getRootSchema(),
            fullSchemaPath, nameMatcher());

        if (schema != null) {
         return;
        }
      }
      SchemaUtilites.throwSchemaNotFoundException(defaultSchema, schemaPath);
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
