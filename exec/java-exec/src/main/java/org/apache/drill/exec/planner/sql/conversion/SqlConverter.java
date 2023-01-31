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
package org.apache.drill.exec.planner.sql.conversion;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.jdbc.DynamicSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.planner.DrillRelBuilder;
import org.apache.drill.exec.planner.cost.DrillCostBase;
import org.apache.drill.exec.planner.logical.DrillConstExecutor;
import org.apache.drill.exec.planner.logical.DrillRelFactories;
import org.apache.drill.exec.planner.physical.DrillDistributionTraitDef;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.planner.sql.DrillConformance;
import org.apache.drill.exec.planner.sql.DrillConvertletTable;
import org.apache.drill.exec.planner.sql.SchemaUtilities;
import org.apache.drill.exec.planner.sql.parser.impl.DrillParserWithCompoundIdConverter;
import org.apache.drill.exec.planner.sql.parser.impl.DrillSqlParseException;
import org.apache.drill.exec.planner.types.DrillRelDataTypeSystem;
import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.exec.util.ImpersonationUtil;
import org.apache.drill.exec.util.Utilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Class responsible for managing:
 * <ul>
 *   <li>parsing - {@link #parse(String)}<li/>
 *   <li>validation - {@link #validate(SqlNode)}<li/>
 *   <li>conversion to rel - {@link #toRel(SqlNode)} (String)}<li/>
 * <ul/>
 */
public class SqlConverter {
  private static final Logger logger = LoggerFactory.getLogger(SqlConverter.class);
  public final static SqlConformance DRILL_CONFORMANCE = new DrillConformance();

  private final JavaTypeFactory typeFactory;
  private final SqlParser.Config parserConfig;
  private final DrillCalciteCatalogReader catalog;
  private final PlannerSettings settings;
  private final SchemaPlus rootSchema;
  private final SchemaPlus defaultSchema;
  private final SqlOperatorTable opTab;
  private final RelOptCostFactory costFactory;
  private final SqlValidator validator;
  private final boolean isInnerQuery;
  private final boolean isExpandedView;
  private final QueryContext util;
  private final FunctionImplementationRegistry functions;
  private final String temporarySchema;
  private final UserSession session;
  private final DrillConfig drillConfig;
  // Allow the default config to be modified using immutable configs
  private final SqlToRelConverter.Config sqlToRelConverterConfig;
  private RelOptCluster cluster;
  private VolcanoPlanner planner;
  private boolean useRootSchema = false;
  private final boolean isImpersonationEnabled;

  public SqlConverter(QueryContext context) {
    this.settings = context.getPlannerSettings();
    this.util = context;
    this.functions = context.getFunctionRegistry();
    this.parserConfig = SqlParser.Config.DEFAULT
      .withIdentifierMaxLength((int) settings.getIdentifierMaxLength())
      .withQuoting(settings.getQuotingIdentifiers())
      .withParserFactory(DrillParserWithCompoundIdConverter.FACTORY)
      .withCaseSensitive(false)
      .withConformance(DRILL_CONFORMANCE)
      .withUnquotedCasing(Casing.UNCHANGED)
      .withQuotedCasing(Casing.UNCHANGED);
    this.sqlToRelConverterConfig = SqlToRelConverter.config()
        .withInSubQueryThreshold((int) settings.getInSubqueryThreshold())
        .withRemoveSortInSubQuery(false)
        .withRelBuilderConfigTransform(t -> t
          .withSimplify(false)
          .withAggregateUnique(true)
          .withPruneInputOfAggregate(false)
          .withDedupAggregateCalls(false)
          .withSimplifyLimit(false)
          .withBloat(DrillRelBuilder.DISABLE_MERGE_PROJECT)
          .withSimplifyValues(false))
        .withExpand(false)
        .withRelBuilderFactory(DrillRelFactories.LOGICAL_BUILDER);
    this.isInnerQuery = false;
    this.isExpandedView = false;
    this.typeFactory = new JavaTypeFactoryImpl(DrillRelDataTypeSystem.DRILL_REL_DATATYPE_SYSTEM);
    this.defaultSchema = context.getNewDefaultSchema();
    this.rootSchema = SchemaUtilities.rootSchema(defaultSchema);
    this.temporarySchema = context.getConfig().getString(ExecConstants.DEFAULT_TEMPORARY_WORKSPACE);
    this.session = context.getSession();
    this.drillConfig = context.getConfig();
    this.catalog = new DrillCalciteCatalogReader(
        rootSchema,
        parserConfig.caseSensitive(),
        DynamicSchema.from(defaultSchema).path(null),
        typeFactory,
        drillConfig,
        session,
        this::useRootSchema
    );
    this.opTab = new ChainedSqlOperatorTable(Arrays.asList(context.getDrillOperatorTable(), catalog));
    this.costFactory = (settings.useDefaultCosting()) ? null : new DrillCostBase.DrillCostFactory();
    this.validator = SqlValidatorUtil.newValidator(opTab, catalog, typeFactory,
        SqlValidator.Config.DEFAULT.withConformance(parserConfig.conformance())
          .withTypeCoercionEnabled(true)
          .withIdentifierExpansion(true));
    this.isImpersonationEnabled = context.isImpersonationEnabled();
    cluster = null;
  }

  SqlConverter(SqlConverter parent, SchemaPlus defaultSchema, SchemaPlus rootSchema,
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
    this.validator = SqlValidatorUtil.newValidator(opTab, catalog, typeFactory,
      SqlValidator.Config.DEFAULT.withConformance(parserConfig.conformance())
        .withTypeCoercionEnabled(true)
        .withIdentifierExpansion(true));
    this.temporarySchema = parent.temporarySchema;
    this.session = parent.session;
    this.drillConfig = parent.drillConfig;
    this.cluster = parent.cluster;
    this.isImpersonationEnabled = util.isImpersonationEnabled();
  }

  public SqlNode parse(String sql) {
    try {
      SqlParser parser = SqlParser.create(sql, parserConfig);
      return parser.parseStmt();
    } catch (SqlParseException parseError) {
      DrillSqlParseException dex = new DrillSqlParseException(sql, parseError);
      UserException.Builder builder = UserException
          .parseError(dex)
          .addContext(dex.getSqlWithErrorPointer());
      if (isInnerQuery) {
        builder.message("Failure parsing a view your query is dependent upon.");
      }
      throw builder.build(logger);
    }
  }

  public SqlNode validate(final SqlNode parsedNode) {
    try {
      if (isImpersonationEnabled) {
        return ImpersonationUtil.getProcessUserUGI().doAs(
          (PrivilegedAction<SqlNode>) () -> validator.validate(parsedNode));
      } else {
        return validator.validate(parsedNode);
      }
    } catch (RuntimeException e) {
      UserException.Builder builder = UserException
          .validationError(e);
      if (isInnerQuery) {
        builder.message("Failure validating a view your query is dependent upon.");
      }
      throw builder.build(logger);
    }
  }

  public RelRoot toRel(final SqlNode validatedNode) {
    initCluster(initPlanner());
    DrillViewExpander viewExpander = new DrillViewExpander(this);
    util.getViewExpansionContext().setViewExpander(viewExpander);
    final SqlToRelConverter sqlToRelConverter = new SqlToRelConverter(
        viewExpander, validator, catalog, cluster,
        DrillConvertletTable.INSTANCE, sqlToRelConverterConfig);

    boolean topLevelQuery = !isInnerQuery || isExpandedView;
    RelRoot rel = sqlToRelConverter.convertQuery(validatedNode, false, topLevelQuery);

    // If extra expressions used in ORDER BY were added to the project list,
    // add another project to remove them.
    if (topLevelQuery && rel.rel.getRowType().getFieldCount() - rel.fields.size() > 0) {
      RexBuilder builder = rel.rel.getCluster().getRexBuilder();

      RelNode relNode = rel.rel;
      List<RexNode> expressions = rel.fields.stream()
          .map(f -> builder.makeInputRef(relNode, f.left))
          .collect(Collectors.toList());

      RelNode project = LogicalProject.create(rel.rel, Collections.emptyList(), expressions, rel.validatedRowType);
      rel = RelRoot.of(project, rel.validatedRowType, rel.kind);
    }
    return rel.withRel(sqlToRelConverter.flattenTypes(rel.rel, true));
  }

  public RelDataType getOutputType(SqlNode validatedNode) {
    return validator.getValidatedNodeType(validatedNode);
  }

  public SqlValidator getValidator() {
    return validator;
  }

  public JavaTypeFactory getTypeFactory() {
    return typeFactory;
  }

  public DrillConfig getDrillConfig() {
    return drillConfig;
  }

  public UserSession getSession() {
    return session;
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

  public boolean isCaseSensitive() {
    return parserConfig.caseSensitive();
  }

  /** Disallow temporary tables presence in sql statement (ex: in view definitions) */
  public void disallowTemporaryTables() {
    catalog.disallowTemporaryTables();
  }

  String getTemporarySchema() {
    return temporarySchema;
  }

  boolean useRootSchema() {
    return this.useRootSchema;
  }

  /**
   * Is root schema path should be used as default schema path.
   *
   * @param useRoot flag
   */
  public void useRootSchemaAsDefault(boolean useRoot) {
    useRootSchema = useRoot;
  }

  private void initCluster(RelOptPlanner planner) {
    if (cluster == null) {
      cluster = RelOptCluster.create(planner, new DrillRexBuilder(typeFactory));
      JaninoRelMetadataProvider relMetadataProvider = Utilities.registerJaninoRelMetadataProvider();
      cluster.setMetadataProvider(relMetadataProvider);
    }
  }

  private RelOptPlanner initPlanner() {
    if (planner == null) {
      planner = new VolcanoPlanner(costFactory, settings);
      planner.setExecutor(new DrillConstExecutor(functions, util, settings));
      planner.clearRelTraitDefs();
      planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
      planner.addRelTraitDef(DrillDistributionTraitDef.INSTANCE);
      planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
    }
    return planner;
  }
}
