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
package org.apache.drill.exec.store.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.sql.DataSource;

import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcRules;
import org.apache.calcite.adapter.jdbc.JdbcRules.JdbcJoin;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.linq4j.tree.ConstantUntypedNull;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.rules.FilterSetOpTransposeRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlDialect;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.OptimizerRulesContext;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.jdbc.DrillJdbcRuleBase.DrillJdbcFilterRule;
import org.apache.drill.exec.store.jdbc.DrillJdbcRuleBase.DrillJdbcProjectRule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

public class JdbcStoragePlugin extends AbstractStoragePlugin {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JdbcStoragePlugin.class);

  // Rules from Calcite's JdbcRules class that we want to avoid using.
  private static String[] RULES_TO_AVOID = {
      "JdbcToEnumerableConverterRule", "JdbcFilterRule", "JdbcProjectRule"
  };


  private final JdbcStorageConfig config;
  private final DrillbitContext context;
  private final DataSource source;
  private final String name;
  private final SqlDialect dialect;
  private final DrillJdbcConvention convention;


  public JdbcStoragePlugin(JdbcStorageConfig config, DrillbitContext context, String name) {
    this.context = context;
    this.config = config;
    this.name = name;
    BasicDataSource source = new BasicDataSource();
    source.setDriverClassName(config.getDriver());
    source.setUrl(config.getUrl());

    if (config.getUsername() != null) {
      source.setUsername(config.getUsername());
    }

    if (config.getPassword() != null) {
      source.setPassword(config.getPassword());
    }

    this.source = source;
    this.dialect = JdbcSchema.createDialect(source);
    this.convention = new DrillJdbcConvention(dialect, name);
  }


  class DrillJdbcConvention extends JdbcConvention {

    private final ImmutableSet<RelOptRule> rules;

    public DrillJdbcConvention(SqlDialect dialect, String name) {
      super(dialect, ConstantUntypedNull.INSTANCE, name);


      // build rules for this convention.
      ImmutableSet.Builder<RelOptRule> builder = ImmutableSet.builder();

      builder.add(JDBC_PRULE_INSTANCE);
      builder.add(new JdbcDrelConverterRule(this));
      builder.add(new DrillJdbcProjectRule(this));
      builder.add(new DrillJdbcFilterRule(this));

      outside: for (RelOptRule rule : JdbcRules.rules(this)) {
        final String description = rule.toString();

        // we want to black list some rules but the parent Calcite package is all or none.
        // Therefore, we remove rules with names we don't like.
        for(String black : RULES_TO_AVOID){
          if(description.equals(black)){
            continue outside;
          }

        }

        builder.add(rule);
      }

      builder.add(FilterSetOpTransposeRule.INSTANCE);
      builder.add(ProjectRemoveRule.INSTANCE);

      rules = builder.build();
    }

    @Override
    public void register(RelOptPlanner planner) {
      for (RelOptRule rule : rules) {
        planner.addRule(rule);
      }
    }

    public Set<RelOptRule> getRules() {
      return rules;
    }

    public JdbcStoragePlugin getPlugin() {
      return JdbcStoragePlugin.this;
    }
  }

  /**
   * Returns whether a condition is supported by {@link JdbcJoin}.
   *
   * <p>Corresponds to the capabilities of
   * {@link JdbcJoin#convertConditionToSqlNode}.
   *
   * @param node Condition
   * @return Whether condition is supported
   */
  private static boolean canJoinOnCondition(RexNode node) {
    final List<RexNode> operands;
    switch (node.getKind()) {
    case AND:
    case OR:
      operands = ((RexCall) node).getOperands();
      for (RexNode operand : operands) {
        if (!canJoinOnCondition(operand)) {
          return false;
        }
      }
      return true;

    case EQUALS:
    case IS_NOT_DISTINCT_FROM:
    case NOT_EQUALS:
    case GREATER_THAN:
    case GREATER_THAN_OR_EQUAL:
    case LESS_THAN:
    case LESS_THAN_OR_EQUAL:
      operands = ((RexCall) node).getOperands();
      if ((operands.get(0) instanceof RexInputRef)
          && (operands.get(1) instanceof RexInputRef)) {
        return true;
      }
      // fall through

    default:
      return false;
    }
  }


  private static final JdbcPrule JDBC_PRULE_INSTANCE = new JdbcPrule();

  private static class JdbcPrule extends ConverterRule {

    private JdbcPrule() {
      super(JdbcDrel.class, DrillRel.DRILL_LOGICAL, Prel.DRILL_PHYSICAL, "JDBC_PREL_Converter");
    }

    @Override
    public RelNode convert(RelNode in) {

      return new JdbcIntermediatePrel(
          in.getCluster(),
          in.getTraitSet().replace(getOutTrait()),
          in.getInput(0));
    }

  }

  private class JdbcDrelConverterRule extends ConverterRule {

    public JdbcDrelConverterRule(DrillJdbcConvention in) {
      super(RelNode.class, in, DrillRel.DRILL_LOGICAL, "JDBC_DREL_Converter" + in.getName());
    }

    @Override
    public RelNode convert(RelNode in) {
      return new JdbcDrel(in.getCluster(), in.getTraitSet().replace(DrillRel.DRILL_LOGICAL),
          convert(in, in.getTraitSet().replace(this.getInTrait())));
    }

  }

  private class CapitalizingJdbcSchema extends AbstractSchema {

    final Map<String, CapitalizingJdbcSchema> schemaMap = Maps.newHashMap();
    private final JdbcSchema inner;

    public CapitalizingJdbcSchema(List<String> parentSchemaPath, String name, DataSource dataSource,
        SqlDialect dialect, JdbcConvention convention, String catalog, String schema) {
      super(parentSchemaPath, name);
      inner = new JdbcSchema(dataSource, dialect, convention, catalog, schema);
    }

    @Override
    public String getTypeName() {
      return JdbcStorageConfig.NAME;
    }

    @Override
    public Collection<Function> getFunctions(String name) {
      return inner.getFunctions(name);
    }

    @Override
    public Set<String> getFunctionNames() {
      return inner.getFunctionNames();
    }

    @Override
    public CapitalizingJdbcSchema getSubSchema(String name) {
      return schemaMap.get(name);
    }

    void setHolder(SchemaPlus plusOfThis) {
      for (String s : getSubSchemaNames()) {
        CapitalizingJdbcSchema inner = getSubSchema(s);
        SchemaPlus holder = plusOfThis.add(s, inner);
        inner.setHolder(holder);
      }
    }

    @Override
    public Set<String> getSubSchemaNames() {
      return schemaMap.keySet();
    }

    @Override
    public Set<String> getTableNames() {
      return inner.getTableNames();
    }

    @Override
    public Table getTable(String name) {
      Table table = inner.getTable(name);
      if (table != null) {
        return table;
      }
      return inner.getTable(name.toUpperCase());

    }

  }

  private class JdbcCatalogSchema extends AbstractSchema {

    private final Map<String, CapitalizingJdbcSchema> schemaMap = Maps.newHashMap();
    private final CapitalizingJdbcSchema defaultSchema;

    public JdbcCatalogSchema(String name) {
      super(ImmutableList.<String> of(), name);

      try (Connection con = source.getConnection(); ResultSet set = con.getMetaData().getCatalogs()) {
        while (set.next()) {
          final String catalogName = set.getString(1);
          CapitalizingJdbcSchema schema = new CapitalizingJdbcSchema(
              getSchemaPath(), catalogName, source, dialect, convention, catalogName, null);
          schemaMap.put(catalogName, schema);
        }
      } catch (SQLException e) {
        logger.warn("Failure while attempting to load JDBC schema.", e);
      }

      // unable to read catalog list.
      if (schemaMap.isEmpty()) {

        // try to add a list of schemas to the schema map.
        boolean schemasAdded = addSchemas();

        if (!schemasAdded) {
          // there were no schemas, just create a default one (the jdbc system doesn't support catalogs/schemas).
          schemaMap.put("default", new CapitalizingJdbcSchema(ImmutableList.<String> of(), name, source, dialect,
              convention, null, null));
        }
      } else {
        // We already have catalogs. Add schemas in this context of their catalogs.
        addSchemas();
      }

      defaultSchema = schemaMap.values().iterator().next();


    }

    void setHolder(SchemaPlus plusOfThis) {
      for (String s : getSubSchemaNames()) {
        CapitalizingJdbcSchema inner = getSubSchema(s);
        SchemaPlus holder = plusOfThis.add(s, inner);
        inner.setHolder(holder);
      }
    }

    private boolean addSchemas() {
      boolean added = false;
      try (Connection con = source.getConnection(); ResultSet set = con.getMetaData().getSchemas()) {
        while (set.next()) {
          final String schemaName = set.getString(1);
          final String catalogName = set.getString(2);

          CapitalizingJdbcSchema parentSchema = schemaMap.get(catalogName);
          if (parentSchema == null) {
            CapitalizingJdbcSchema schema = new CapitalizingJdbcSchema(getSchemaPath(), schemaName, source, dialect,
                convention, catalogName, schemaName);

            // if a catalog schema doesn't exist, we'll add this at the top level.
            schemaMap.put(schemaName, schema);
          } else {
            CapitalizingJdbcSchema schema = new CapitalizingJdbcSchema(parentSchema.getSchemaPath(), schemaName,
                source, dialect,
                convention, catalogName, schemaName);
            parentSchema.schemaMap.put(schemaName, schema);

          }
          added = true;
        }
      } catch (SQLException e) {
        logger.warn("Failure while attempting to load JDBC schema.", e);
      }

      return added;
    }


    @Override
    public String getTypeName() {
      return JdbcStorageConfig.NAME;
    }

    @Override
    public Schema getDefaultSchema() {
      return defaultSchema;
    }

    @Override
    public CapitalizingJdbcSchema getSubSchema(String name) {
      return schemaMap.get(name);
    }

    @Override
    public Set<String> getSubSchemaNames() {
      return schemaMap.keySet();
    }

    @Override
    public Table getTable(String name) {
      Schema schema = getDefaultSchema();
      if (schema != null) {
        Table t = schema.getTable(name);
        if (t != null) {
          return t;
        }
        return schema.getTable(name.toUpperCase());
      } else {
        return null;
      }
    }

    @Override
    public Set<String> getTableNames() {
      return defaultSchema.getTableNames();
    }

  }

  @Override
  public void registerSchemas(SchemaConfig config, SchemaPlus parent) {
    JdbcCatalogSchema schema = new JdbcCatalogSchema(name);
    SchemaPlus holder = parent.add(name, schema);
    schema.setHolder(holder);
  }


  @Override
  public JdbcStorageConfig getConfig() {
    return config;
  }

  public DrillbitContext getContext() {
    return this.context;
  }

  public String getName() {
    return this.name;
  }

  @Override
  public boolean supportsRead() {
    return true;
  }

  public DataSource getSource() {
    return source;
  }

  public SqlDialect getDialect() {
    return dialect;
  }

  @Override
  public AbstractGroupScan getPhysicalScan(String userName, JSONOptions selection, List<SchemaPath> columns)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<RelOptRule> getOptimizerRules(OptimizerRulesContext context) {
    return convention.getRules();
  }
}
