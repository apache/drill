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

import java.io.IOException;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.drill.common.expression.ExpressionStringBuilder;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.LiteralExpression;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.common.parser.LogicalExpressionParser;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.planner.PhysicalPlanReader;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.planner.logical.DrillTableSelection;
import org.apache.drill.exec.rpc.NamedThreadFactory;
import org.apache.drill.exec.store.StoragePlugin;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.StoragePluginRegistry.PluginException;
import org.apache.drill.exec.store.PlanCacheTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Objects;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.hash.Hashing;

/** Drillbit-scoped, immutable JSON snapshots of query plans. */
public final class PlanCache implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(PlanCache.class);
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final String MARKER = ExpressionStringBuilder.BOUND_DYNAMIC_PARAM + "(";
  private final Cache<String, Entry> entries = CacheBuilder.newBuilder()
      .maximumWeight(32 * 1024 * 1024)
      .weigher((String key, Entry value) -> value.json.length())
      .expireAfterWrite(10, TimeUnit.MINUTES)
      .recordStats()
      .build();
  private final AtomicLong successfulBinds = new AtomicLong();
  private final ThreadPoolExecutor writer = new ThreadPoolExecutor(1, 1, 0L,
      TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(64),
      new NamedThreadFactory("plan-cache-writer-"));

  public long getHitCount() {
    return successfulBinds.get();
  }

  public void recordBind() {
    successfulBinds.incrementAndGet();
  }

  /** Appends values from this query without changing the cached explain template. */
  static String textWithBindings(String template, List<SqlLiteral> literals) {
    if (template == null || literals.isEmpty()) {
      return template;
    }
    StringBuilder text = new StringBuilder(template);
    if (text.length() > 0 && text.charAt(text.length() - 1) != '\n') {
      text.append('\n');
    }
    text.append("Parameters: ");
    for (int i = 0; i < literals.size(); i++) {
      if (i > 0) {
        text.append(", ");
      }
      text.append('?').append(i).append(" = ")
          .append(literals.get(i).toString().replace("\r", "\\r").replace("\n", "\\n"));
    }
    return text.toString();
  }

  public Entry get(String key) {
    return entries.getIfPresent(key);
  }

  /** Waits for writes already submitted by completed queries; intended for tests. */
  public void awaitWrites() throws InterruptedException, ExecutionException, TimeoutException {
    writer.submit(() -> { }).get(10, TimeUnit.SECONDS);
  }

  public boolean canCache(PhysicalPlan plan) {
    return plan.getSortedOperators().stream()
        .noneMatch(op -> op instanceof GroupScan && !((GroupScan) op).supportPlanCache());
  }

  public void writeAfterSuccess(String key, PhysicalPlan plan, String textPlan,
      PhysicalPlanReader reader, ContextSnapshot context) {
    try {
      writer.execute(() -> {
        try {
          put(key, plan, textPlan, reader, context);
        } catch (RuntimeException | IOException e) {
          logger.debug("Plan cache entry could not be published", e);
        }
      });
    } catch (RejectedExecutionException e) {
      logger.debug("Plan cache writer is busy; skipping cache entry", e);
    }
  }

  public void invalidate(String key) {
    entries.invalidate(key);
  }

  public boolean put(String key, PhysicalPlan plan, String textPlan,
      PhysicalPlanReader reader, ContextSnapshot context) throws IOException {
    // The engine checks generic context and expression compatibility. Each
    // opted-in scan is responsible for rebuilding its value-dependent state.
    if (context == null || context.optionsFingerprint == null || !canCache(plan)) {
      return false;
    }
    String json = reader.writeJson(plan);
    // Confirm that standard PhysicalPlan JSON can be read back before publishing.
    reader.readPhysicalPlan(json);
    entries.put(key, new Entry(json, textPlan, context));
    return true;
  }

  @Override
  public void close() {
    writer.shutdownNow();
  }

  public static final class Entry {
    private final String json;
    private final String textPlan;
    private final ContextSnapshot context;

    private Entry(String json, String textPlan, ContextSnapshot context) {
      this.json = json;
      this.textPlan = textPlan;
      this.context = context;
    }

    public boolean matchesContext(String optionsFingerprint, StoragePluginRegistry plugins) {
      return context.isCurrent(optionsFingerprint, plugins);
    }

    public String getTextPlan() {
      return textPlan;
    }

    public PhysicalPlan bind(List<SqlLiteral> literals, PhysicalPlanReader reader)
        throws IOException {
      JsonNode tree = JSON.readTree(json);
      walk(tree, literals);
      return reader.readPhysicalPlan(JSON.writeValueAsString(tree));
    }
  }

  /** Options, plugin configurations, and table versions checked after lookup. */
  public static final class ContextSnapshot {
    private final String optionsFingerprint;
    private final Map<TableIdentifier, String> tableVersions;
    private final Map<String, String> pluginConfigs;

    private ContextSnapshot(String optionsFingerprint, Map<TableIdentifier, String> tableVersions,
        Map<String, String> pluginConfigs) {
      this.optionsFingerprint = optionsFingerprint;
      this.tableVersions = java.util.Collections.unmodifiableMap(tableVersions);
      this.pluginConfigs = java.util.Collections.unmodifiableMap(pluginConfigs);
    }

    public ContextSnapshot withOptionsFingerprint(String fingerprint) {
      return new ContextSnapshot(Objects.requireNonNull(fingerprint), tableVersions, pluginConfigs);
    }

    private static final class TableIdentifier {
      private final String storageName;
      private final String tableId;

      private TableIdentifier(String storageName, String tableId) {
        this.storageName = storageName;
        this.tableId = tableId;
      }

      @Override
      public boolean equals(Object other) {
        if (this == other) {
          return true;
        }
        if (!(other instanceof TableIdentifier)) {
          return false;
        }
        TableIdentifier that = (TableIdentifier) other;
        return Objects.equals(storageName, that.storageName)
            && Objects.equals(tableId, that.tableId);
      }

      @Override
      public int hashCode() {
        return Objects.hash(storageName, tableId);
      }
    }

    private static final class ResolvedTable {
      private final List<String> tableNames;
      private final TableIdentifier identifier;
      private final String version;
      private final StoragePluginConfig pluginConfig;

      private ResolvedTable(List<String> tableNames, TableIdentifier identifier, String version,
          StoragePluginConfig pluginConfig) {
        this.tableNames = tableNames;
        this.identifier = identifier;
        this.version = version;
        this.pluginConfig = pluginConfig;
      }
    }

    /** Resolves every physical table in a query, including joins and subqueries. */
    public static ContextSnapshot resolve(SchemaPlus defaultSchema, SqlNode query,
        StoragePluginRegistry plugins) {
      List<ResolvedTable> dependencies = new ArrayList<>();
      try {
        if (!collectQuery(defaultSchema, query, new HashSet<>(), dependencies)) {
          return null;
        }
      } catch (RuntimeException e) {
        logger.debug("Query sources could not be validated for the plan cache", e);
        return null;
      }
      Map<TableIdentifier, String> versions = new LinkedHashMap<>();
      Map<String, String> configs = new LinkedHashMap<>();
      for (ResolvedTable dependency : dependencies) {
        String previous = versions.putIfAbsent(dependency.identifier, dependency.version);
        if (previous != null && !previous.equals(dependency.version)) {
          return null;
        }
        try {
          String fingerprint = configFingerprint(plugins, dependency.pluginConfig);
          String previousConfig = configs.putIfAbsent(dependency.identifier.storageName,
              fingerprint);
          if (previousConfig != null && !previousConfig.equals(fingerprint)) {
            return null;
          }
        } catch (RuntimeException e) {
          logger.debug("Plugin configuration could not be fingerprinted for the plan cache", e);
          return null;
        }
      }
      return new ContextSnapshot(null, versions, configs);
    }

    private static boolean collectQuery(SchemaPlus schema, SqlNode node,
        Set<String> ctes, List<ResolvedTable> dependencies) {
      if (node == null) {
        return true;
      }
      if (node instanceof SqlNodeList) {
        for (SqlNode child : (SqlNodeList) node) {
          if (!collectQuery(schema, child, ctes, dependencies)) {
            return false;
          }
        }
        return true;
      }
      if (node instanceof SqlWith) {
        SqlWith with = (SqlWith) node;
        Set<String> local = new HashSet<>(ctes);
        for (SqlNode itemNode : with.withList) {
          SqlWithItem item = (SqlWithItem) itemNode;
          if (!collectQuery(schema, item.query, local, dependencies)) {
            return false;
          }
          local.add(item.name.getSimple());
        }
        return collectQuery(schema, with.body, local, dependencies);
      }
      if (node instanceof SqlSelect) {
        SqlSelect select = (SqlSelect) node;
        if (!collectFrom(schema, select.getFrom(), ctes, dependencies)) {
          return false;
        }
        for (SqlNode operand : select.getOperandList()) {
          if (operand != select.getFrom()
              && !collectQuery(schema, operand, ctes, dependencies)) {
            return false;
          }
        }
        return true;
      }
      if (node instanceof SqlCall) {
        for (SqlNode operand : ((SqlCall) node).getOperandList()) {
          if (!collectQuery(schema, operand, ctes, dependencies)) {
            return false;
          }
        }
      }
      return true;
    }

    private static boolean collectFrom(SchemaPlus schema, SqlNode from,
        Set<String> ctes, List<ResolvedTable> dependencies) {
      if (from == null || from.getKind() == SqlKind.VALUES) {
        return true;
      }
      if (from instanceof SqlIdentifier) {
        List<String> names = ((SqlIdentifier) from).names;
        if (names.size() == 1 && ctes.contains(names.get(0))) {
          return true;
        }
        for (ResolvedTable dependency : dependencies) {
          if (dependency.tableNames.equals(names)) {
            return true;
          }
        }
        ResolvedTable source = resolveTable(schema, names);
        if (source == null) {
          return false;
        }
        dependencies.add(source);
        return true;
      }
      if (from instanceof SqlCall) {
        SqlCall call = (SqlCall) from;
        if (call.getKind() == SqlKind.AS || call.getKind() == SqlKind.LATERAL) {
          return collectFrom(schema, call.operand(0), ctes, dependencies);
        }
        if (call.getKind() == SqlKind.JOIN) {
          return collectFrom(schema, call.operand(0), ctes, dependencies)
              && collectFrom(schema, call.operand(3), ctes, dependencies)
              && collectQuery(schema, call.operand(5), ctes, dependencies);
        }
        if (call.getKind().belongsTo(SqlKind.QUERY)) {
          return collectQuery(schema, call, ctes, dependencies);
        }
      }
      return false;
    }

    private static ResolvedTable resolveTable(SchemaPlus defaultSchema, List<String> names) {
      if (names.isEmpty() || names.get(names.size() - 1).contains("*")
          || names.get(names.size() - 1).contains("?")) {
        return null;
      }
      SchemaPlus schema = SchemaUtilities.findSchema(defaultSchema,
          names.subList(0, names.size() - 1));
      if (schema == null) {
        return null;
      }
      Table table = schema.getTable(names.get(names.size() - 1));
      if (!(table instanceof DrillTable)) {
        return null;
      }
      DrillTable drillTable = (DrillTable) table;
      Object selection = drillTable.getSelection();
      if (!(selection instanceof DrillTableSelection)) {
        return null;
      }
      StoragePlugin storagePlugin = drillTable.getPlugin();
      if (!storagePlugin.supportPlanCache()) {
        return null;
      }
      try {
        PlanCacheTable source = storagePlugin.planCacheTable((DrillTableSelection) selection);
        if (source == null) {
          return null;
        }
        return new ResolvedTable(
            java.util.Collections.unmodifiableList(new java.util.ArrayList<>(names)),
            new TableIdentifier(drillTable.getStorageEngineName(), source.getIdentifier()),
            source.getVersion(), storagePlugin.getConfig());
      } catch (IOException e) {
        logger.debug("Table version could not be read for the plan cache", e);
        return null;
      }
    }

    public boolean isCurrent(String currentOptionsFingerprint, StoragePluginRegistry plugins) {
      if (!Objects.equals(optionsFingerprint, currentOptionsFingerprint)) {
        return false;
      }
      Map<String, StoragePlugin> currentPlugins = new HashMap<>();
      for (Map.Entry<String, String> entry : pluginConfigs.entrySet()) {
        try {
          StoragePlugin plugin = plugins.getPlugin(entry.getKey());
          if (plugin == null || !plugin.supportPlanCache()
              || !entry.getValue().equals(configFingerprint(plugins, plugin.getConfig()))) {
            return false;
          }
          currentPlugins.put(entry.getKey(), plugin);
        } catch (PluginException | RuntimeException e) {
          logger.debug("Plugin configuration could not be checked for the plan cache", e);
          return false;
        }
      }
      for (Map.Entry<TableIdentifier, String> entry : tableVersions.entrySet()) {
        TableIdentifier identifier = entry.getKey();
        try {
          StoragePlugin storagePlugin = currentPlugins.get(identifier.storageName);
          if (!Objects.equals(entry.getValue(),
              storagePlugin.planCacheTableVersion(identifier.tableId))) {
            return false;
          }
        } catch (IOException e) {
          logger.debug("Table version could not be read for the plan cache", e);
          return false;
        }
      }
      return true;
    }

    private static String configFingerprint(StoragePluginRegistry plugins,
        StoragePluginConfig config) {
      if (config == null) {
        throw new IllegalArgumentException("Missing plugin configuration");
      }
      String encoded = plugins.encode(config);
      if (encoded == null) {
        throw new IllegalArgumentException("Plugin configuration could not be encoded");
      }
      return Hashing.sha256().hashString(encoded, StandardCharsets.UTF_8).toString();
    }
  }

  private static String serialize(SqlLiteral literal, MajorType type, int index) {
    Object value = literal.getValue();
    LogicalExpression expression;
    if (value instanceof java.math.BigDecimal) {
      java.math.BigDecimal number = (java.math.BigDecimal) value;
      if (type.getMinorType() == MinorType.VARDECIMAL) {
        java.math.BigDecimal decimal = number.setScale(type.getScale(), RoundingMode.UNNECESSARY);
        if (decimal.precision() > type.getPrecision()) {
          throw new IllegalArgumentException("Decimal parameter exceeds cached precision");
        }
        expression = ValueExpressions.getVarDecimal(decimal,
            type.getPrecision(), type.getScale());
      } else if (type.getMinorType() == MinorType.FLOAT8) {
        expression = ValueExpressions.getFloat8(number.doubleValue());
      } else if (type.getMinorType() == MinorType.FLOAT4) {
        expression = ValueExpressions.getFloat4(number.floatValue());
      } else if (type.getMinorType() == MinorType.BIGINT) {
        expression = ValueExpressions.getBigInt(number.longValueExact());
      } else if (type.getMinorType() == MinorType.INT) {
        expression = ValueExpressions.getInt(number.intValueExact());
      } else {
        throw new IllegalArgumentException("Unsupported numeric parameter type");
      }
    } else if (value instanceof Boolean) {
      expression = ValueExpressions.getBit((Boolean) value);
    } else if (value instanceof org.apache.calcite.util.NlsString) {
      String string = ((org.apache.calcite.util.NlsString) value).getValue();
      expression = ValueExpressions.getChar(string, type.getPrecision());
    } else {
      throw new IllegalArgumentException("Unsupported parameter type");
    }
    ((LiteralExpression) expression).setDynamicParamIndex(index);
    return ExpressionStringBuilder.toString(expression);
  }

  /** Visits every serialized field, including nested plugin-specific fields. */
  private static void walk(JsonNode node, List<SqlLiteral> replacements) {
    if (node.isObject()) {
      ObjectNode object = (ObjectNode) node;
      object.fields().forEachRemaining(field -> {
        JsonNode value = field.getValue();
        if (value.isTextual() && value.asText().contains(MARKER)) {
          object.put(field.getKey(), rewrite(value.asText(), replacements));
        } else {
          walk(value, replacements);
        }
      });
    } else if (node.isArray()) {
      ArrayNode array = (ArrayNode) node;
      for (int i = 0; i < array.size(); i++) {
        JsonNode value = array.get(i);
        if (value.isTextual() && value.asText().contains(MARKER)) {
          array.set(i, JSON.getNodeFactory().textNode(rewrite(value.asText(), replacements)));
        } else {
          walk(value, replacements);
        }
      }
    }
  }

  private static String rewrite(String input, List<SqlLiteral> replacements) {
    // Parsing first ensures that a marker belongs to a Drill expression field.
    LogicalExpression original = LogicalExpressionParser.parse(input);
    Map<Integer, MajorType> originalTypes = new HashMap<>();
    collectTypes(original, originalTypes);
    StringBuilder output = new StringBuilder();
    int offset = 0;
    while (offset < input.length()) {
      int start = findMarker(input, offset);
      if (start < 0) {
        output.append(input, offset, input.length());
        break;
      }
      output.append(input, offset, start);
      int indexEnd = input.indexOf(',', start + MARKER.length());
      if (indexEnd < 0) {
        throw new IllegalArgumentException("Malformed cached parameter");
      }
      int index = Integer.parseInt(input.substring(start + MARKER.length(), indexEnd).trim());
      if (!originalTypes.containsKey(index)) {
        throw new IllegalArgumentException("Cached marker is not an expression parameter");
      }
      int end = matchingClose(input, indexEnd + 1);
      if (index < 0 || index >= replacements.size()) {
        throw new IllegalArgumentException("Unexpected cached parameter index");
      }
      // Keep the slot marker in the execution plan for round-trip checks.
      output.append(serialize(replacements.get(index), originalTypes.get(index), index));
      offset = end + 1;
    }
    String result = output.toString();
    LogicalExpression parsed = LogicalExpressionParser.parse(result);
    Map<Integer, MajorType> newTypes = new HashMap<>();
    collectTypes(parsed, newTypes);
    if (!originalTypes.equals(newTypes)) {
      throw new IllegalArgumentException("Cached parameter type changed");
    }
    return result;
  }

  private static int findMarker(String input, int offset) {
    boolean quoted = false;
    boolean escaped = false;
    for (int i = 0; i < input.length(); i++) {
      char c = input.charAt(i);
      if (escaped) {
        escaped = false;
      } else if (c == '\\' && quoted) {
        escaped = true;
      } else if (c == '\'') {
        quoted = !quoted;
      } else if (!quoted && i >= offset && input.startsWith(MARKER, i)) {
        return i;
      }
    }
    return -1;
  }

  private static void collectTypes(LogicalExpression expression,
      Map<Integer, MajorType> types) {
    if (expression instanceof LiteralExpression) {
      int index = ((LiteralExpression) expression).getDynamicParamIndex();
      if (index >= 0) {
        MajorType type = expression.getMajorType();
        MajorType previous = types.putIfAbsent(index, type);
        if (previous != null && !previous.equals(type)) {
          throw new IllegalArgumentException("Conflicting cached parameter types");
        }
      }
    }
    for (LogicalExpression child : expression) {
      collectTypes(child, types);
    }
  }

  private static int matchingClose(String input, int start) {
    boolean quoted = false;
    boolean escaped = false;
    int depth = 1;
    for (int i = start; i < input.length(); i++) {
      char c = input.charAt(i);
      if (escaped) {
        escaped = false;
      } else if (c == '\\' && quoted) {
        escaped = true;
      } else if (c == '\'') {
        quoted = !quoted;
      } else if (!quoted && c == '(') {
        depth++;
      } else if (!quoted && c == ')' && --depth == 0) {
        return i;
      }
    }
    throw new IllegalArgumentException("Unclosed cached parameter");
  }
}
