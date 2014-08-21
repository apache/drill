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
package org.apache.drill.exec.planner.common;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.TableScan;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.planner.sql.DirectPlan;
import org.apache.drill.exec.record.MajorTypeSerDe;
import org.apache.drill.exec.store.StoragePlugin;
import org.apache.drill.exec.store.dfs.FileSystemPlugin;
import org.apache.drill.exec.store.dfs.FormatPlugin;
import org.apache.drill.exec.store.dfs.FormatSelection;
import org.apache.drill.exec.store.parquet.ParquetFormatConfig;
import org.apache.drill.exec.util.ImpersonationUtil;
import org.apache.drill.shaded.guava.com.google.common.collect.Maps;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Wraps the stats table info including schema and tableName. Also materializes stats from storage
 * and keeps them in memory.
 */
public class DrillStatsTable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillStatsTable.class);
  // All the statistics versions till date
  public enum STATS_VERSION {V0, V1};
  // The current version
  public static final STATS_VERSION CURRENT_VERSION = STATS_VERSION.V1;
  private final FileSystem fs;
  private final Path tablePath;
  private final String schemaName;
  private final String tableName;
  private final Map<String, Long> ndv = Maps.newHashMap();
  private double rowCount = -1;
  private boolean materialized = false;
  private TableStatistics statistics = null;

  public DrillStatsTable(String schemaName, String tableName, Path tablePath, FileSystem fs) {
    this.schemaName = schemaName;
    this.tableName = tableName;
    this.tablePath = tablePath;
    this.fs = ImpersonationUtil.createFileSystem(ImpersonationUtil.getProcessUserName(), fs.getConf());
  }

  public String getSchemaName() {
    return schemaName;
  }

  public String getTableName() {
    return tableName;
  }

  /*
   * Returns whether statistics have materialized or not i.e. were the table statistics successfully read
   * from the persistent store?
   */
  public boolean isMaterialized() { return materialized; }
  /**
   * Get the approximate number of distinct values of given column. If stats are not present for the
   * given column, a null is returned.
   *
   * Note: returned data may not be accurate. Accuracy depends on whether the table data has changed
   * after the stats are computed.
   *
   * @param col - column for which approximate count distinct is desired
   * @return approximate count distinct of the column, if available. NULL otherwise.
   */
  public Double getNdv(String col) {
    // Stats might not have materialized because of errors.
    if (!materialized) {
      return null;
    }
    final String upperCol = col.toUpperCase();
    Long ndvCol = ndv.get(upperCol);
    if (ndvCol == null) {
      ndvCol = ndv.get(SchemaPath.getSimplePath(upperCol).toString());
    }
    // Ndv estimation techniques like HLL may over-estimate, hence cap it at rowCount
    if (ndvCol != null) {
      return Math.min(ndvCol, rowCount);
    }
    return null;
  }

  /**
   * Get row count of the table. Returns null if stats are not present.
   *
   * Note: returned data may not be accurate. Accuracy depends on whether the table data has
   * changed after the stats are computed.
   *
   * @return rowcount for the table, if available. NULL otherwise.
   */
  public Double getRowCount() {
    // Stats might not have materialized because of errors.
    if (!materialized) {
      return null;
    }
    return rowCount > 0 ? rowCount : null;
  }

  /**
   * Read the stats from storage and keep them in memory.
   * @param table - Drill table for which we require stats
   * @param context - Query context
   * @throws Exception
   */
  public void materialize(final DrillTable table, final QueryContext context) throws IOException {
    if (materialized) {
      return;
    }
    // Deserialize statistics from JSON
    try {
      this.statistics = readStatistics(table, tablePath);
      // Handle based on the statistics version read from the file
      if (statistics instanceof Statistics_v0) {
        // Do nothing
      } else if (statistics instanceof Statistics_v1) {
        for (DirectoryStatistics_v1 ds : ((Statistics_v1) statistics).getDirectoryStatistics()) {
          for (ColumnStatistics_v1 cs : ds.getColumnStatistics()) {
            ndv.put(cs.getName().toUpperCase(), cs.getNdv());
            rowCount = Math.max(rowCount, cs.getCount());
          }
        }
      }
      if (statistics != null) { // See stats are available before setting materialized
        materialized = true;
      }
    } catch (IOException ex) {
      logger.warn("Failed to read the stats file.", ex);
      throw ex;
    }
  }

  /**
   * Materialize on nodes that have an attached stats table
   */
  public static class StatsMaterializationVisitor extends RelVisitor {
    private QueryContext context;

    public static void materialize(final RelNode relNode, final QueryContext context) {
      new StatsMaterializationVisitor(context).go(relNode);
    }

    private StatsMaterializationVisitor(QueryContext context) {
      this.context = context;
    }

    @Override
    public void visit(RelNode node, int ordinal, RelNode parent) {
      if (node instanceof TableScan) {
        try {
          final DrillTable drillTable = node.getTable().unwrap(DrillTable.class);
          final DrillStatsTable statsTable = drillTable.getStatsTable();
          if (statsTable != null) {
            statsTable.materialize(drillTable, context);
          } else {
            throw new DrillRuntimeException(
                String.format("Failed to find the stats for table [%s] in schema [%s]",
                    node.getTable().getQualifiedName(), node.getTable().getRelOptSchema()));
          }
        } catch (Exception e) {
          // Log a warning and proceed. We don't want to fail a query.
          logger.warn("Failed to materialize the stats. Continuing without stats.", e);
        }
      }
      super.visit(node, ordinal, parent);
    }
  }

  /* Each change to the format SHOULD increment the default and/or the max values of the option
   * exec.statistics.capability_version
   */
  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY,
      property = "statistics_version")
  @JsonSubTypes({
      @JsonSubTypes.Type(value = DrillStatsTable.Statistics_v1.class, name="v0"),
      @JsonSubTypes.Type(value = DrillStatsTable.Statistics_v1.class, name="v1")
  })
  public static abstract class TableStatistics {
    @JsonIgnore public abstract List<? extends DirectoryStatistics> getDirectoryStatistics();
  }

  public static abstract class DirectoryStatistics {
  }

  public static abstract class ColumnStatistics {
  }

  @JsonTypeName("v0")
  public static class Statistics_v0 extends TableStatistics {
    @JsonProperty ("directories") List<DirectoryStatistics_v0> directoryStatistics;
    // Default constructor required for deserializer
    public Statistics_v0 () { }
    @JsonGetter ("directories")
    public List<DirectoryStatistics_v0> getDirectoryStatistics() {
      return directoryStatistics;
    }
    @JsonSetter ("directories")
    public void setDirectoryStatistics(List<DirectoryStatistics_v0> directoryStatistics) {
      this.directoryStatistics = directoryStatistics;
    }
  }

  public static class DirectoryStatistics_v0 extends DirectoryStatistics {
    @JsonProperty private double computed;
    // Default constructor required for deserializer
    public DirectoryStatistics_v0() { }
    @JsonGetter ("computed")
    public double getComputedTime() {
      return this.computed;
    }
    @JsonSetter ("computed")
    public void setComputedTime(double computed) {
      this.computed = computed;
    }
  }

  /**
   * Struct which contains the statistics for the entire directory structure
   */
  @JsonTypeName("v1")
  public static class Statistics_v1 extends TableStatistics {
    @JsonProperty ("directories")
    List<DirectoryStatistics_v1> directoryStatistics;
    // Default constructor required for deserializer
    public Statistics_v1 () { }
    @JsonGetter ("directories")
    public List<DirectoryStatistics_v1> getDirectoryStatistics() {
      return directoryStatistics;
    }
    @JsonSetter ("directories")
    public void setDirectoryStatistics(List<DirectoryStatistics_v1> directoryStatistics) {
      this.directoryStatistics = directoryStatistics;
    }
  }

  public static class DirectoryStatistics_v1 extends DirectoryStatistics {
    @JsonProperty private String computed;
    @JsonProperty ("columns") private List<ColumnStatistics_v1> columnStatistics;
    // Default constructor required for deserializer
    public DirectoryStatistics_v1() { }
    @JsonGetter ("computed")
    public String getComputedTime() {
      return this.computed;
    }
    @JsonSetter ("computed")
    public void setComputedTime(String computed) {
      this.computed = computed;
    }
    @JsonGetter ("columns")
    public List<ColumnStatistics_v1> getColumnStatistics() {
      return this.columnStatistics;
    }
    @JsonSetter ("columns")
    public void setColumnStatistics(List<ColumnStatistics_v1> columnStatistics) {
      this.columnStatistics = columnStatistics;
    }
  }

  public static class ColumnStatistics_v1 extends ColumnStatistics {
    @JsonProperty ("column") private String name = null;
    @JsonProperty ("majortype")   private TypeProtos.MajorType type = null;
    @JsonProperty ("schema") private long schema = 0;
    @JsonProperty ("rowcount") private long count = 0;
    @JsonProperty ("nonnullrowcount") private long nonNullCount = 0;
    @JsonProperty ("ndv") private long ndv = 0;
    @JsonProperty ("avgwidth") private double width = 0;

    public ColumnStatistics_v1() {}
    @JsonGetter ("column")
    public String getName() { return this.name; }
    @JsonSetter ("column")
    public void setName(String name) {
      this.name = name;
    }
    @JsonGetter ("majortype")
    public TypeProtos.MajorType getType() { return this.type; }
    @JsonSetter ("type")
    public void setType(TypeProtos.MajorType type) {
      this.type = type;
    }
    @JsonGetter ("schema")
    public double getSchema() {
      return this.schema;
    }
    @JsonSetter ("schema")
    public void setSchema(long schema) {
      this.schema = schema;
    }
    @JsonGetter ("rowcount")
    public double getCount() {
      return this.count;
    }
    @JsonSetter ("rowcount")
    public void setCount(long count) {
      this.count = count;
    }
    @JsonGetter ("nonnullrowcount")
    public double getNonNullCount() {
      return this.nonNullCount;
    }
    @JsonSetter ("nonnullrowcount")
    public void setNonNullCount(long nonNullCount) {
      this.nonNullCount = nonNullCount;
    }
    @JsonGetter ("ndv")
    public long getNdv() {
      return this.ndv;
    }
    @JsonSetter ("ndv")
    public void setNdv(long ndv) { this.ndv = ndv; }
    @JsonGetter ("avgwidth")
    public double getAvgWidth() {
      return this.width;
    }
    @JsonSetter ("avgwidth")
    public void setAvgWidth(double width) { this.width = width; }
  }

  private TableStatistics readStatistics(DrillTable drillTable, Path path) throws IOException {
    final Object selection = drillTable.getSelection();

    if (selection instanceof FormatSelection) {
      StoragePlugin storagePlugin =  drillTable.getPlugin();
      FormatSelection formatSelection = (FormatSelection) selection;
      FormatPluginConfig formatConfig = formatSelection.getFormat();

      if (storagePlugin instanceof FileSystemPlugin
          && (formatConfig instanceof ParquetFormatConfig)) {
        FormatPlugin fmtPlugin = storagePlugin.getFormatPlugin(formatConfig);
        if (fmtPlugin.supportsStatistics()) {
          return fmtPlugin.readStatistics(fs, path);
        }
      }
    }
    return null;
  }

  public static TableStatistics generateDirectoryStructure(String dirComputedTime,
      List<ColumnStatistics> columnStatisticsList) {
    // TODO: Split up columnStatisticsList() based on directory names. We assume only
    // one directory right now but this WILL change in the future
    // HashMap<String, Boolean> dirNames = new HashMap<String, Boolean>();
    TableStatistics statistics = new Statistics_v1();
    List<DirectoryStatistics_v1> dirStats = new ArrayList<DirectoryStatistics_v1>();
    List<ColumnStatistics_v1> columnStatisticsV1s = new ArrayList<DrillStatsTable.ColumnStatistics_v1>();
    // Create dirStats
    DirectoryStatistics_v1 dirStat = new DirectoryStatistics_v1();
    // Add columnStats corresponding to this dirStats
    for (ColumnStatistics colStats : columnStatisticsList) {
      columnStatisticsV1s.add((ColumnStatistics_v1) colStats);
    }
    dirStat.setComputedTime(dirComputedTime);
    dirStat.setColumnStatistics(columnStatisticsV1s);
    // Add this dirStats to the list of dirStats
    dirStats.add(dirStat);
    // Add list of dirStats to tableStats
    ((Statistics_v1) statistics).setDirectoryStatistics(dirStats);
    return statistics;
  }

  public static PhysicalPlan direct(QueryContext context, boolean outcome, String message, Object... values) {
    return DirectPlan.createDirectPlan(context, outcome, String.format(message, values));
  }

  /* Helper function to generate error - statistics not supported on non-parquet tables */
  public static PhysicalPlan notSupported(QueryContext context, String tbl) {
    return direct(context, false, "Table %s is not supported by ANALYZE."
        + " Support is currently limited to directory-based Parquet tables.", tbl);
  }

  public static PhysicalPlan notRequired(QueryContext context, String tbl) {
    return direct(context, false, "Table %s has not changed since last ANALYZE!", tbl);
  }

  /**
   * This method returns the statistics (de)serializer which can be used to (de)/serialize the
   * {@link TableStatistics} from/to JSON
   */
  public static ObjectMapper getMapper() {
    ObjectMapper mapper = new ObjectMapper();
    SimpleModule deModule = new SimpleModule("StatisticsSerDeModule")
        .addSerializer(TypeProtos.MajorType.class, new MajorTypeSerDe.Se())
        .addDeserializer(TypeProtos.MajorType.class, new MajorTypeSerDe.De());
    mapper.registerModule(deModule);
    return mapper;
  }
}
