/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.planner.common;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Maps;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.TableScan;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.util.ImpersonationUtil;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;

/**
 * Wraps the stats table info including schema and tableName. Also materializes stats from storage
 * and keeps them in memory.
 */
public class DrillStatsTable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillStatsTable.class);
  private final FileSystem fs;
  private final Path tablePath;
  /**
   * List of columns in stats table.
   */
  public static final String COL_COLUMN = "column";
  public static final String COL_COMPUTED = "computed";
  public static final String COL_STATCOUNT = "statcount";
  public static final String COL_NDV = "ndv";

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
  /**
   * Get number of distinct values of given column. If stats are not present for the given column,
   * a null is returned.
   *
   * Note: returned data may not be accurate. Accuracy depends on whether the table data has changed after the
   * stats are computed.
   *
   * @param col
   * @return
   */
  public Double getNdv(String col) {
    // Stats might not have materialized because of errors.
    if (!materialized) {
      return null;
    }
    final String upperCol = col.toUpperCase();
    final Long ndvCol = ndv.get(upperCol);
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
   * @return
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
   * @param context
   * @throws Exception
   */
  public void materialize(final QueryContext context) throws IOException {
    if (materialized) {
      return;
    }
    // Deserialize statistics from JSON
    try {
      this.statistics = readStatistics(tablePath);
      //Handle based on the statistics version read from the file
      if (statistics instanceof Statistics_v0) {
        //Do nothing
      } else if (statistics instanceof Statistics_v1) {
        for (DirectoryStatistics_v1 ds : ((Statistics_v1) statistics).getDirectoryStatistics()) {
          for (ColumnStatistics_v1 cs : ds.getColumnStatistics()) {
            ndv.put(cs.getName().toUpperCase(), cs.getNdv());
            rowCount = Math.max(rowCount, cs.getCount());
          }
        }
      }
      materialized = true;
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
            statsTable.materialize(context);
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
    @JsonProperty ("directories") List<DirectoryStatistics_v1> directoryStatistics;
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
    @JsonProperty ("schema") private long schema = -1;
    @JsonProperty ("statcount") private long count = -1;
    @JsonProperty ("nonnullstatcount") private long nonNullCount = -1;
    @JsonProperty ("ndv") private long ndv = -1;
    @JsonProperty ("hll") private byte[] hll = null;
    @JsonProperty ("avgwidth") private double width = -1;

    public ColumnStatistics_v1() {}
    @JsonGetter ("column")
    public String getName() { return this.name; }
    @JsonSetter ("column")
    public void setName(String name) {
      this.name = name;
    }
    @JsonGetter ("schema")
    public double getSchema() {
      return this.schema;
    }
    @JsonSetter ("schema")
    public void setSchema(long schema) {
      this.schema = schema;
    }
    @JsonGetter ("statcount")
    public double getCount() {
      return this.count;
    }
    @JsonSetter ("statcount")
    public void setCount(long count) {
      this.count = count;
    }
    @JsonGetter ("nonnullstatcount")
    public double getNonNullCount() {
      return this.nonNullCount;
    }
    @JsonSetter ("nonnullstatcount")
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
    @JsonGetter ("hll")
    public byte[] getHLL() {
      return this.hll;
    }
    @JsonSetter ("hll")
    public void setHLL(byte[] hll) { this.hll = hll; }
  }

  private TableStatistics readStatistics(Path path) throws IOException {
    Stopwatch timer = Stopwatch.createStarted();
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    FSDataInputStream is = fs.open(path);

    TableStatistics statistics = mapper.readValue(is, TableStatistics.class);
    logger.info("Took {} ms to read metadata from cache file", timer.elapsed(TimeUnit.MILLISECONDS));
    timer.stop();
    return statistics;
  }
}
