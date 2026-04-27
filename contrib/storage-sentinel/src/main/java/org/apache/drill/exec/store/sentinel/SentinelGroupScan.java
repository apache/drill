package org.apache.drill.exec.store.sentinel;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.drill.common.PlanStringBuilder;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.metastore.MetadataProviderManager;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.util.Utilities;
import org.apache.drill.metastore.metadata.TableMetadataProvider;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class SentinelGroupScan extends AbstractGroupScan {
  private final SentinelStoragePluginConfig config;
  private final List<SchemaPath> columns;
  private final SentinelScanSpec scanSpec;
  private final ScanStats scanStats;
  private final MetadataProviderManager metadataProviderManager;

  private int hashCode;

  public SentinelGroupScan(SentinelScanSpec scanSpec, MetadataProviderManager metadataProviderManager) {
    super("sentinel-scan");
    this.scanSpec = scanSpec;
    this.config = null;
    this.columns = ALL_COLUMNS;
    this.metadataProviderManager = metadataProviderManager;
    this.scanStats = computeScanStats();
  }

  public SentinelGroupScan(
      SentinelStoragePluginConfig config,
      SentinelScanSpec scanSpec,
      MetadataProviderManager metadataProviderManager) {
    super("sentinel-scan");
    this.config = config;
    this.scanSpec = scanSpec;
    this.columns = ALL_COLUMNS;
    this.metadataProviderManager = metadataProviderManager;
    this.scanStats = computeScanStats();
  }

  public SentinelGroupScan(SentinelGroupScan that) {
    super(that);
    this.config = that.config;
    this.scanSpec = that.scanSpec;
    this.columns = that.columns;
    this.metadataProviderManager = that.metadataProviderManager;
    this.scanStats = that.scanStats;
    this.hashCode = that.hashCode;
  }

  public SentinelGroupScan(SentinelGroupScan that, List<SchemaPath> columns) {
    super(that);
    this.config = that.config;
    this.scanSpec = that.scanSpec;
    this.columns = columns;
    this.metadataProviderManager = that.metadataProviderManager;
    this.scanStats = computeScanStats();
  }

  @JsonCreator
  public SentinelGroupScan(
      @JsonProperty("config") SentinelStoragePluginConfig config,
      @JsonProperty("scanSpec") SentinelScanSpec scanSpec,
      @JsonProperty("columns") List<SchemaPath> columns) {
    super("no-user");
    this.config = config;
    this.scanSpec = scanSpec;
    this.columns = columns;
    this.metadataProviderManager = null;
    this.scanStats = computeScanStats();
  }

  @JsonProperty("config")
  public SentinelStoragePluginConfig getConfig() {
    return config;
  }

  @JsonProperty("scanSpec")
  public SentinelScanSpec getScanSpec() {
    return scanSpec;
  }

  @JsonProperty("columns")
  public List<SchemaPath> getColumns() {
    return columns;
  }

  @Override
  public void applyAssignments(List<CoordinationProtos.DrillbitEndpoint> endpoints) {
  }

  @Override
  public SubScan getSpecificScan(int minorFragmentId) {
    return new SentinelSubScan(config, scanSpec, columns);
  }

  @Override
  public int getMaxParallelizationWidth() {
    return 1;
  }

  @Override
  public boolean canPushdownProjects(List<SchemaPath> columns) {
    return true;
  }

  @Override
  public boolean supportsLimitPushdown() {
    return true;
  }

  @Override
  public GroupScan applyLimit(int maxRecords) {
    return null;
  }

  @Override
  public GroupScan clone(List<SchemaPath> columns) {
    return new SentinelGroupScan(this, columns);
  }

  @Override
  public String getDigest() {
    return toString();
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    return new SentinelGroupScan(this);
  }

  @Override
  public ScanStats getScanStats() {
    return scanStats;
  }

  private ScanStats computeScanStats() {
    double estRowCount = 100_000;
    double estColCount = Utilities.isStarQuery(columns) ? DrillScanRel.STAR_COLUMN_COST : columns.size();
    double valueCount = estRowCount * estColCount;
    double cpuCost = valueCount;
    double ioCost = valueCount;

    return new ScanStats(ScanStats.GroupScanProperty.ESTIMATED_TOTAL_COST,
        estRowCount, cpuCost, ioCost);
  }

  @Override
  public TableMetadataProvider getMetadataProvider() {
    if (metadataProviderManager == null) {
      return null;
    }
    return metadataProviderManager.getTableMetadataProvider();
  }

  @Override
  public int hashCode() {
    if (hashCode == 0) {
      hashCode = Objects.hash(scanSpec, config, columns);
    }
    return hashCode;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SentinelGroupScan that = (SentinelGroupScan) o;
    return Objects.equals(scanSpec, that.scanSpec)
        && Objects.equals(config, that.config)
        && Objects.equals(columns, that.columns);
  }

  @Override
  public String toString() {
    return new PlanStringBuilder(this)
        .field("config", config)
        .field("scanSpec", scanSpec)
        .field("columns", columns)
        .toString();
  }
}
