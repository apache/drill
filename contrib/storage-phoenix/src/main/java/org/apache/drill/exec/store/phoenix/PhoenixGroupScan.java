package org.apache.drill.exec.store.phoenix;

import java.util.List;
import java.util.Objects;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.PhysicalOperatorSetupException;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.ScanStats.GroupScanProperty;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("phoenix-scan")
public class PhoenixGroupScan extends AbstractGroupScan {

  private final List<SchemaPath> columns;
  private final PhoenixScanSpec scanSpec;
  private final ScanStats scanStats;

  private int hashCode;

  public PhoenixGroupScan(PhoenixScanSpec scanSpec) {
    super("no-user");
    this.scanSpec = scanSpec;
    this.columns = ALL_COLUMNS;
    this.scanStats = computeScanStats();
  }

  public PhoenixGroupScan(PhoenixGroupScan groupScan) {
    super(groupScan);
    this.scanSpec = groupScan.scanSpec;
    this.columns = groupScan.columns;
    this.scanStats = groupScan.scanStats;
  }

  public PhoenixGroupScan(PhoenixGroupScan groupScan, List<SchemaPath> columns) {
    super(groupScan);
    this.scanSpec = groupScan.scanSpec;
    this.columns = columns;
    this.scanStats = computeScanStats();
  }

  @JsonCreator
  public PhoenixGroupScan(
      @JsonProperty("columns") List<SchemaPath> columns,
      @JsonProperty("scanSpec") PhoenixScanSpec scanSpec) {
    super("no-user");
    this.columns = columns;
    this.scanSpec = scanSpec;
    this.scanStats = computeScanStats();
  }

  @Override
  @JsonProperty("columns")
  public List<SchemaPath> getColumns() {
    return columns;
  }

  @JsonProperty("scanSpec")
  public PhoenixScanSpec getScanSpec() {
    return scanSpec;
  }

  @Override
  public void applyAssignments(List<DrillbitEndpoint> endpoints) throws PhysicalOperatorSetupException {  }

  @Override
  public SubScan getSpecificScan(int minorFragmentId) throws ExecutionSetupException {
    return new PhoenixSubScan(scanSpec, columns);
  }

  @Override
  public int getMaxParallelizationWidth() {
    return 1;
  }

  @Override
  public String getDigest() {
    return toString();
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
    return new PhoenixGroupScan(columns, scanSpec);
  }

  @Override
  public GroupScan clone(List<SchemaPath> columns) {
    return new PhoenixGroupScan(this, columns);
  }

  @Override
  public ScanStats getScanStats() {
    return scanStats;
  }

  @Override
  public int hashCode() {
    if(hashCode == 0) {
      hashCode = Objects.hash(scanSpec, columns);
    }
    return hashCode;
  }

  @Override
  public boolean equals(Object obj) {
    if(this == obj) {
      return true;
    }
    if(obj == null || getClass() != obj.getClass()) {
      return false;
    }
    PhoenixGroupScan groupScan = (PhoenixGroupScan) obj;
    return Objects.equals(scanSpec, groupScan.getScanSpec()) && Objects.equals(columns, groupScan.getColumns());
  }

  private ScanStats computeScanStats() {
    int estRowCount = 10_000;
    double cpuRatio = 1.0;
    return new ScanStats(GroupScanProperty.NO_EXACT_ROW_COUNT, estRowCount, cpuRatio, 0);
  }
}
