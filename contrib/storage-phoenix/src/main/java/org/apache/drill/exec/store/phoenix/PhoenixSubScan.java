package org.apache.drill.exec.store.phoenix;

import java.util.Iterator;
import java.util.List;

import org.apache.drill.common.PlanStringBuilder;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.AbstractBase;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableSet;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("phoenix-sub-scan")
public class PhoenixSubScan extends AbstractBase implements SubScan {

  private final List<SchemaPath> columns;
  private final PhoenixScanSpec scanSpec;

  public PhoenixSubScan(
      @JsonProperty("scanSpec") PhoenixScanSpec scanSpec,
      @JsonProperty("columns") List<SchemaPath> columns) {
    super("user-if-needed");
    this.scanSpec = scanSpec;
    this.columns = columns;
  }

  public List<SchemaPath> getColumns() {
    return columns;
  }

  public PhoenixScanSpec getScanSpec() {
    return scanSpec;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitSubScan(this, value);
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
    return new PhoenixSubScan(scanSpec, columns);
  }

  @Override
  public String getOperatorType() {
    return "PHOENIX";
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return ImmutableSet.<PhysicalOperator>of().iterator();
  }

  @Override
  public String toString() {
    return new PlanStringBuilder(this)
      .field("columns", columns)
      .field("scanSpec", scanSpec)
      .toString();
  }
}
