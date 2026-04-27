package org.apache.drill.exec.store.sentinel;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.PlanStringBuilder;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.AbstractBase;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.physical.base.SubScan;

import org.apache.drill.exec.store.sentinel.auth.SentinelTokenManager;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

@JsonTypeName("sentinel-sub-scan")
public class SentinelSubScan extends AbstractBase implements SubScan {
  private static final String OPERATOR_TYPE = "SENTINEL";

  private final SentinelStoragePluginConfig config;
  private final SentinelScanSpec scanSpec;
  private final List<SchemaPath> columns;
  private transient SentinelTokenManager tokenManager;

  @JsonCreator
  public SentinelSubScan(
      @JsonProperty("config") SentinelStoragePluginConfig config,
      @JsonProperty("scanSpec") SentinelScanSpec scanSpec,
      @JsonProperty("columns") List<SchemaPath> columns) {
    super("sentinel");
    this.config = config;
    this.scanSpec = scanSpec;
    this.columns = columns;
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

  @JsonIgnore
  public void setTokenManager(SentinelTokenManager tokenManager) {
    this.tokenManager = tokenManager;
  }

  @JsonIgnore
  public SentinelTokenManager getTokenManager() {
    return tokenManager;
  }

  @Override
  @JsonIgnore
  public String getOperatorType() {
    return OPERATOR_TYPE;
  }

  @Override
  public <T, X, E extends Throwable> T accept(
      PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitSubScan(this, value);
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    return new SentinelSubScan(config, scanSpec, columns);
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return Collections.emptyIterator();
  }

  @Override
  public String toString() {
    return new PlanStringBuilder(this)
        .field("config", config)
        .field("scanSpec", scanSpec)
        .field("columns", columns)
        .toString();
  }

  @Override
  public int hashCode() {
    return Objects.hash(config, scanSpec, columns);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SentinelSubScan that = (SentinelSubScan) o;
    return Objects.equals(config, that.config)
        && Objects.equals(scanSpec, that.scanSpec)
        && Objects.equals(columns, that.columns);
  }
}
