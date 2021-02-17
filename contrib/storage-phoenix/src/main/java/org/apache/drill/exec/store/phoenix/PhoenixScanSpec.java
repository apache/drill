package org.apache.drill.exec.store.phoenix;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("phoenix-scan-spec")
public class PhoenixScanSpec {

  private final String tableName;

  @JsonCreator
  public PhoenixScanSpec(String tableName) {
    this.tableName = tableName;
  }

  public String getTableName() {
    return tableName;
  }
}
