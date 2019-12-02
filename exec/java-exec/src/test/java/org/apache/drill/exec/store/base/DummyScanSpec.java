package org.apache.drill.exec.store.base;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("dummy-scan-spec")
@JsonInclude(Include.NON_NULL)
public class DummyScanSpec  {

  protected final String schemaName;
  protected final String tableName;

  @JsonCreator
  public DummyScanSpec(
      @JsonProperty("schemaName") String schemaName,
      @JsonProperty("tableName") String tableName) {
    this.schemaName = schemaName;
    this.tableName = tableName;
  }

  @JsonProperty("schemaName")
  public String schemaName() { return schemaName; }

  @JsonProperty("tableName")
  public String tableName() { return tableName; }

  @Override
  public String toString() {
    PlanStringBuilder builder = new PlanStringBuilder(this);
    if (schemaName != null && !schemaName.equals(BaseStoragePlugin.DEFAULT_SCHEMA_NAME)) {
      builder.field("schema", schemaName);
    }
    builder.field("table", tableName);
    return builder.toString();
  }
}
