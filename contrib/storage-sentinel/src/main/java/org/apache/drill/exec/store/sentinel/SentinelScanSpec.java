package org.apache.drill.exec.store.sentinel;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.exec.planner.logical.DrillTableSelection;

import java.util.Objects;

@JsonTypeName("sentinel-scan-spec")
public class SentinelScanSpec implements DrillTableSelection {
  private final String pluginName;
  private final String tableName;
  private final String kqlQuery;

  @JsonCreator
  public SentinelScanSpec(
      @JsonProperty("pluginName") String pluginName,
      @JsonProperty("tableName") String tableName,
      @JsonProperty("kqlQuery") String kqlQuery) {
    this.pluginName = pluginName;
    this.tableName = tableName;
    this.kqlQuery = kqlQuery != null ? kqlQuery : tableName;
  }

  public String getPluginName() {
    return pluginName;
  }

  public String getTableName() {
    return tableName;
  }

  public String getKqlQuery() {
    return kqlQuery;
  }

  @Override
  public String digest() {
    return toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SentinelScanSpec that = (SentinelScanSpec) o;
    return Objects.equals(pluginName, that.pluginName)
        && Objects.equals(tableName, that.tableName)
        && Objects.equals(kqlQuery, that.kqlQuery);
  }

  @Override
  public int hashCode() {
    return Objects.hash(pluginName, tableName, kqlQuery);
  }

  @Override
  public String toString() {
    return "SentinelScanSpec{" +
        "pluginName='" + pluginName + '\'' +
        ", tableName='" + tableName + '\'' +
        ", kqlQuery='" + kqlQuery + '\'' +
        '}';
  }
}
