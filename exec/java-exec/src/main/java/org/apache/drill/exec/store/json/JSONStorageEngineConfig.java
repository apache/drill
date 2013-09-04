package org.apache.drill.exec.store.json;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.logical.StorageEngineConfigBase;

@JsonTypeName("json")
public class JSONStorageEngineConfig extends StorageEngineConfigBase {
  private String dfsName;

  public String getDfsName() {
    return dfsName;
  }

  @JsonCreator
  public JSONStorageEngineConfig(@JsonProperty("dfsName") String dfsName) {
    this.dfsName = dfsName;
  }

  @Override
  public int hashCode() {
    return dfsName != null ? dfsName.hashCode() : 0;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    JSONStorageEngineConfig that = (JSONStorageEngineConfig) o;

    if (dfsName != null ? !dfsName.equals(that.dfsName) : that.dfsName != null) return false;

    return true;
  }
}
