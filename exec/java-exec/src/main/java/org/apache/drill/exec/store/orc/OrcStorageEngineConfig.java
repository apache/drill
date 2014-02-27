package org.apache.drill.exec.store.orc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.logical.StorageEngineConfigBase;

@JsonTypeName("orc")
public class OrcStorageEngineConfig extends StorageEngineConfigBase {
  private String dfsName;

  public String getDfsName() {
    return dfsName;
  }

  @JsonCreator
  public OrcStorageEngineConfig(@JsonProperty("dfsName") String dfsName) {
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

    OrcStorageEngineConfig that = (OrcStorageEngineConfig) o;

    if (dfsName != null ? !dfsName.equals(that.dfsName) : that.dfsName != null) return false;

    return true;
  }
}
