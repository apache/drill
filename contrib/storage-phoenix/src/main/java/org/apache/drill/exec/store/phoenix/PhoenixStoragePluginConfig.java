package org.apache.drill.exec.store.phoenix;

import java.util.Objects;
import java.util.Properties;

import org.apache.drill.common.PlanStringBuilder;
import org.apache.drill.common.logical.StoragePluginConfig;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName(PhoenixStoragePluginConfig.NAME)
public class PhoenixStoragePluginConfig extends StoragePluginConfig {

  public static final String NAME = "phoenix";

  public String driverName = "org.apache.phoenix.queryserver.client.Driver";
  public String host;
  public int port = 8765;
  public String username;
  public String password;
  public Properties props;

  @JsonCreator
  public PhoenixStoragePluginConfig(
      @JsonProperty("driverName") String driverName,
      @JsonProperty("host") String host,
      @JsonProperty("port") int port,
      @JsonProperty("username") String username,
      @JsonProperty("password") String password,
      @JsonProperty("props") Properties props) {
    this.driverName = driverName;
    this.host = host;
    this.port = port;
    this.username = username;
    this.password = password;
    this.props = props;
  }

  @JsonProperty("driverName")
  public String getDriverName() {
    return driverName;
  }

  @JsonProperty("host")
  public String getHost() {
    return host;
  }

  @JsonProperty("port")
  public int getPort() {
    return port;
  }

  @JsonProperty("username")
  public String getUsername() {
    return username;
  }

  @JsonProperty("password")
  public String getPassword() {
    return password;
  }

  @JsonProperty("props")
  public Properties getProps() {
    return props;
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o == null || !(o instanceof PhoenixStoragePluginConfig) ) {
      return false;
    }
    return Objects.equals(this.host, ((PhoenixStoragePluginConfig)o).getHost()) &&
        Objects.equals(this.port, ((PhoenixStoragePluginConfig)o).getPort());
  }

  @Override
  public int hashCode() {
    return Objects.hash(host, port);
  }

  @Override
  public String toString() {
    return new PlanStringBuilder(PhoenixStoragePluginConfig.NAME)
        .field("driverName", driverName)
        .field("host", host)
        .field("port", port)
        .field("username", username)
        .toString();
  }
}
