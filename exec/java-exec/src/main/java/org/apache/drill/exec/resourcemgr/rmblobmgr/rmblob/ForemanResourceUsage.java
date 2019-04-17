/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.resourcemgr.rmblobmgr.rmblob;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.apache.drill.exec.resourcemgr.NodeResources;

import java.io.IOException;
import java.util.Map;

public class ForemanResourceUsage {

  private int version;

  private Map<String, NodeResources> foremanUsage;

  private int runningCount;

  @JsonCreator
  public ForemanResourceUsage(@JsonProperty("version") int version,
                              @JsonProperty("foremanUsage") Map<String, NodeResources> foremanUsage,
                              @JsonProperty("runningCount") int runningCount) {
    this.version = version;
    this.foremanUsage = foremanUsage;
    this.runningCount = runningCount;
  }

  public int getVersion() {
    return version;
  }

  public Map<String, NodeResources> getForemanUsage() {
    return foremanUsage;
  }

  public int getRunningCount() {
    return runningCount;
  }

  public void setRunningCount(int runningCount) {
    this.runningCount = runningCount;
  }

  public void setForemanUsage(Map<String, NodeResources> foremanUsage) {
    this.foremanUsage = foremanUsage;
  }

  @Override
  public int hashCode() {
     int result = 31;
     result = result ^ Integer.hashCode(version);
     result = result ^ Integer.hashCode(runningCount);
     result = result ^ foremanUsage.hashCode();
     return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || (obj.getClass() != this.getClass())) {
      return false;
    }

    if (this == obj) {
      return true;
    }

    ForemanResourceUsage other = (ForemanResourceUsage) obj;
    return this.version == other.getVersion() && this.runningCount == other.getRunningCount() &&
      this.foremanUsage.equals(other.getForemanUsage());
  }

  public static class ForemanResourceUsageDe extends StdDeserializer<ForemanResourceUsage> {

    private static final ObjectMapper mapper = new ObjectMapper();

    public ForemanResourceUsageDe() {
      super(ForemanResourceUsage.class);
    }

    @Override
    public ForemanResourceUsage deserialize(JsonParser p, DeserializationContext ctxt)
      throws IOException, JsonProcessingException {
      return mapper.readValue(p, ForemanResourceUsage.class);
    }
  }
}
