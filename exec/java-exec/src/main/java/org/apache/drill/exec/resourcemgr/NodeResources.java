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
package org.apache.drill.exec.resourcemgr;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.apache.drill.common.DrillNode;

import java.io.IOException;
import java.util.Map;

/**
 * Provides resources for a node in cluster. Currently it is used to support only 2 kind of resources:
 * <ul>
 * <li>Memory</li>
 * <li>Virtual CPU count</li>
 * </ul>
 * It also has a version field to support extensibility in future to add other resources like network, disk, etc
 */
public class NodeResources {

  private final int version;

  private long memoryInBytes;

  private int numVirtualCpu;

  private static final int CURRENT_VERSION = 1;

  public NodeResources(long memoryInBytes, int numVirtualCpu) {
    this(CURRENT_VERSION, memoryInBytes, numVirtualCpu);
  }

  @JsonCreator
  public NodeResources(@JsonProperty("version") int version,
                       @JsonProperty("memoryInBytes") long memoryInBytes,
                       @JsonProperty("numVirtualCpu") int numVirtualCpu) {
    this.version = version;
    this.memoryInBytes = memoryInBytes;
    this.numVirtualCpu = numVirtualCpu;
  }

  public NodeResources(long memoryInBytes, int numPhysicalCpu, int vFactor) {
    this(CURRENT_VERSION, memoryInBytes, numPhysicalCpu * vFactor);
  }

  public long getMemoryInBytes() {
    return memoryInBytes;
  }

  @JsonIgnore
  public long getMemoryInMB() {
    return Math.round((memoryInBytes / 1024L) / 1024L);
  }

  @JsonIgnore
  public long getMemoryInGB() {
    return Math.round(getMemoryInMB() / 1024L);
  }

  public int getNumVirtualCpu() {
    return numVirtualCpu;
  }

  public int getVersion() {
    return version;
  }

  public void setMemoryInBytes(long memoryInBytes) {
    this.memoryInBytes = memoryInBytes;
  }

  public void setNumVirtualCpu(int numVCpu) {
    this.numVirtualCpu = numVCpu;
  }

  public void add(NodeResources other) {
    if (other == null) {
      return;
    }
    this.numVirtualCpu += other.getNumVirtualCpu();
    this.memoryInBytes += other.getMemoryInBytes();
  }

  public static Map<DrillNode, NodeResources> merge(Map<DrillNode, NodeResources> to,
                                                           Map<DrillNode, NodeResources> from) {
    to.entrySet().stream().forEach((toEntry) -> toEntry.getValue().add(from.get(toEntry.getKey())));
    return to;
  }

  @Override
  public String toString() {
    return "{ Version: " + version + ", MemoryInBytes: " + memoryInBytes + ", VirtualCPU: " + numVirtualCpu + " }";
  }

  @Override
  public int hashCode() {
    int result = 31 ^ Integer.hashCode(version);
    result = result ^ Long.hashCode(numVirtualCpu);
    result = result ^ Long.hashCode(memoryInBytes);
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
    NodeResources other = (NodeResources) obj;
    return this.version == other.getVersion() && this.numVirtualCpu == other.getNumVirtualCpu() &&
      this.memoryInBytes == other.getMemoryInBytes();
  }

  public static NodeResources create() {
    return create(0,0);
  }

  public static NodeResources create(int cpu) {
    return create(cpu,0);
  }

  public static NodeResources create(int cpu, long memory) {
    return new NodeResources(CURRENT_VERSION, memory, cpu);
  }

  public static class NodeResourcesDe extends StdDeserializer<NodeResources> {

    private static final ObjectMapper mapper = new ObjectMapper();

    public NodeResourcesDe() {
      super(NodeResources.class);
    }

    @Override
    public NodeResources deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
      return mapper.readValue(p, NodeResources.class);
    }
  }
}
