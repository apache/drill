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
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.drill.exec.resourcemgr.NodeResources;

import java.util.Map;
@JsonTypeName(ClusterStateBlob.NAME)
public class ClusterStateBlob extends AbstractRMStateBlob {
  public static final String NAME = "cluster_usage";

  // Stores the resources available out of total resources on each node of the cluster
  @JsonDeserialize(contentUsing = NodeResources.NodeResourcesDe.class)
  private Map<String, NodeResources> clusterState;

  @JsonCreator
  public ClusterStateBlob(@JsonProperty("version") int version,
                          @JsonProperty("clusterState") Map<String, NodeResources> clusterState) {
    super(version);
    this.clusterState = clusterState;
  }

  public Map<String, NodeResources> getClusterState() {
    return clusterState;
  }

  public void setClusterState(Map<String, NodeResources> clusterState) {
    this.clusterState = clusterState;
  }

  @Override
  public int hashCode() {
    int result = 31;
    result = result ^ Integer.hashCode(version);
    result = result ^ clusterState.hashCode();
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

    ClusterStateBlob other = (ClusterStateBlob) obj;
    return this.version == other.getVersion() && this.clusterState.equals(other.getClusterState());
  }
}