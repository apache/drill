/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.hadoop.serialization.dto;

import java.io.Serializable;
import java.util.Map;

public class Shard implements Comparable<Shard>, Serializable {

    public enum State {
        UNASSIGNED, INITIALIZING, STARTED, RELOCATING;

        public boolean isStarted() {
            return STARTED == this;
        }
    }

    private final State state;
    private final boolean primary;
    private final String node;
    private final String relocatingNode;
    private final Integer id;
    private final String index;

    public Shard(Map<String, Object> data) {
        state = State.valueOf((String) data.get("state"));
        id = (Integer) data.get("shard");
        index = (String) data.get("index");
        relocatingNode = (String) data.get("relocating_node");
        node = (String) data.get("node");
        primary = Boolean.TRUE.equals(data.get("primary"));
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((index == null) ? 0 : index.hashCode());
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        result = prime * result + ((node == null) ? 0 : node.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Shard other = (Shard) obj;

        if (index == null) {
            if (other.index != null)
                return false;
        }
        else if (!index.equals(other.index))
            return false;

        if (id == null) {
            if (other.id != null)
                return false;
        }
        else if (!id.equals(other.id))
            return false;
        if (node == null) {
            if (other.node != null)
                return false;
        }
        else if (!node.equals(other.node))
            return false;
        return true;
    }

    public State getState() {
        return state;
    }

    public boolean isPrimary() {
        return primary;
    }

    public String getNode() {
        return node;
    }

    public String getRelocatingNode() {
        return relocatingNode;
    }

    public Integer getName() {
        return id;
    }

    public String getIndex() {
        return index;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Shard[state=").append(state).append(", primary=").append(primary).append(", node=")
        .append(node).append(", name=")
        .append(id).append(", index=").append(index).append("]");
        return builder.toString();
    }

    @Override
    public int compareTo(Shard o) {
        return id - o.id;
    }
}