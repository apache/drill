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

package org.apache.drill.exec.store.cassandra;


import org.apache.drill.common.logical.StoragePluginConfig;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

@JsonTypeName(CassandraStoragePluginConfig.NAME)
public class CassandraStoragePluginConfig extends StoragePluginConfig {
    static final Logger logger = LoggerFactory.getLogger(CassandraStoragePluginConfig.class);

    public static final String NAME = "cassandra";

    private List<String> hosts;

    private int port;

    @JsonCreator
    public CassandraStoragePluginConfig( @JsonProperty("hosts") List<String> hosts, @JsonProperty("port") int port ) {
        this.hosts = hosts;
        this.port = port;
    }

    @Override
    public boolean  equals(Object that) {
        if (this == that) {
            return true;
        } else if (that == null || getClass() != that.getClass()) {
            return false;
        }
        CassandraStoragePluginConfig thatConfig = (CassandraStoragePluginConfig) that;
        return (this.hosts.equals(thatConfig.hosts)) && (this.port==thatConfig.port);

    }

    @Override
    public int hashCode() {
        return this.hosts != null ? this.hosts.hashCode() : 0;
    }

    public List<String> getHosts() {
        return hosts;
    }

    public int getPort(){ return port; }
}
