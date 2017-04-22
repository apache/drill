/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.openTSDB;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import lombok.extern.slf4j.Slf4j;
import org.apache.drill.common.logical.StoragePluginConfigBase;

import java.io.IOException;

@Slf4j
@JsonTypeName(OpenTSDBStoragePluginConfig.NAME)
public class OpenTSDBStoragePluginConfig extends StoragePluginConfigBase {

    public static final String NAME = "openTSDB";

    private final String connection;

    @JsonCreator
    public OpenTSDBStoragePluginConfig(@JsonProperty("connection") String connection) throws IOException {
        this.connection = connection;
    }

    public String getConnection() {
        return connection;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        OpenTSDBStoragePluginConfig that = (OpenTSDBStoragePluginConfig) o;

        return connection != null ? connection.equals(that.connection) : that.connection == null;

    }

    @Override
    public int hashCode() {
        return connection != null ? connection.hashCode() : 0;
    }
}
