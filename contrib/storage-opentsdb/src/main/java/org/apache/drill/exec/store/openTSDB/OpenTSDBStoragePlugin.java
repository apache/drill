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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.openTSDB.client.OpenTSDB;
import org.apache.drill.exec.store.openTSDB.schema.OpenTSDBSchemaFactory;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

import java.io.IOException;

@Slf4j
public class OpenTSDBStoragePlugin extends AbstractStoragePlugin {

    private final DrillbitContext context;
    private final OpenTSDBStoragePluginConfig engineConfig;
    private final OpenTSDBSchemaFactory schemaFactory;

    @SuppressWarnings("unused")
    private final String name;
    private final OpenTSDB client;

    public OpenTSDBStoragePlugin(OpenTSDBStoragePluginConfig configuration, DrillbitContext context, String name) throws IOException {
        this.context = context;
        this.schemaFactory = new OpenTSDBSchemaFactory(this, name);
        this.engineConfig = configuration;
        this.name = name;
        this.client = new Retrofit.Builder()
                .baseUrl("http://" + configuration.getConnection())
                .addConverterFactory(JacksonConverterFactory.create())
                .build()
                .create(OpenTSDB.class);
    }

    @Override
    public void start() throws IOException {
    }

    public OpenTSDB getClient() {
        return client;
    }

    @Override
    public void close() throws Exception {
//        client.close();
    }

    public DrillbitContext getContext() {
        return this.context;
    }

    @Override
    public boolean supportsRead() {
        return true;
    }

    @Override
    public OpenTSDBGroupScan getPhysicalScan(String userName, JSONOptions selection) throws IOException {
        OpenTSDBScanSpec scanSpec = selection.getListWith(new ObjectMapper(), new TypeReference<OpenTSDBScanSpec>() {
        });
        return new OpenTSDBGroupScan(this, scanSpec, null);
    }

    @Override
    public boolean supportsWrite() {
        return true;
    }

    @Override
    public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
        schemaFactory.registerSchemas(schemaConfig, parent);
    }

    @Override
    public OpenTSDBStoragePluginConfig getConfig() {
        return engineConfig;
    }

}
