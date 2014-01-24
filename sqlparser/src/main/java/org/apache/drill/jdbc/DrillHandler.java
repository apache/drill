/**
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
package org.apache.drill.jdbc;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import net.hydromatic.linq4j.QueryProvider;
import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.optiq.MutableSchema;
import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.impl.java.MapSchema;
import net.hydromatic.optiq.jdbc.HandlerImpl;
import net.hydromatic.optiq.jdbc.OptiqConnection;
import net.hydromatic.optiq.model.ModelHandler;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.StorageEngineConfig;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.coord.ZKClusterCoordinator;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.apache.drill.exec.store.SchemaProvider;
import org.apache.drill.exec.store.SchemaProviderRegistry;
import org.apache.drill.exec.store.hive.HiveStorageEngine.HiveSchemaProvider;
import org.apache.drill.exec.store.json.JsonSchemaProvider;
import org.apache.drill.exec.store.parquet.ParquetSchemaProvider;
import org.apache.drill.sql.client.full.FileSystemSchema;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.io.Resources;
import org.apache.drill.sql.client.full.HiveSchema;

public class DrillHandler extends HandlerImpl {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillHandler.class);

  private ClusterCoordinator coordinator;
  private volatile DrillClient client;
  private Drillbit bit;
  private DrillConfig config = DrillConfig.create();
  private SchemaProviderRegistry registry;
  private final boolean ref;
  
  public DrillHandler(boolean ref){
    this.ref = ref;
  }

  public void onConnectionInit(OptiqConnection connection) throws SQLException {
    super.onConnectionInit(connection);

    final Properties p = connection.getProperties();
    
    if (ref) {
      final String model = p.getProperty("model");
      if (model != null) {
        if (model != null) {
          try {
            new ModelHandler(connection, model);
          } catch (IOException e) {
            throw new SQLException(e);
          }
        }
      }
    } else {
      
      registry = new SchemaProviderRegistry(config);
      
      Preconditions.checkArgument(bit == null);
      Preconditions.checkArgument(client == null);
      Preconditions.checkArgument(coordinator == null);

      final String zk = connection.getProperties().getProperty("zk");

      try {
        String enginesData = Resources.toString(Resources.getResource("storage-engines.json"), Charsets.UTF_8);

        StorageEngines engines = config.getMapper().readValue(enginesData, StorageEngines.class);

        if (zk != null) {
          coordinator = new ZKClusterCoordinator(config, zk);
          coordinator.start(10000);
          DrillClient cl = new DrillClient(config, coordinator);
          cl.connect();
          client = cl;
        } else {

          RemoteServiceSet local = RemoteServiceSet.getLocalServiceSet();
          this.coordinator = local.getCoordinator();
          bit = new Drillbit(config, local);
          bit.run();

          DrillClient cl = new DrillClient(config, coordinator);
          cl.connect();
          client = cl;
        }
        
        MutableSchema rootSchema = connection.getRootSchema();

        for (Map.Entry<String, StorageEngineConfig> entry : engines) {
          SchemaProvider provider = registry.getSchemaProvider(entry.getValue());
          Schema schema = getSchema(provider, client, entry.getKey(), entry.getValue(), rootSchema);
          rootSchema.addSchema(entry.getKey(), schema);
        }

        rootSchema.addSchema(
            "--FAKE--",
            new FakeSchema(rootSchema, rootSchema.getQueryProvider(), rootSchema.getTypeFactory(), "fake", rootSchema
                .getExpression()));
        
      } catch (Exception ex) {
        System.out.println(ex);
        logger.error("Failure while setting up jdbc handler", ex);
        throw new SQLException("Failure trying to connect to Drill.", ex);
      }
    }

    // The "schema" parameter currently gives a name to the schema. In future
    // it will choose a schema that (presumably) already exists.
    final String schemaName = connection.getProperties().getProperty("schema");
    if (schemaName != null) {
      connection.setSchema(schemaName);
    }

    final String catalogName = connection.getProperties().getProperty("catalog");
    if (catalogName != null) {
      connection.setCatalog(catalogName);
    }
  }

  private Schema getSchema(SchemaProvider provider, DrillClient client, String name, StorageEngineConfig config, Schema rootSchema)
    throws SQLException {
    if (provider instanceof ParquetSchemaProvider || provider instanceof JsonSchemaProvider) {
      return new FileSystemSchema(client, config, provider,
        rootSchema.getTypeFactory(), rootSchema, name, rootSchema.getExpression(),
        rootSchema.getQueryProvider());
    } else if (provider instanceof HiveSchemaProvider) {
      return new HiveSchema(client, config, provider,
        rootSchema.getTypeFactory(), rootSchema, name, rootSchema.getExpression(),
        rootSchema.getQueryProvider());
    }

    throw new SQLException("Unknown schema provider");
  }

  public class FakeSchema extends MapSchema {

    public FakeSchema(Schema parentSchema, QueryProvider queryProvider, JavaTypeFactory typeFactory, String name,
        Expression expression) {
      super(parentSchema, queryProvider, typeFactory, name, expression);

    }

    public DrillClient getClient() {
      return client;
    }
  }

  public DrillClient getClient() {
    return client;
  }

  @Override
  public void onConnectionClose(OptiqConnection connection) throws RuntimeException {
    super.onConnectionClose(connection);
    if (client != null)
      client.close();
    if (bit != null)
      bit.close();
    if (coordinator != null)
      try {
        coordinator.close();
      } catch (IOException e) {
        throw new RuntimeException("Failure closing coordinator.", e);
      }
    bit = null;
    client = null;
    coordinator = null;
  }

}
/*
 * 
 * optiq work ==========
 * 
 * 1. todo: test can cast(<any> as varchar) (or indeed to any type)
 * 
 * 2. We declare a variant record by adding a '_MAP any' field. It's nice that there can be some declared fields, and
 * some undeclared. todo: Better syntactic sugar.
 */