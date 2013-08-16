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

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.StorageEngineConfig;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.coord.ZKClusterCoordinator;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.apache.drill.exec.store.SchemaProvider;
import org.apache.drill.exec.store.SchemaProviderRegistry;
import org.apache.drill.sql.client.full.FileSystemSchema;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.io.Resources;

public class DrillHandler extends HandlerImpl {

  private ClusterCoordinator coordinator;
  private volatile DrillClient client;
  private Drillbit bit;
  private DrillConfig config = DrillConfig.create();
  private SchemaProviderRegistry registry;

  public void onConnectionInit(OptiqConnection connection) throws SQLException {
    super.onConnectionInit(connection);

    registry = new SchemaProviderRegistry(config);
    final Properties p = connection.getProperties();
    Preconditions.checkArgument(bit == null);
    Preconditions.checkArgument(client == null);
    Preconditions.checkArgument(coordinator == null);
    // final String model = p.getProperty("model");
    // if (model != null) {
    // if (model != null) {
    // try {
    // new ModelHandler(connection, model);
    // } catch (IOException e) {
    // throw new SQLException(e);
    // }
    // }
    // }

    final String zk = connection.getProperties().getProperty("zk");

    try {
      String enginesData = Resources.toString(Resources.getResource("storage-engines.json"), Charsets.UTF_8);

      StorageEngines engines = config.getMapper().readValue(enginesData, StorageEngines.class);
      MutableSchema rootSchema = connection.getRootSchema();

      for (Map.Entry<String, StorageEngineConfig> entry : engines) {
        SchemaProvider provider = registry.getSchemaProvider(entry.getValue());
        FileSystemSchema schema = new FileSystemSchema(client, entry.getValue(), provider, rootSchema.getTypeFactory(),
            rootSchema, entry.getKey(), rootSchema.getExpression(), rootSchema.getQueryProvider());
        rootSchema.addSchema(entry.getKey(), schema);
      }

      rootSchema.addSchema("--FAKE--", new FakeSchema(rootSchema, rootSchema.getQueryProvider(), rootSchema.getTypeFactory(), "fake", rootSchema.getExpression()));
      
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
    } catch (Exception ex) {
      throw new SQLException("Failure trying to connect to Drill.", ex);
    }

    // The "schema" parameter currently gives a name to the schema. In future
    // it will choose a schema that (presumably) already exists.
    final String schemaName = connection.getProperties().getProperty("schema");
    if (schemaName != null) {
      connection.setSchema(schemaName);
    }

  }
  
  public class FakeSchema extends MapSchema{

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