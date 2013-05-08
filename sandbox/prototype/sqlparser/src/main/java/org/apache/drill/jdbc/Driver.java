/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.jdbc;

import java.sql.SQLException;
import java.util.Collections;

import net.hydromatic.optiq.MutableSchema;
import net.hydromatic.optiq.impl.java.MapSchema;
import net.hydromatic.optiq.jdbc.DriverVersion;
import net.hydromatic.optiq.jdbc.Handler;
import net.hydromatic.optiq.jdbc.HandlerImpl;
import net.hydromatic.optiq.jdbc.OptiqConnection;
import net.hydromatic.optiq.jdbc.UnregisteredDriver;

import org.apache.drill.exec.ref.rops.DataWriter.ConverterType;
import org.apache.drill.exec.ref.rse.ClasspathRSE.ClasspathInputConfig;
import org.apache.drill.exec.ref.rse.ClasspathRSE.ClasspathRSEConfig;

/**
 * JDBC driver for Apache Drill.
 */
public class Driver extends UnregisteredDriver {
  public static final String CONNECT_STRING_PREFIX = "jdbc:drill:";

  static {
    new Driver().register();
  }

  protected String getConnectStringPrefix() {
    return CONNECT_STRING_PREFIX;
  }

  protected DriverVersion createDriverVersion() {
    return new DrillDriverVersion();
  }

  @Override
  protected Handler createHandler() {
    return new DrillHandler();
  }

  private static class DrillHandler extends HandlerImpl {
    public void onConnectionInit(OptiqConnection connection)
        throws SQLException
    {
      super.onConnectionInit(connection);

      // The "schema" parameter currently gives a name to the schema. In future
      // it will choose a schema that (presumably) already exists.
      final String schemaName =
          connection.getProperties().getProperty("schema");
      if (schemaName == null) {
        throw new SQLException("schema connection property must be specified");
      }
      final MutableSchema rootSchema = connection.getRootSchema();
      final MapSchema schema =
          MapSchema.create(connection, rootSchema, schemaName);

      connection.setSchema(schemaName);
      final ClasspathRSEConfig rseConfig = new ClasspathRSEConfig();
      final ClasspathInputConfig inputConfig = new ClasspathInputConfig();
      inputConfig.path = "/donuts.json";
      inputConfig.type = ConverterType.JSON; 
      


      // "tables" is a temporary parameter. We should replace with
      // "schemaUri", which is the URI of a schema.json file, or the name of a
      // schema from a server, if drill is running in a server.
      final String tables = connection.getProperties().getProperty("tables");
      if (tables == null) {
        throw new SQLException("tables connection property must be specified");
      }
      final String[] tables2 = tables.split(",");
      for (String table : tables2) {
        DrillTable.addTable(connection.getTypeFactory(), schema, table,
            rseConfig, inputConfig);
      }
    }
  }

/*

optiq work
==========

1. todo: test can cast(<any> as varchar)  (or indeed to any type)

2. We declare a variant record by adding a '_extra any' field. It's nice that
there can be some declared fields, and some undeclared. todo: Better syntactic
sugar.

*/
}

// End Driver.java
