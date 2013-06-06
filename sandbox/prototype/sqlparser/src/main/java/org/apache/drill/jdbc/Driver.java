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

import java.io.IOException;
import java.sql.SQLException;

import net.hydromatic.linq4j.function.Function0;
import net.hydromatic.optiq.jdbc.*;
import net.hydromatic.optiq.model.ModelHandler;
import org.apache.drill.optiq.DrillPrepareImpl;

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
  protected Function0<OptiqPrepare> createPrepareFactory() {
    return new Function0<OptiqPrepare>() {
        @Override
        public OptiqPrepare apply() {
            return new DrillPrepareImpl();
        }
    };
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

      final String model = connection.getProperties().getProperty("model");
      if (model != null) {
        try {
          new ModelHandler(connection, model);
        } catch (IOException e) {
          throw new SQLException(e);
        }
      }

      // The "schema" parameter currently gives a name to the schema. In future
      // it will choose a schema that (presumably) already exists.
      final String schemaName =
          connection.getProperties().getProperty("schema");
      if (schemaName != null) {
        connection.setSchema(schemaName);
      }
    }
  }

    /*

optiq work
==========

1. todo: test can cast(<any> as varchar)  (or indeed to any type)

2. We declare a variant record by adding a '_MAP any' field. It's nice that
there can be some declared fields, and some undeclared. todo: Better syntactic
sugar.

*/
}

// End Driver.java
