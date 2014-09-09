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

import net.hydromatic.avatica.DriverVersion;
import net.hydromatic.avatica.Handler;
import net.hydromatic.avatica.HandlerImpl;
import net.hydromatic.avatica.UnregisteredDriver;

/**
 * Optiq JDBC driver.
 */
public class Driver extends UnregisteredDriver {
  public static final String CONNECT_STRING_PREFIX = "jdbc:drill:";



  public Driver() {
    super();
  }


  public static boolean load(){
    return true;
  }

  @Override
  protected String getConnectStringPrefix() {
    return CONNECT_STRING_PREFIX;
  }

  @Override
  protected String getFactoryClassName(JdbcVersion jdbcVersion) {
    switch (jdbcVersion) {
    case JDBC_30:
      return "org.apache.drill.jdbc.DrillJdbc3Factory";
    case JDBC_40:
      return "org.apache.drill.jdbc.DrillJdbc40Factory";
    case JDBC_41:
    default:
      return "org.apache.drill.jdbc.DrillJdbc41Factory";
    }
  }

  @Override
  protected DriverVersion createDriverVersion() {
    return DriverVersion.load(
        Driver.class,
        "apache-drill-jdbc.properties",
        "Drill JDBC Driver",
        "unknown version",
        "Optiq",
        "unknown version");
  }


  @Override
  protected Handler createHandler() {
    return new HandlerImpl();
  }

  static {
    new Driver().register();
  }
}
