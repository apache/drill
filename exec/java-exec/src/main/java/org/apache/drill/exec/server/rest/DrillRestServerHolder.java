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
package org.apache.drill.exec.server.rest;

import jakarta.servlet.ServletContext;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.work.WorkManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holder for DrillRestServer dependencies needed by DrillRestServerApplication.
 *
 * Since Jersey requires Application classes to have a no-arg constructor,
 * DrillRestServerApplication needs to retrieve its dependencies from somewhere.
 * This holder stores the WorkManager, ServletContext, and Drillbit so that
 * DrillRestServerApplication can instantiate itself properly.
 *
 * This is a thread-safe holder using synchronization to ensure visibility of data.
 */
public class DrillRestServerHolder {
  private static final Logger logger = LoggerFactory.getLogger(DrillRestServerHolder.class);
  private static volatile WorkManager workManager;
  private static volatile ServletContext servletContext;
  private static volatile Drillbit drillbit;

  public static synchronized void setDependencies(WorkManager wm, ServletContext sc, Drillbit db) {
    logger.info("Setting DrillRestServer dependencies in holder");
    DrillRestServerHolder.workManager = wm;
    DrillRestServerHolder.servletContext = sc;
    DrillRestServerHolder.drillbit = db;
    logger.info("Dependencies set successfully");
  }

  public static synchronized WorkManager getWorkManager() {
    logger.info("Getting WorkManager from holder: " + (workManager != null ? "NOT NULL" : "NULL"));
    if (workManager == null) {
      logger.error("WorkManager is null - dependencies may not have been initialized", new Exception("Stack trace for debugging"));
    }
    return workManager;
  }

  public static synchronized ServletContext getServletContext() {
    logger.info("Getting ServletContext from holder: " + (servletContext != null ? "NOT NULL" : "NULL"));
    if (servletContext == null) {
      logger.error("ServletContext is null - dependencies may not have been initialized", new Exception("Stack trace for debugging"));
    }
    return servletContext;
  }

  public static synchronized Drillbit getDrillbit() {
    logger.info("Getting Drillbit from holder: " + (drillbit != null ? "NOT NULL" : "NULL"));
    if (drillbit == null) {
      logger.error("Drillbit is null - dependencies may not have been initialized", new Exception("Stack trace for debugging"));
    }
    return drillbit;
  }
}
