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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Jersey 3 Application class that IS a DrillRestServer instance.
 *
 * Jersey 3 requires Application classes to have a no-arg constructor, but DrillRestServer
 * requires dependencies (WorkManager, ServletContext, Drillbit). This class extends
 * DrillRestServer and retrieves those dependencies from a static holder, allowing Jersey
 * to instantiate a fully configured ResourceConfig with all HK2 binders intact.
 *
 * This is the KEY solution: by making this class EXTEND DrillRestServer, we ensure that
 * Jersey gets a proper ResourceConfig with all binders, not just a wrapper that delegates.
 */
public class DrillRestServerApplication extends DrillRestServer {
  private static final Logger logger = LoggerFactory.getLogger(DrillRestServerApplication.class);

  /**
   * No-arg constructor required by Jersey.
   * This retrieves dependencies from the holder and calls the parent DrillRestServer constructor.
   */
  public DrillRestServerApplication() {
    super(
        DrillRestServerHolder.getWorkManager(),
        DrillRestServerHolder.getServletContext(),
        DrillRestServerHolder.getDrillbit()
    );
    logger.info("DrillRestServerApplication (extends DrillRestServer) instantiated with dependencies from holder");
  }
}
