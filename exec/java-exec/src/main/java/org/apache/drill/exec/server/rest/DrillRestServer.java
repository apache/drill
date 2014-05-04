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
package org.apache.drill.exec.server.rest;

import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.work.WorkManager;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.server.mvc.freemarker.FreemarkerMvcFeature;

public class DrillRestServer extends ResourceConfig {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillRestServer.class);

  public DrillRestServer(final WorkManager workManager) {
//    registerClasses(HelloResource.class);
    register(JacksonFeature.class);
    register(DrillRoot.class);
    register(FreemarkerMvcFeature.class);
    property(ServerProperties.METAINF_SERVICES_LOOKUP_DISABLE, true);
    register(new AbstractBinder() {
      @Override
      protected void configure() {
        bind(workManager).to(WorkManager.class);
        bind(new DrillClient()).to(DrillClient.class);
      }
    });
  }
}
