/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ranger.authorization.drill.authorizer;

import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Singleton wrapper around {@link RangerBasePlugin} for the Drill service type.
 *
 * <p>Initialized once at Drillbit startup with the service name configured in
 * {@code ranger-drill-security.xml}. After initialization,
 * {@link #isAccessAllowed(RangerAccessRequest)}
 * performs local in-memory policy evaluation (policies are pulled periodically by the
 * {@code PolicyRefresher} background thread, identical to the Hive plugin).</p>
 */
public class RangerBaseAuthorizer {
  private static final Logger LOG = LoggerFactory.getLogger(RangerBaseAuthorizer.class);

  private static RangerBaseAuthorizer instance;
  private volatile RangerBasePlugin plugin;

  private RangerBaseAuthorizer() {

  }

  private static class LazyHolder {
    private static final RangerBaseAuthorizer INSTANCE = new RangerBaseAuthorizer();
  }

  public static RangerBaseAuthorizer getInstance() {
    return LazyHolder.INSTANCE;
  }

  /**
   * Initializes the Ranger plugin. The {@code serviceType} MUST be "drill" to match the
   * service-def registered in Ranger Admin.
   *
   * @param serviceName the service instance name (matches {@code ranger.plugin.drill.service.name})
   */
  public synchronized void init(String serviceName) {
    if (plugin != null) {
      return;
    }
    try {
      plugin = new RangerDrillPlugin(serviceName);
      plugin.setResultProcessor(new RangerDefaultAuditHandler());
      plugin.init();
      LOG.info("RangerPlugin initialized successfully for serviceName: {}", serviceName);
    } catch (Exception e) {
      plugin = null;
      throw new RuntimeException("Failed to initialize RangerPlugin", e);
    }
  }

  public boolean isAccessAllowed(RangerAccessRequest request) {
    if (plugin == null) {
      LOG.error("Plugin not initialized!");
      return false;
    }
    RangerAccessResult result = plugin.isAccessAllowed(request);
    return result != null && result.getIsAllowed();
  }

  /**
   * Forces an immediate policy refresh. Normally NOT needed — the {@code PolicyRefresher}
   * background thread pulls policies periodically. Kept for administrative use only.
   */
  public void refreshPoliciesNow() {
    if (plugin != null) {
      plugin.refreshPoliciesAndTags();
    }
  }

  public void cleanUp() {
    if (plugin != null) {
      plugin.cleanup();
    }
  }
}
