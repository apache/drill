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
package org.apache.drill.exec.security.ranger;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Singleton factory for {@link AccessAuthorizer}. Follows the same configuration-driven
 * reflective loading pattern as
 * {@link org.apache.drill.exec.rpc.user.security.UserAuthenticatorFactory}.
 *
 * <p>When {@code drill.exec.security.ranger.enabled} is {@code false} (the default),
 * a {@link NoOpAccessAuthorizer} is returned. When enabled, the implementation class
 * named by {@code drill.exec.security.ranger.impl} is loaded reflectively and
 * initialized with the configured service name.</p>
 */
public class AccessAuthorizerFactory {
  private static final Logger logger = LoggerFactory.getLogger(AccessAuthorizerFactory.class);

  // Default values used when the corresponding config keys are absent.
  private static final String DEFAULT_AUTHORIZER_IMPL =
      "org.apache.drill.exec.security.ranger.RangerAccessAuthorizer";
  private static final String DEFAULT_SERVICE_NAME = "drill";

  private static volatile AccessAuthorizer instance;

  private AccessAuthorizerFactory() {
  }

  /**
   * Returns the singleton {@link AccessAuthorizer} instance, initializing it from
   * the given configuration on first call.
   *
   * @param config the Drill configuration
   * @return the authorizer (never {@code null})
   */
  public static AccessAuthorizer getAuthorizer(DrillConfig config) {
    if (instance != null) {
      return instance;
    }
    synchronized (AccessAuthorizerFactory.class) {
      if (instance != null) {
        return instance;
      }
      instance = createAuthorizer(config);
      return instance;
    }
  }

  private static AccessAuthorizer createAuthorizer(DrillConfig config) {
    boolean enabled = config.hasPath(ExecConstants.RANGER_AUTH_ENABLED)
        && config.getBoolean(ExecConstants.RANGER_AUTH_ENABLED);
    if (!enabled) {
      logger.info("Ranger authorization is disabled (drill.exec.security.ranger.enabled=false)");
      return new NoOpAccessAuthorizer();
    }
    String impl = config.hasPath(ExecConstants.RANGER_AUTHORIZER_IMPL)
        ? config.getString(ExecConstants.RANGER_AUTHORIZER_IMPL)
        : DEFAULT_AUTHORIZER_IMPL;
    String serviceName = config.hasPath(ExecConstants.RANGER_SERVICE_NAME)
        ? config.getString(ExecConstants.RANGER_SERVICE_NAME)
        : DEFAULT_SERVICE_NAME;
    logger.info("Initializing Ranger authorizer: impl={}, service={}", impl, serviceName);
    try {
      Class<?> clazz = Class.forName(impl);
      AccessAuthorizer authorizer = (AccessAuthorizer) clazz.getDeclaredConstructor().newInstance();
      authorizer.init(serviceName);
      logger.info("Ranger authorizer initialized, enabled={}", authorizer.isEnabled());
      return authorizer;
    } catch (Exception e) {
      logger.error("Failed to initialize Ranger authorizer {}, falling back to NoOp (fail-open)", impl, e);
      throw new RuntimeException("Failed to initialize Ranger authorizer impl:" + impl + ", serviceName:" + serviceName);
    }
  }
}
