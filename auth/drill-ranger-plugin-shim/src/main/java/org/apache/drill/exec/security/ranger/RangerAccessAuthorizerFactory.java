/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file distributed with
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

import org.apache.drill.exec.security.spi.AccessAuthorizer;
import org.apache.drill.exec.security.spi.AccessAuthorizerFactory;

import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * {@link AccessAuthorizerFactory} for the Ranger-backed authorizer.
 * <p>Registered via
 * {@code META-INF/services/org.apache.drill.exec.security.spi.AccessAuthorizerFactory}
 * and selected by {@code drill.exec.security.authorizer.name=ranger}. The
 * config map carries the flattened {@code drill.exec.security.authorizer}
 * subtree; Ranger-specific keys are parsed here (never in the engine):
 *
 * <ul>
 *   <li>{@code service.name} — Ranger service instance name
 *       (default {@code "drill"})</li>
 * </ul>
 *
 * <p>{@link #createAuthorizer(Map)} returns a fully-initialized shim;
 * initialization failures propagate as exceptions (fail-closed).</p>
 */
public class RangerAccessAuthorizerFactory implements AccessAuthorizerFactory {

  public static final String NAME = "ranger";

  static final String CONFIG_SERVICE_NAME = "service.name";
  static final String DEFAULT_SERVICE_NAME = "drill";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public AccessAuthorizer createAuthorizer(Map<String, String> config) {
    requireNonNull(config, "config is null");
    String serviceName = config.getOrDefault(CONFIG_SERVICE_NAME, DEFAULT_SERVICE_NAME);
    return new RangerAccessAuthorizer(serviceName);
  }
}
