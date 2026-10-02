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
package org.apache.drill.exec.security.spi;

import java.util.Map;

/**
 * Factory SPI for creating {@link AccessAuthorizer} instances.
 *
 * <p>Mirrors Presto's {@code SystemAccessControlFactory}. Implementations are
 * discovered via {@code ServiceLoader} (registered under
 * {@code META-INF/services/org.apache.drill.exec.security.spi.AccessAuthorizerFactory})
 * and selected by the {@code drill.exec.security.authorizer.name} configuration
 * key matching {@link #getName()}.</p>
 *
 * <p>The {@code config} map carries the flattened properties of the
 * {@code drill.exec.security.authorizer} configuration subtree (with
 * {@code enabled} and {@code name} already removed). It contains only JDK
 * types so implementations never depend on Drill engine configuration
 * classes — this keeps an authorization plugin (e.g. the Ranger plugin)
 * portable to external repositories.</p>
 */
public interface AccessAuthorizerFactory {

  /**
   * @return the factory name matched against
   *         {@code drill.exec.security.authorizer.name} (e.g. "ranger")
   */
  String getName();

  /**
   * Creates a fully-initialized {@link AccessAuthorizer}. All initialization
   * (classloader setup, policy engine bootstrap, ...) must complete here or
   * by throwing — there is no separate init lifecycle on the authorizer.
   *
   * @param config flattened authorizer configuration properties; never {@code null}
   * @return a ready-to-use authorizer instance
   */
  AccessAuthorizer createAuthorizer(Map<String, String> config);
}
