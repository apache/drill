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
package org.apache.drill.exec.security;

import org.apache.drill.exec.security.spi.AccessAuthorizer;
import org.apache.drill.exec.security.spi.AccessAuthorizerFactory;

import java.util.Map;

/**
 * Test-only {@link AccessAuthorizerFactory} registered via the test
 * {@code META-INF/services} file. Selected by configuring
 * {@code drill.exec.security.authorizer.name=test}, which lets
 * {@link AccessAuthorizerManagerTest} exercise the manager's ServiceLoader
 * discovery and config-flattening logic WITHOUT touching the production
 * Ranger shim (whose {@code RangerPluginClassLoader} needs the assembled
 * distribution directories).
 */
public class TestAccessAuthorizerFactory implements AccessAuthorizerFactory {

  public static final String NAME = "test";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public AccessAuthorizer createAuthorizer(Map<String, String> config) {
    return new TestAccessAuthorizer(config);
  }
}
