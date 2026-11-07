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

import org.apache.drill.exec.security.spi.AccessType;
import org.apache.drill.exec.security.spi.UserIdentity;
import org.apache.drill.test.BaseTest;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link AllowAllAccessAuthorizer}.
 * Verifies the fail-open behavior used when the access authorizer is
 * disabled or unavailable.
 */
public class AllowAllAccessAuthorizerTest extends BaseTest {

  private final AllowAllAccessAuthorizer authorizer = new AllowAllAccessAuthorizer();

  @Test
  public void checkTableAccess_returnsTrue() {
    assertTrue(authorizer.checkTableAccess(
        UserIdentity.of("alice"), "mysql", "shf", "orders", AccessType.SELECT));
    assertTrue(authorizer.checkTableAccess(
        UserIdentity.of("bob"), "dfs", "tmp", "foo", AccessType.CREATE));
  }

  @Test
  public void checkColumnAccess_returnsTrue() {
    Set<String> columns = new HashSet<>(Arrays.asList("id", "amount"));
    assertTrue(authorizer.checkColumnAccess(
        UserIdentity.of("alice"), "mysql", "shf", "orders", columns, AccessType.SELECT));
    // Empty column set must still pass (fail-open)
    assertTrue(authorizer.checkColumnAccess(
        UserIdentity.of("alice"), "mysql", "shf", "orders", Collections.emptySet(), AccessType.SELECT));
  }

  @Test
  public void checkTableAccess_returnsTrue_withNullArgs() {
    // Fail-open contract: even null inputs must not throw; method returns true
    assertTrue(authorizer.checkTableAccess(null, null, null, null, null));
  }
}
