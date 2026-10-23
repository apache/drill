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

import java.util.Properties;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.exec.security.spi.AccessType;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.test.BaseTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link DdlAccessChecker}. Uses the {@link TestAccessAuthorizer}
 * (factory "test", registered via the test META-INF/services file) loaded
 * through {@link AccessAuthorizerManager} with a real {@link DrillConfig};
 * {@link QueryContext} and {@link AbstractSchema} are Mockito mocks.
 */
public class DdlAccessCheckerTest extends BaseTest {

  private QueryContext context;
  private AbstractSchema schema;

  @Before
  @After
  public void reset() {
    AccessAuthorizerManager.reset();
    TestAccessAuthorizer.reset();
  }

  private void initContext(DrillConfig config, String userName) {
    UserSession session = mock(UserSession.class);
    when(session.getCredentials())
        .thenReturn(UserBitShared.UserCredentials.newBuilder().setUserName(userName).build());
    context = mock(QueryContext.class);
    when(context.getSession()).thenReturn(session);
    when(context.getConfig()).thenReturn(config);
    schema = mock(AbstractSchema.class);
  }

  private DrillConfig enabledConfig() {
    Properties props = new Properties();
    props.setProperty(ExecConstants.AUTHORIZER_ENABLED, "true");
    props.setProperty(ExecConstants.AUTHORIZER_NAME, "test");
    return DrillConfig.create(props);
  }

  @Test
  public void allowsAndMapsResourceWhenNothingDenied() {
    initContext(enabledConfig(), "alice");
    when(schema.getSchemaPath()).thenReturn(ImmutableList.of("dfs", "tmp"));

    DdlAccessChecker.checkDdlAccess(context, schema, "t1", AccessType.CREATE);

    TestAccessAuthorizer.TableCheck check = TestAccessAuthorizer.getLastTableCheck();
    assertNotNull(check);
    assertEquals("alice", check.user);
    assertEquals("dfs", check.dataSource);
    assertEquals("tmp", check.schema);
    assertEquals("t1", check.table);
    assertEquals(AccessType.CREATE, check.accessType);
  }

  @Test
  public void throwsPermissionErrorWhenCreateDenied() {
    initContext(enabledConfig(), "bob");
    when(schema.getSchemaPath()).thenReturn(ImmutableList.of("dfs", "tmp"));
    TestAccessAuthorizer.setDeniedAccessTypes(ImmutableSet.of(AccessType.CREATE));

    UserException e = assertThrows(UserException.class,
        () -> DdlAccessChecker.checkDdlAccess(context, schema, "t1", AccessType.CREATE));

    assertEquals(UserBitShared.DrillPBError.ErrorType.PERMISSION, e.getErrorType());
    assertTrue(e.getOriginalMessage().contains("lacks CREATE privilege"));
    assertTrue(e.getOriginalMessage().contains("dfs.tmp.t1"));
  }

  @Test
  public void denyIsExactPerAccessType() {
    initContext(enabledConfig(), "bob");
    when(schema.getSchemaPath()).thenReturn(ImmutableList.of("dfs", "tmp"));
    TestAccessAuthorizer.setDeniedAccessTypes(ImmutableSet.of(AccessType.DROP));

    // DROP is denied, CREATE is not: the CREATE check must pass.
    DdlAccessChecker.checkDdlAccess(context, schema, "t1", AccessType.CREATE);

    UserException e = assertThrows(UserException.class,
        () -> DdlAccessChecker.checkDdlAccess(context, schema, "t1", AccessType.DROP));
    assertEquals(UserBitShared.DrillPBError.ErrorType.PERMISSION, e.getErrorType());
    assertTrue(e.getOriginalMessage().contains("lacks DROP privilege"));
  }

  @Test
  public void mapsSchemaLessQualifiedNameToDefaultSchema() {
    initContext(enabledConfig(), "alice");
    when(schema.getSchemaPath()).thenReturn(ImmutableList.of("dfs"));

    DdlAccessChecker.checkDdlAccess(context, schema, "t", AccessType.CREATE);

    TestAccessAuthorizer.TableCheck check = TestAccessAuthorizer.getLastTableCheck();
    assertNotNull(check);
    assertEquals("dfs", check.dataSource);
    assertEquals("default", check.schema);
    assertEquals("t", check.table);
  }

  @Test
  public void allowsAllWhenAuthorizationDisabled() {
    initContext(DrillConfig.forClient(), "alice");
    when(schema.getSchemaPath()).thenReturn(ImmutableList.of("dfs", "tmp"));
    // deny rules are irrelevant: the manager returns the allow-all authorizer
    TestAccessAuthorizer.setDeniedAccessTypes(ImmutableSet.of(AccessType.CREATE));

    DdlAccessChecker.checkDdlAccess(context, schema, "t1", AccessType.CREATE);

    // The test authorizer was never consulted.
    assertNull(TestAccessAuthorizer.getLastTableCheck());
  }
}
