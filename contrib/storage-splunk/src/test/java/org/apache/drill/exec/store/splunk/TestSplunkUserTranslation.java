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

package org.apache.drill.exec.store.splunk;

import org.apache.drill.categories.SlowTest;
import org.apache.drill.common.config.DrillProperties;
import org.apache.drill.common.exceptions.UserRemoteException;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.test.ClientFixture;
import org.junit.Test;
import org.junit.experimental.categories.Category;


import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.ADMIN_USER;
import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.ADMIN_USER_PASSWORD;
import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.TEST_USER_1;
import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.TEST_USER_1_PASSWORD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@Category({SlowTest.class})
public class TestSplunkUserTranslation extends SplunkBaseTest {

  @Test
  public void testInfoSchemaQueryWithMissingCredentials() throws Exception {
    // This test validates that the correct credentials are sent down to Splunk.
    // This user should not see the ut_splunk because they do not have valid credentials
    ClientFixture client = cluster
      .clientBuilder()
      .property(DrillProperties.USER, ADMIN_USER)
      .property(DrillProperties.PASSWORD, ADMIN_USER_PASSWORD)
      .build();

    String sql = "SHOW DATABASES WHERE schema_name LIKE '%splunk%'";

    RowSet results = client.queryBuilder().sql(sql).rowSet();
    assertEquals(1, results.rowCount());
  }

  @Test
  public void testInfoSchemaQueryWithValidCredentials() throws Exception {
    ClientFixture client = cluster
      .clientBuilder()
      .property(DrillProperties.USER, TEST_USER_1)
      .property(DrillProperties.PASSWORD, TEST_USER_1_PASSWORD)
      .build();

    String sql = "SHOW DATABASES WHERE schema_name LIKE '%splunk'";

    RowSet results = client.queryBuilder().sql(sql).rowSet();
    assertEquals(2, results.rowCount());
  }

  @Test
  public void testSplunkQueryWithUserTranslation() throws Exception {
    ClientFixture client = cluster
      .clientBuilder()
      .property(DrillProperties.USER, TEST_USER_1)
      .property(DrillProperties.PASSWORD, TEST_USER_1_PASSWORD)
      .build();

    String sql = "SELECT acceleration_id, action, add_offset, add_timestamp FROM ut_splunk._audit LIMIT 2";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    assertEquals(2, results.rowCount());
  }

  @Test
  public void testSplunkQueryWithUserTranslationAndInvalidCredentials() throws Exception {
    ClientFixture client = cluster
      .clientBuilder()
      .property(DrillProperties.USER, ADMIN_USER)
      .property(DrillProperties.PASSWORD, ADMIN_USER_PASSWORD)
      .build();

    String sql = "SELECT acceleration_id, action, add_offset, add_timestamp FROM ut_splunk._audit LIMIT 2";
    try {
      client.queryBuilder().sql(sql).rowSet();
      fail();
    } catch (UserRemoteException e) {
      assertTrue(e.getMessage().contains("Schema [[ut_splunk]] is not valid"));
    }
  }
}
