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
package org.apache.drill.exec.store.jdbc;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.test.BaseTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

public class TestBasicDataSource extends BaseTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testInitWithoutUserAndPassword() throws Exception {
    JdbcStorageConfig config = new JdbcStorageConfig(
      "driver", "url", null, null, false, null);
    try (BasicDataSource dataSource = JdbcStoragePlugin.initDataSource(config)) {
      assertEquals("driver", dataSource.getDriverClassName());
      assertEquals("url", dataSource.getUrl());
      assertNull(dataSource.getUsername());
      assertNull(dataSource.getPassword());
    }
  }

  @Test
  public void testInitWithUserAndPassword() throws Exception {
    JdbcStorageConfig config = new JdbcStorageConfig(
      "driver", "url", "user", "password", false, null);
    try (BasicDataSource dataSource = JdbcStoragePlugin.initDataSource(config)) {
      assertEquals("user", dataSource.getUsername());
      assertEquals("password", dataSource.getPassword());
    }
  }

  @Test
  public void testInitWithSourceParameters() throws Exception {
    Map<String, Object> sourceParameters = new HashMap<>();
    sourceParameters.put("maxIdle", 5);
    sourceParameters.put("cacheState", false);
    sourceParameters.put("validationQuery", "select * from information_schema.collations");
    JdbcStorageConfig config = new JdbcStorageConfig(
      "driver", "url", "user", "password", false, sourceParameters);
    try (BasicDataSource dataSource = JdbcStoragePlugin.initDataSource(config)) {
      assertEquals(5, dataSource.getMaxIdle());
      assertFalse(dataSource.getCacheState());
      assertEquals("select * from information_schema.collations", dataSource.getValidationQuery());
    }
  }

  @Test
  public void testInitWithIncorrectSourceParameterName() throws Exception {
    Map<String, Object> sourceParameters = new HashMap<>();
    sourceParameters.put("maxIdle", 5);
    sourceParameters.put("abc", "abc");
    sourceParameters.put("cacheState", false);
    sourceParameters.put("validationQuery", null);
    JdbcStorageConfig config = new JdbcStorageConfig(
      "driver", "url", "user", "password", false, sourceParameters);
    try (BasicDataSource dataSource = JdbcStoragePlugin.initDataSource(config)) {
      // "abc" parameter will be ignored
      assertEquals(5, dataSource.getMaxIdle());
      assertFalse(dataSource.getCacheState());
      assertNull(dataSource.getValidationQuery());
    }
  }

  @Test
  public void testInitWithIncorrectSourceParameterValue() {
    Map<String, Object> sourceParameters = new HashMap<>();
    sourceParameters.put("maxIdle", "abc");
    JdbcStorageConfig config = new JdbcStorageConfig(
      "driver", "url", "user", "password", false, sourceParameters);

    thrown.expect(UserException.class);
    thrown.expectMessage(UserBitShared.DrillPBError.ErrorType.CONNECTION.name());

    JdbcStoragePlugin.initDataSource(config);
  }
}
