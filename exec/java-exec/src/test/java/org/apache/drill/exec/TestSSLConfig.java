/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.drill.exec;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.DrillException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.SSLConfig;
import org.apache.drill.test.OperatorFixture;
import org.junit.Test;
import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestSSLConfig {


  @Test
  public void firstTest() throws Exception {

    OperatorFixture.OperatorFixtureBuilder builder = OperatorFixture.builder();
    builder.configBuilder().put(ExecConstants.HTTP_KEYSTORE_PASSWORD, "root");
    builder.configBuilder().put(ExecConstants.HTTP_KEYSTORE_PATH, "");
    try (OperatorFixture fixture = builder.build()) {
      DrillConfig config = fixture.config();
      try {
        SSLConfig sslv = new SSLConfig(config);
        fail();
      } catch (Exception e) {
        assertTrue(e instanceof DrillException);
      }
    }
  }

  @Test
  public void secondTest() throws Exception {

    OperatorFixture.OperatorFixtureBuilder builder = OperatorFixture.builder();
    builder.configBuilder().put(ExecConstants.HTTP_KEYSTORE_PASSWORD, "");
    builder.configBuilder().put(ExecConstants.HTTP_KEYSTORE_PATH, "/root");
    try (OperatorFixture fixture = builder.build()) {
      DrillConfig config = fixture.config();
      try {
        SSLConfig sslv = new SSLConfig(config);
        fail();
      } catch (Exception e) {
        assertTrue(e instanceof DrillException);
      }
    }
  }

  @Test
  public void thirdTest() throws Exception {

    OperatorFixture.OperatorFixtureBuilder builder = OperatorFixture.builder();
    builder.configBuilder().put(ExecConstants.HTTP_KEYSTORE_PASSWORD, "root");
    builder.configBuilder().put(ExecConstants.HTTP_KEYSTORE_PATH, "/root");
    try (OperatorFixture fixture = builder.build()) {
      DrillConfig config = fixture.config();
      SSLConfig sslv = new SSLConfig(config);
      assertEquals("root", sslv.getkeystorePassword());
      assertEquals("/root", sslv.getkeystorePath());
    }
  }
}








