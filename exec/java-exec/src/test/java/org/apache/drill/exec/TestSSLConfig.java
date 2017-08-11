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


import org.apache.drill.common.exceptions.DrillException;
import org.apache.drill.test.ConfigBuilder;
import org.junit.Test;
import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestSSLConfig {

  @Test
  public void testMissingKeystorePath() throws Exception {

    ConfigBuilder config = new ConfigBuilder();
    config.put(ExecConstants.HTTP_KEYSTORE_PATH, "");
    config.put(ExecConstants.HTTP_KEYSTORE_PASSWORD, "root");
    try {
      SSLConfig sslv = new SSLConfig(config.build());
      fail();
      //Expected
    } catch (Exception e) {
      assertTrue(e instanceof DrillException);
    }
  }

  @Test
  public void testMissingKeystorePassword() throws Exception {

    ConfigBuilder config = new ConfigBuilder();
    config.put(ExecConstants.HTTP_KEYSTORE_PATH, "/root");
    config.put(ExecConstants.HTTP_KEYSTORE_PASSWORD, "");
    try {
      SSLConfig sslv = new SSLConfig(config.build());
      fail();
      //Expected
    } catch (Exception e) {
      assertTrue(e instanceof DrillException);
    }
  }

  @Test
  public void testForKeystoreConfig() throws Exception {

    ConfigBuilder config = new ConfigBuilder();
    config.put(ExecConstants.HTTP_KEYSTORE_PATH, "/root");
    config.put(ExecConstants.HTTP_KEYSTORE_PASSWORD, "root");
    try {
      SSLConfig sslv = new SSLConfig(config.build());
      assertEquals("/root", sslv.getKeyStorePath());
      assertEquals("root", sslv.getKeyStorePassword());
    } catch (Exception e) {
      fail();

    }
  }

  @Test
  public void testForBackwardCompatability() throws Exception {

    ConfigBuilder config = new ConfigBuilder();
    config.put("javax.net.ssl.keyStore", "/root");
    config.put("javax.net.ssl.keyStorePassword", "root");
    SSLConfig sslv = new SSLConfig(config.build());
    assertEquals("/root",sslv.getKeyStorePath());
    assertEquals("root", sslv.getKeyStorePassword());
  }

  @Test
  public void testForTrustStore() throws Exception {

    ConfigBuilder config = new ConfigBuilder();
    config.put(ExecConstants.HTTP_TRUSTSTORE_PATH, "/root");
    config.put(ExecConstants.HTTP_TRUSTSTORE_PASSWORD, "root");
    SSLConfig sslv = new SSLConfig(config.build());
    assertEquals(true, sslv.hasTrustStorePath());
    assertEquals(true,sslv.hasTrustStorePassword());
    assertEquals("/root",sslv.getTrustStorePath());
    assertEquals("root",sslv.getTrustStorePassword());
  }
}