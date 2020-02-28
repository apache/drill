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
package org.apache.drill.exec.store.mongo;

import com.mongodb.MongoCredential;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestMongoStoragePluginUsesCredentialsStore {

  private void test(String expectedUserName, String expectedPassword, String connection, String name) throws IOException, ExecutionSetupException {
    MongoStoragePlugin plugin = new MongoStoragePlugin(new MongoStoragePluginConfig(
      connection), null, name);
    List<MongoCredential> creds = plugin.getClient().getCredentialsList();
    if (expectedUserName == null) {
      assertEquals(0, creds.size());
    } else {
      assertEquals(1, creds.size());
      MongoCredential cred = creds.get(0);
      assertEquals(expectedUserName, cred.getUserName());
      assertEquals(expectedPassword, new String(cred.getPassword()));
    }
  }

  @Test
  public void usesPasswordFromCoreSiteXml() throws Exception {
    test(
      "test",
      "pw",
      "mongodb://example:27017/somedb?readPreference=primary",
      "mongo");
  }

  @Test
  public void supportsInterpolation() throws Exception {
    test(
      "mooch_test",
      "mooch_pw",
      "mongodb://example:27017/somedb?readPreference=primary",
      "mongomooch");
  }

  @Test
  public void doesNotReplaceExistingCredentials() throws Exception {
    test(
      "u",
      "p",
      "mongodb://u:p@example:27017/somedb?readPreference=primary",
      "mongo");
  }

  @Test
  public void urlEncodesCredentials() throws Exception {
    test(
      "!@#$%//:+^*()",
      ":;[]}{!@#/?$#@",
      "mongodb://example:27017/somedb?readPreference=primary",
      "mongosec");
  }

  @Test
  public void doesNotAddCredentialsIfNoneFound() throws Exception {
    test(
      null,
      null,
      "mongodb://example:27017/somedb?readPreference=primary",
      "mongopublic");
  }
}
