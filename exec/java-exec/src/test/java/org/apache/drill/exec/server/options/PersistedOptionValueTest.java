/**
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

package org.apache.drill.exec.server.options;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.serialization.JacksonSerializer;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class PersistedOptionValueTest {
  @Test
  public void oldDeserializeTest() throws IOException {
    testHelper("/options/old_booleanopt.json",
      "/options/old_doubleopt.json",
      "/options/old_longopt.json",
      "/options/old_stringopt.json");
  }

  @Test
  public void newDeserializeTest() throws IOException {
    testHelper("/options/new_booleanopt.json",
      "/options/new_doubleopt.json",
      "/options/new_longopt.json",
      "/options/new_stringopt.json");
  }

  private void testHelper(String booleanOptionFile, String doubleOptionFile,
                          String longOptionFile, String stringOptionFile) throws IOException {
    JacksonSerializer serializer = new JacksonSerializer<>(new ObjectMapper(), PersistedOptionValue.class);
    String booleanOptionJson = FileUtils.getResourceAsString(booleanOptionFile);
    String doubleOptionJson = FileUtils.getResourceAsString(doubleOptionFile);
    String longOptionJson = FileUtils.getResourceAsString(longOptionFile);
    String stringOptionJson = FileUtils.getResourceAsString(stringOptionFile);

    PersistedOptionValue booleanValue = (PersistedOptionValue) serializer.deserialize(booleanOptionJson.getBytes());
    PersistedOptionValue doubleValue = (PersistedOptionValue) serializer.deserialize(doubleOptionJson.getBytes());
    PersistedOptionValue longValue = (PersistedOptionValue) serializer.deserialize(longOptionJson.getBytes());
    PersistedOptionValue stringValue = (PersistedOptionValue) serializer.deserialize(stringOptionJson.getBytes());

    PersistedOptionValue expectedBooleanValue = new PersistedOptionValue("true");
    PersistedOptionValue expectedDoubleValue = new PersistedOptionValue("1.5");
    PersistedOptionValue expectedLongValue = new PersistedOptionValue("5000");
    PersistedOptionValue expectedStringValue = new PersistedOptionValue("wabalubadubdub");

    Assert.assertEquals(expectedBooleanValue, booleanValue);
    Assert.assertEquals(expectedDoubleValue, doubleValue);
    Assert.assertEquals(expectedLongValue, longValue);
    Assert.assertEquals(expectedStringValue, stringValue);
  }
}
