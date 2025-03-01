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
package org.apache.drill.exec.store.druid;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.drill.common.util.JacksonUtils;
import com.google.common.io.Resources;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class DruidStoragePluginConfigTest {

  @Test
  public void testDruidStoragePluginConfigSuccessfullyParsed()
      throws URISyntaxException, IOException {
    ObjectMapper mapper = JacksonUtils.createObjectMapper();
    JsonNode storagePluginJson = mapper.readTree(new File(
        Resources.getResource("bootstrap-storage-plugins.json").toURI()));
    DruidStoragePluginConfig druidStoragePluginConfig =
        mapper.treeToValue(storagePluginJson.get("storage").get("druid"), DruidStoragePluginConfig.class);
    assertNotNull(druidStoragePluginConfig);
    assertEquals("http://localhost:8082", druidStoragePluginConfig.getBrokerAddress());
    assertEquals("http://localhost:8081", druidStoragePluginConfig.getCoordinatorAddress());
    assertEquals(200, druidStoragePluginConfig.getAverageRowSizeBytes());
    assertFalse(druidStoragePluginConfig.isEnabled());
  }

  @Test
  public void testDefaultRowSizeUsedWhenNotProvidedInConfig()
      throws JsonProcessingException {
    String druidConfigStr = "{\n" + "  \"storage\":{\n" + "    \"druid\" : {\n"
        + "      \"type\" : \"druid\",\n"
        + "      \"brokerAddress\" : \"http://localhost:8082\",\n"
        + "      \"coordinatorAddress\": \"http://localhost:8081\",\n"
        + "      \"enabled\" : false\n" + "    }\n" + "  }\n" + "}\n";
    ObjectMapper mapper = JacksonUtils.createObjectMapper();
    JsonNode storagePluginJson = mapper.readTree(druidConfigStr);
    DruidStoragePluginConfig druidStoragePluginConfig =
        mapper.treeToValue(storagePluginJson.get("storage").get("druid"), DruidStoragePluginConfig.class);
    assertEquals(100, druidStoragePluginConfig.getAverageRowSizeBytes());
  }
}
