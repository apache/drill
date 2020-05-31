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


package org.apache.drill.exec.store.ipfs;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.drill.categories.IPFSStorageTest;
import org.apache.drill.categories.SlowTest;
import org.apache.drill.shaded.guava.com.google.common.io.Resources;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

@RunWith(Suite.class)
@Suite.SuiteClasses({TestIPFSQueries.class, TestIPFSGroupScan.class, TestIPFSScanSpec.class})
@Category({SlowTest.class, IPFSStorageTest.class})
public class IPFSTestSuit {
  private static final Logger logger = LoggerFactory.getLogger(IPFSTestSuit.class);

  private static final ObjectMapper mapper = new ObjectMapper();

  private static IPFSStoragePluginConfig ipfsStoragePluginConfig = null;

  @BeforeClass
  public static void initIPFS() {
    try {
      JsonNode storagePluginJson = mapper.readTree(new File(Resources.getResource("bootstrap-storage-plugins.json").toURI()));
      ipfsStoragePluginConfig = mapper.treeToValue(storagePluginJson.get("storage").get("ipfs"), IPFSStoragePluginConfig.class);
      ipfsStoragePluginConfig.setEnabled(true);
    } catch (Exception e) {
      logger.error("Error initializing IPFS ", e);
    }
  }

  public static IPFSStoragePluginConfig getIpfsStoragePluginConfig() {
    return ipfsStoragePluginConfig;
  }
}
