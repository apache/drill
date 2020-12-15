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

import io.ipfs.api.IPFS;
import io.ipfs.api.MerkleNode;
import io.ipfs.multihash.Multihash;
import org.apache.drill.shaded.guava.com.google.common.io.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Optional;

public class IPFSTestDataGenerator {
  private static final Logger logger = LoggerFactory.getLogger(IPFSTestDataGenerator.class);

  public static Multihash importSimple(IPFSStoragePluginConfig config) {
    try {
      final IPFS client = new IPFS(config.getHost(), config.getPort());
      File testFile = new File(Resources.getResource("simple.json").toURI());
      return addObject(client, Files.readAllBytes(testFile.toPath()));
    } catch (Exception e) {
      logger.error("Failed to import data: %s", e);
      return null;
    }
  }

  public static Multihash importChunked(IPFSStoragePluginConfig config) {
    try {
      final IPFS client = new IPFS(config.getHost(), config.getPort());

      Multihash base = IPFSHelper.IPFS_NULL_OBJECT;
      for (int i = 1; i <= 3; i++) {
        File testFile = new File(Resources.getResource(String.format("chunked-json-%d.json", i)).toURI());
        Multihash chunk = addObject(client, Files.readAllBytes(testFile.toPath()));
        base = addLink(client, base, String.format("%d", i), chunk);
      }

      return base;
    } catch (Exception e) {
      logger.error("Failed to import data: %s", e);
      return null;
    }
  }

  private static Multihash addObject(IPFS client, byte[] data) throws IOException {
    MerkleNode node = client.object.patch(IPFSHelper.IPFS_NULL_OBJECT, "set-data", Optional.of(data), Optional.empty(), Optional.empty());
    return node.hash;
  }

  private static Multihash addLink(IPFS client, Multihash base, String name, Multihash referent) throws IOException {
    MerkleNode node = client.object.patch(base, "add-link", Optional.empty(), Optional.of(name), Optional.of(referent));
    return node.hash;
  }
}
