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
import io.ipfs.api.JSONParser;
import io.ipfs.api.MerkleNode;
import io.ipfs.multiaddr.MultiAddress;
import io.ipfs.multihash.Multihash;
import org.apache.drill.categories.IPFSStorageTest;
import org.apache.drill.categories.SlowTest;
import org.apache.drill.shaded.guava.com.google.common.cache.CacheBuilder;
import org.apache.drill.shaded.guava.com.google.common.cache.CacheLoader;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableMap;
import org.apache.drill.shaded.guava.com.google.common.io.Resources;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@Category({SlowTest.class, IPFSStorageTest.class})
public class TestIPFSGroupScan extends IPFSTestBase implements IPFSTestConstants {
  @Mock
  private IPFS ipfs;
  @Mock
  private IPFSCompat ipfsCompat;
  @Mock
  private IPFSHelper ipfsHelper;
  @Mock
  private IPFSStoragePlugin plugin;
  @Mock
  private IPFSPeer myself;

  @Before
  public void before() {

    ipfs = Mockito.mock(IPFS.class);
    ipfsCompat = Mockito.mock(IPFSCompat.class);
    ipfsHelper = Mockito.mock(IPFSHelper.class);
    plugin = Mockito.mock(IPFSStoragePlugin.class);

    try {
      IPFSStoragePluginConfig config = IPFSTestSuit.getIpfsStoragePluginConfig();

      Mockito.when(ipfs.id()).thenReturn(ImmutableMap.of(
          "ID", MOCK_NODE_ID_STRING,
          "Addresses", MOCK_NODE_ADDRS
      ));

      IPFSContext context = Mockito.mock(IPFSContext.class);
      myself = getMockedIPFSPeer(
          MOCK_NODE_ID_MULTIHASH,
          MOCK_NODE_MULTIADDRS,
          true,
          Optional.of(MOCK_NODE_ADDR)
      );

      Mockito.when(plugin.getConfig()).thenReturn(config);
      Mockito.when(plugin.getIPFSContext()).thenReturn(context);
      Mockito.when(plugin.getContext()).thenReturn(cluster.drillbit().getContext());
      Mockito.when(context.getMyself()).thenReturn(myself);
      Mockito.when(context.getIPFSHelper()).thenReturn(ipfsHelper);
      Mockito.when(context.getStoragePlugin()).thenReturn(plugin);
      Mockito.when(context.getStoragePluginConfig()).thenReturn(config);
      Mockito.when(context.getIPFSClient()).thenReturn(ipfs);
      Mockito.when(context.getIPFSPeerCache()).thenReturn(
          CacheBuilder.newBuilder()
              .maximumSize(1)
              .build(CacheLoader.from(key -> {
                    if (myself.getId().equals(key)) {
                      return myself;
                    } else {
                      return null;
                    }
                  })
              ));
      Mockito.when(context.getProviderCache()).thenReturn(
          CacheBuilder.newBuilder()
              .maximumSize(1)
              .build(CacheLoader.from(key -> ImmutableList.of(myself.getId())))
      );
      Mockito.when(ipfsHelper.getClient()).thenReturn(ipfs);
    } catch (IOException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testSimpleDatasetWithNoAnyOtherProviders() {
    try {
      Mockito.when(ipfsHelper.getObjectLinksTimeout(Mockito.any(Multihash.class))).thenReturn(new MerkleNode(SIMPLE_DATASET_HASH_STRING));
      Mockito.when(ipfsHelper.findprovsTimeout(Mockito.any(Multihash.class))).thenReturn(ImmutableList.of(MOCK_NODE_ID_MULTIHASH));
      //called in IPFSScanSpec.getTargetHash
      Mockito.when(ipfsHelper.resolve(Mockito.anyString(), Mockito.anyString(), Mockito.anyBoolean())).thenReturn(SIMPLE_DATASET_MULTIHASH);
      Mockito.when(ipfsHelper.isDrillReady(Mockito.any(Multihash.class))).thenReturn(true);
      Mockito.when(ipfsHelper.findpeerTimeout(Mockito.any(Multihash.class))).thenReturn(MOCK_NODE_MULTIADDRS);

      File simpleDataset = new File(Resources.getResource("simple.json").toURI());
      byte[] contents = Files.readAllBytes(simpleDataset.toPath());
      Mockito.when(ipfsHelper.getObjectDataTimeout(Mockito.any(Multihash.class))).thenReturn(contents);

      IPFSContext context = plugin.getIPFSContext();
      IPFSGroupScan groupScan = new IPFSGroupScan(context, new IPFSScanSpec(context, IPFSTestConstants.getQueryPath(SIMPLE_DATASET_MULTIHASH)), null);
      Map<Multihash, String> map = groupScan.getLeafAddrMappings(SIMPLE_DATASET_MULTIHASH);
      assertEquals(map.keySet().size(), 1);
      assertEquals(map.get(SIMPLE_DATASET_MULTIHASH), MOCK_NODE_ADDR);
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testChunkedDatasetWithNoAnyOtherProviders() {
    try {
      Mockito.when(ipfsHelper.getObjectLinksTimeout(CHUNKED_DATASET_MULTIHASH)).thenReturn(MerkleNode.fromJSON(JSONParser.parse("{\"Hash\":\"QmSeX1YAGWMXoPrgeKBTq2Be6NdRzTVESeeWyt7mQFuvzo\",\"Links\":[{\"Name\":\"1\",\"Hash\":\"QmSmDFd1GcLPyYtscdtkBCj7gbNKiJ8MkaBPEFMz9orPEi\",\"Size\":162},{\"Name\":\"2\",\"Hash\":\"QmQVBWTZ7MZjwHv5q9qG3zLzczsh8PGAVRWhF2gKsrj1hP\",\"Size\":159},{\"Name\":\"3\",\"Hash\":\"QmY8ghdB3mwdUAdBmft3bdgzPVcq8bCvtqTRd9wu3LjyTd\",\"Size\":89}]}\n")));
      Mockito.when(ipfsHelper.findprovsTimeout(Mockito.any(Multihash.class))).thenReturn(ImmutableList.of(MOCK_NODE_ID_MULTIHASH));
      Mockito.when(ipfsHelper.resolve(Mockito.anyString(), Mockito.anyString(), Mockito.anyBoolean())).thenReturn(CHUNKED_DATASET_MULTIHASH);
      Mockito.when(ipfsHelper.isDrillReady(Mockito.any(Multihash.class))).thenReturn(true);
      Mockito.when(ipfsHelper.findpeerTimeout(Mockito.any(Multihash.class))).thenReturn(MOCK_NODE_MULTIADDRS);
      for (Map.Entry<String, Multihash> entry : CHUNKS_MULTIHASH.entrySet()) {
        File chunkFile = new File(Resources.getResource(entry.getKey()).toURI());
        Mockito.when(ipfsHelper.getObjectDataTimeout(entry.getValue())).thenReturn(Files.readAllBytes(chunkFile.toPath()));
        Mockito.when(ipfsHelper.getObjectLinksTimeout(entry.getValue())).thenReturn(new MerkleNode(entry.getValue().toBase58()));
      }

      IPFSContext context = plugin.getIPFSContext();
      IPFSGroupScan groupScan = new IPFSGroupScan(context, new IPFSScanSpec(context, IPFSTestConstants.getQueryPath(SIMPLE_DATASET_MULTIHASH)), null);
      Map<Multihash, String> map = groupScan.getLeafAddrMappings(CHUNKED_DATASET_MULTIHASH);
      assertEquals(map.keySet().size(), 3);
      for (Map.Entry<String, Multihash> entry : CHUNKS_MULTIHASH.entrySet()) {
        assertEquals(map.get(entry.getValue()), MOCK_NODE_ADDR);
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  private IPFSPeer getMockedIPFSPeer(Multihash multihashId, List<MultiAddress> addrs, boolean isDrillReady,
                                     Optional<String> drillbitAddress) {
    IPFSPeer peer = Mockito.mock(IPFSPeer.class);
    Mockito.when(peer.getId()).thenReturn(multihashId);
    Mockito.when(peer.getMultiAddresses()).thenReturn(addrs);
    Mockito.when(peer.getDrillbitAddress()).thenReturn(drillbitAddress);
    Mockito.when(peer.hasDrillbitAddress()).thenReturn(drillbitAddress.isPresent());
    Mockito.when(peer.toString()).thenReturn(multihashId.toBase58());

    return peer;
  }
}
