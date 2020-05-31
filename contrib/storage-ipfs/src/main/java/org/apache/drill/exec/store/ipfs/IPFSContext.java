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
import io.ipfs.multihash.Multihash;
import org.apache.drill.exec.store.ipfs.IPFSStoragePluginConfig.IPFSCacheType;
import org.apache.drill.shaded.guava.com.google.common.cache.CacheBuilder;
import org.apache.drill.shaded.guava.com.google.common.cache.CacheLoader;
import org.apache.drill.shaded.guava.com.google.common.cache.LoadingCache;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class IPFSContext {
  private final IPFS ipfsClient;
  private final IPFSHelper ipfsHelper;
  private final IPFSPeer myself;
  private final IPFSStoragePluginConfig storagePluginConfig;
  private final IPFSStoragePlugin storagePlugin;
  private final LoadingCache<Multihash, IPFSPeer> ipfsPeerCache;
  private final LoadingCache<Multihash, List<Multihash>> providerCache;

  public IPFSContext(IPFSStoragePluginConfig config, IPFSStoragePlugin plugin) throws IOException {
    this.ipfsClient = new IPFS(config.getHost(), config.getPort());
    this.ipfsHelper = new IPFSHelper(ipfsClient, Executors.newCachedThreadPool());
    ipfsHelper.setMaxPeersPerLeaf(config.getMaxNodesPerLeaf());
    ipfsHelper.setTimeouts(config.getIPFSTimeouts());
    this.storagePlugin = plugin;
    this.storagePluginConfig = config;

    this.myself = ipfsHelper.getMyself();
    this.ipfsPeerCache = CacheBuilder.newBuilder()
        .maximumSize(config.getIPFSCache(IPFSCacheType.PEER).size)
        .refreshAfterWrite(config.getIPFSCache(IPFSCacheType.PEER).ttl, TimeUnit.SECONDS)
        .build(new CacheLoader<Multihash, IPFSPeer>() {
          @Override
          public IPFSPeer load(Multihash key) {
            return new IPFSPeer(getIPFSHelper(), key);
          }
        });
    this.providerCache = CacheBuilder.newBuilder()
        .maximumSize(config.getIPFSCache(IPFSCacheType.PROVIDER).size)
        .refreshAfterWrite(config.getIPFSCache(IPFSCacheType.PROVIDER).ttl, TimeUnit.SECONDS)
        .build(new CacheLoader<Multihash, List<Multihash>>() {
          @Override
          public List<Multihash> load(Multihash key) {
            return ipfsHelper.findprovsTimeout(key);
          }
        });
  }

  public IPFS getIPFSClient() {
    return ipfsClient;
  }

  public IPFSHelper getIPFSHelper() {
    return ipfsHelper;
  }

  public IPFSPeer getMyself() {
    return myself;
  }

  public IPFSStoragePlugin getStoragePlugin() {
    return storagePlugin;
  }

  public IPFSStoragePluginConfig getStoragePluginConfig() {
    return storagePluginConfig;
  }

  public LoadingCache<Multihash, IPFSPeer> getIPFSPeerCache() {
    return ipfsPeerCache;
  }

  public LoadingCache<Multihash, List<Multihash>> getProviderCache() {
    return providerCache;
  }
}
