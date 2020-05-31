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
import io.ipfs.cid.Cid;
import io.ipfs.multiaddr.MultiAddress;
import io.ipfs.multihash.Multihash;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.store.ipfs.IPFSStoragePluginConfig.IPFSTimeOut;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.bouncycastle.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.apache.drill.exec.store.ipfs.IPFSStoragePluginConfig.IPFSTimeOut.FETCH_DATA;
import static org.apache.drill.exec.store.ipfs.IPFSStoragePluginConfig.IPFSTimeOut.FIND_PEER_INFO;

/**
 * Helper class with some utilities that are specific to Drill with an IPFS storage
 *
 * DRILL-7778: refactor to support CIDv1
 */
public class IPFSHelper {
  private static final Logger logger = LoggerFactory.getLogger(IPFSHelper.class);

  public static final String IPFS_NULL_OBJECT_HASH = "QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n";
  public static final Multihash IPFS_NULL_OBJECT = Cid.decode(IPFS_NULL_OBJECT_HASH);

  private ExecutorService executorService;
  private final IPFS client;
  private final IPFSCompat clientCompat;
  private IPFSPeer myself;
  private int maxPeersPerLeaf;
  private Map<IPFSTimeOut, Integer> timeouts;

  public IPFSHelper(IPFS ipfs) {
    this.client = ipfs;
    this.clientCompat = new IPFSCompat(ipfs);
  }

  public IPFSHelper(IPFS ipfs, ExecutorService executorService) {
    this(ipfs);
    this.executorService = executorService;
  }

  public void setTimeouts(Map<IPFSTimeOut, Integer> timeouts) {
    this.timeouts = timeouts;
  }

  public void setMyself(IPFSPeer myself) {
    this.myself = myself;
  }

  /**
   * Set maximum number of providers per leaf node. The more providers, the more time it takes to do DHT queries, while
   * it is more likely we can find an optimal peer.
   *
   * @param maxPeersPerLeaf max number of providers to search per leaf node
   */
  public void setMaxPeersPerLeaf(int maxPeersPerLeaf) {
    this.maxPeersPerLeaf = maxPeersPerLeaf;
  }

  public IPFS getClient() {
    return client;
  }

  public IPFSCompat getClientCompat() {
    return clientCompat;
  }

  public List<Multihash> findprovsTimeout(Multihash id) {
    List<String> providers;
    providers = clientCompat.dht.findprovsListTimeout(id, maxPeersPerLeaf, timeouts.get(IPFSTimeOut.FIND_PROV), executorService);

    return providers.stream().map(Cid::decode).collect(Collectors.toList());
  }

  public List<MultiAddress> findpeerTimeout(Multihash peerId) {
    // trying to resolve addresses of a node itself will always hang
    // so we treat it specially
    if (peerId.equals(myself.getId())) {
      return myself.getMultiAddresses();
    }

    List<String> addrs;
    addrs = clientCompat.dht.findpeerListTimeout(peerId, timeouts.get(IPFSTimeOut.FIND_PEER_INFO), executorService);
    return addrs.stream()
        .filter(addr -> !addr.equals(""))
        .map(MultiAddress::new).collect(Collectors.toList());
  }

  public byte[] getObjectDataTimeout(Multihash object) throws IOException {
    return timedFailure(client.object::data, object, timeouts.get(IPFSTimeOut.FETCH_DATA));
  }

  public MerkleNode getObjectLinksTimeout(Multihash object) throws IOException {
    return timedFailure(client.object::links, object, timeouts.get(IPFSTimeOut.FETCH_DATA));
  }

  public IPFSPeer getMyself() throws IOException {
    if (this.myself != null) {
      return this.myself;
    }

    Map res = timedFailure(client::id, timeouts.get(FIND_PEER_INFO));
    Multihash myID = Cid.decode((String) res.get("ID"));
    // Rule out any non-local addresses as they might be NAT-ed external
    // addresses that are not always reachable from the inside.
    // But is it safe to assume IPFS always listens on loopback and local addresses?
    List<MultiAddress> myAddrs = ((List<String>) res.get("Addresses"))
        .stream()
        .map(MultiAddress::new)
        .filter(addr -> {
          try {
            InetAddress inetAddress = InetAddress.getByName(addr.getHost());
            if (inetAddress instanceof Inet6Address) {
              return false;
            }
            return inetAddress.isSiteLocalAddress()
                || inetAddress.isLinkLocalAddress()
                || inetAddress.isLoopbackAddress();
          } catch (UnknownHostException e) {
            return false;
          }
        })
        .collect(Collectors.toList());
    this.myself = new IPFSPeer(this, myID, myAddrs);

    return this.myself;
  }

  public Multihash resolve(String prefix, String path, boolean recursive) {
    Map<String, String> result = timedFailure(
        (args) -> clientCompat.resolve((String) args.get(0), (String) args.get(1), (boolean) args.get(2)),
        ImmutableList.<Object>of(prefix, path, recursive),
        timeouts.get(IPFSTimeOut.FIND_PEER_INFO)
    );
    if (!result.containsKey("Path")) {
      return null;
    }

    // the path returned is of form /ipfs/Qma...
    String hashString = result.get("Path").split("/")[2];
    return Cid.decode(hashString);
  }

  @FunctionalInterface
  public interface ThrowingFunction<T, R, E extends Exception> {
    R apply(final T in) throws E;
  }

  @FunctionalInterface
  public interface ThrowingSupplier<R, E extends Exception> {
    R get() throws E;
  }

  /**
   * Execute a time-critical operation op within time timeout. Causes the query to fail completely
   * if the operation times out.
   *
   * @param op      a Function that represents the operation to perform
   * @param in      the parameter for op
   * @param timeout consider the execution has timed out after this amount of time in seconds
   * @param <T>     Input type
   * @param <R>     Return type
   * @param <E>     Type of checked exception op throws
   * @return R the result of the operation
   * @throws E when the function throws an E
   */
  public <T, R, E extends Exception> R timedFailure(ThrowingFunction<T, R, E> op, T in, int timeout) throws E {
    Callable<R> task = () -> op.apply(in);
    return timedFailure(task, timeout, TimeUnit.SECONDS);
  }

  public <R, E extends Exception> R timedFailure(ThrowingSupplier<R, E> op, int timeout) throws E {
    Callable<R> task = op::get;
    return timedFailure(task, timeout, TimeUnit.SECONDS);
  }

  private <R, E extends Exception> R timedFailure(Callable<R> task, int timeout, TimeUnit timeUnit) throws E {
    Future<R> res = executorService.submit(task);
    try {
      return res.get(timeout, timeUnit);
    } catch (ExecutionException e) {
      throw (E) e.getCause();
    } catch (TimeoutException e) {
      throw UserException.executionError(e).message("IPFS operation timed out").build(logger);
    } catch (CancellationException | InterruptedException e) {
      throw UserException.executionError(e).message("IPFS operation was cancelled or interrupted").build(logger);
    }
  }

  /*
   * DRILL-7753: implement a more advanced algorithm that picks optimal addresses. Maybe check reachability, latency
   * and bandwidth?
   */

  /**
   * Choose a peer's network address from its advertised Multiaddresses.
   * Prefer globally routable address over local addresses.
   *
   * @param peerAddrs Multiaddresses obtained from IPFS.DHT.findprovs
   * @return network address
   */
  public static Optional<String> pickPeerHost(List<MultiAddress> peerAddrs) {
    String localAddr = null;
    for (MultiAddress addr : peerAddrs) {
      String host = addr.getHost();
      try {
        InetAddress inetAddress = InetAddress.getByName(host);
        if (inetAddress instanceof Inet6Address) {
          // ignore IPv6 addresses
          continue;
        }
        if (inetAddress.isSiteLocalAddress() || inetAddress.isLinkLocalAddress()) {
          localAddr = host;
        } else {
          return Optional.of(host);
        }
      } catch (UnknownHostException ignored) {
      }
    }

    return Optional.ofNullable(localAddr);
  }

  public Optional<String> getPeerDrillHostname(Multihash peerId) {
    return getPeerData(peerId, "drill-hostname").map(Strings::fromByteArray);
  }

  /**
   * Check if an IPFS peer is also running a Drillbit so that it can be used to execute a part of a query.
   *
   * @param peerId the id of the peer
   * @return if the peer is Drill-ready
   */
  public boolean isDrillReady(Multihash peerId) {
    try {
      return getPeerData(peerId, "drill-ready").isPresent();
    } catch (RuntimeException e) {
      return false;
    }
  }

  public Optional<Multihash> getIPNSDataHash(Multihash peerId) {
    Optional<List<MerkleNode>> links = getPeerLinks(peerId);
    if (!links.isPresent()) {
      return Optional.empty();
    }

    return links.get().stream()
        .filter(l -> l.name.equals(Optional.of("drill-data")))
        .findFirst()
        .map(l -> l.hash);
  }

  /**
   * Get from IPFS data under a peer's ID, i.e. the data identified by /ipfs/{ID}/key.
   *
   * @param peerId the peer's ID
   * @param key    key
   * @return data in bytes
   */
  private Optional<byte[]> getPeerData(Multihash peerId, String key) {
    Optional<List<MerkleNode>> links = getPeerLinks(peerId);
    if (!links.isPresent()) {
      return Optional.empty();
    }

    for (MerkleNode link : links.get()) {
      if (link.name.equals(Optional.of(key))) {
        try {
          byte[] result = timedFailure(client.object::data, link.hash, timeouts.get(FETCH_DATA));
          return Optional.of(result);
        } catch (IOException e) {
          return Optional.empty();
        }
      }
    }
    return Optional.empty();
  }

  /**
   * Get all the links under a peer's ID.
   *
   * @param peerId peer's ID
   * @return List of links
   */
  private Optional<List<MerkleNode>> getPeerLinks(Multihash peerId) {
    try {
      Optional<String> optionalPath = clientCompat.name.resolve(peerId, timeouts.get(FIND_PEER_INFO), executorService);
      if (!optionalPath.isPresent()) {
        return Optional.empty();
      }
      String path = optionalPath.get().substring(6); // path starts with /ipfs/Qm...

      List<MerkleNode> links = timedFailure(
          client.object::get,
          Cid.decode(path),
          timeouts.get(FETCH_DATA)
      ).links;
      if (links.size() > 0) {
        return Optional.of(links);
      }
    } catch (IOException ignored) {
    }
    return Optional.empty();
  }
}
