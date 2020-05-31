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
import io.ipfs.multihash.Multihash;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Compatibility fixes for java-ipfs-http-client library
 */
public class IPFSCompat {
  public final String host;
  public final int port;
  private final String version;
  public final String protocol;
  public final int readTimeout;
  public static final int DEFAULT_READ_TIMEOUT = 0;

  public final DHT dht = new DHT();
  public final Name name = new Name();

  public IPFSCompat(IPFS ipfs) {
    this(ipfs.host, ipfs.port);
  }

  public IPFSCompat(String host, int port) {
    this(host, port, "/api/v0/", false, DEFAULT_READ_TIMEOUT);
  }

  public IPFSCompat(String host, int port, String version, boolean ssl, int readTimeout) {
    this.host = host;
    this.port = port;

    if (ssl) {
      this.protocol = "https";
    } else {
      this.protocol = "http";
    }

    this.version = version;
    this.readTimeout = readTimeout;
  }

  /**
   * Resolve names to IPFS CIDs.
   * See <a href="https://docs.ipfs.io/reference/http/api/#api-v0-resolve">resolve in IPFS doc</a>.
   *
   * @param scheme    the scheme of the name to resolve, usually IPFS or IPNS
   * @param path      the path to the object
   * @param recursive whether recursively resolve names until it is a IPFS CID
   * @return a Map of JSON object, with the result as the value of key "Path"
   */
  public Map resolve(String scheme, String path, boolean recursive) {
    AtomicReference<Map> ret = new AtomicReference<>();
    getObjectStream(
        "resolve?arg=/" + scheme + "/" + path + "&r=" + recursive,
        res -> {
          ret.set((Map) res);
          return true;
        },
        err -> {
          throw new RuntimeException(err);
        }
    );
    return ret.get();
  }

  /**
   * As defined in https://github.com/libp2p/go-libp2p-core/blob/b77fd280f2bfcce22f10a000e8e1d9ec53c47049/routing/query.go#L16
   */
  public enum DHTQueryEventType {
    // Sending a query to a peer.
    SendingQuery,
    // Got a response from a peer.
    PeerResponse,
    // Found a "closest" peer (not currently used).
    FinalPeer,
    // Got an error when querying.
    QueryError,
    // Found a provider.
    Provider,
    // Found a value.
    Value,
    // Adding a peer to the query.
    AddingPeer,
    // Dialing a peer.
    DialingPeer
  }

  public class DHT {
    /**
     * Find internet addresses of a given peer.
     * See <a href="https://docs.ipfs.io/reference/http/api/#api-v0-dht-findpeer">dht/findpeer in IPFS doc</a>.
     *
     * @param id       the id of the peer to query
     * @param timeout  timeout value in seconds
     * @param executor executor
     * @return List of Multiaddresses of the peer
     */
    public List<String> findpeerListTimeout(Multihash id, int timeout, ExecutorService executor) {
      AtomicReference<List<String>> ret = new AtomicReference<>(new ArrayList<>());
      timeLimitedExec(
          "dht/findpeer?arg=" + id,
          timeout,
          res -> {
            Map peer = (Map) res;
            if (peer == null) {
              return false;
            }
            if ((int) peer.get("Type") != DHTQueryEventType.FinalPeer.ordinal()) {
              return false;
            }
            List<Map> responses = (List<Map>) peer.get("Responses");
            if (responses == null || responses.size() == 0) {
              return false;
            }
            // FinalPeer responses have exactly one response
            Map<String, List<String>> response = responses.get(0);
            if (response == null) {
              return false;
            }
            List<String> addrs = response.get("Addrs");

            ret.set(addrs);
            return true;
          },
          err -> {
            if (!(err instanceof TimeoutException)) {
              throw new RuntimeException(err);
            }
          },
          executor
      );
      if (ret.get().size() > 0) {
        return ret.get();
      } else {
        return Collections.emptyList();
      }
    }

    /**
     * Find providers of a given CID.
     * See <a href="https://docs.ipfs.io/reference/http/api/#api-v0-dht-findprovs">dht/findprovs in IPFS doc</a>.
     *
     * @param id       the CID of the IPFS object
     * @param timeout  timeout value in seconds
     * @param executor executor
     * @return List of Multihash of providers of the object
     */
    public List<String> findprovsListTimeout(Multihash id, int maxPeers, int timeout, ExecutorService executor) {
      AtomicReference<List<String>> ret = new AtomicReference<>(new ArrayList<>());
      timeLimitedExec(
          "dht/findprovs?arg=" + id + "&n=" + maxPeers,
          timeout,
          res -> {
            Map peer = (Map) res;
            if (peer == null) {
              return false;
            }
            if ((int) peer.get("Type") != DHTQueryEventType.Provider.ordinal()) {
              return false;
            }
            List<Map> responses = (List<Map>) peer.get("Responses");
            if (responses == null || responses.size() == 0) {
              return false;
            }
            // One Provider message contains only one provider
            Map<String, String> response = responses.get(0);
            if (response == null) {
              return false;
            }
            String providerID = response.get("ID");

            ret.get().add(providerID);
            return ret.get().size() >= maxPeers;
          },
          err -> {
            if (!(err instanceof TimeoutException)) {
              throw new RuntimeException(err);
            }
          },
          executor
      );
      if (ret.get().size() > 0) {
        return ret.get();
      } else {
        return Collections.emptyList();
      }
    }
  }

  public class Name {
    /**
     * Resolve a IPNS name.
     * See <a href="https://docs.ipfs.io/reference/http/api/#api-v0-name-resolve">name/resolve in IPFS doc</a>.
     *
     * @param hash     the IPNS name to resolve
     * @param timeout  timeout value in seconds
     * @param executor executor
     * @return a Multihash of resolved name
     */
    public Optional<String> resolve(Multihash hash, int timeout, ExecutorService executor) {
      AtomicReference<String> ret = new AtomicReference<>();
      timeLimitedExec(
          "name/resolve?arg=" + hash,
          timeout,
          res -> {
            Map peer = (Map) res;
            if (peer != null) {
              ret.set((String) peer.get(("Path")));
              return true;
            }
            return false;
          },
          err -> {
            if (!(err instanceof TimeoutException)) {
              throw new RuntimeException(err);
            }
          },
          executor
      );
      return Optional.ofNullable(ret.get());
    }
  }

  private void timeLimitedExec(String path, int timeout, Predicate<Object> processor, Consumer<Exception> error,
                               ExecutorService executor) {
    CompletableFuture<Void> f = CompletableFuture.runAsync(
        () -> getObjectStream(path, processor, error),
        executor
    );
    try {
      f.get(timeout, TimeUnit.SECONDS);
    } catch (TimeoutException | ExecutionException | InterruptedException e) {
      f.cancel(true);
      error.accept(e);
    }
  }

  private void getObjectStream(String path, Predicate<Object> processor, Consumer<Exception> error) {
    byte LINE_FEED = (byte) 10;

    try {
      InputStream in = getStream(path);
      ByteArrayOutputStream resp = new ByteArrayOutputStream();

      byte[] buf = new byte[4096];
      int r;
      while ((r = in.read(buf)) >= 0) {
        resp.write(buf, 0, r);
        if (buf[r - 1] == LINE_FEED) {
          try {
            boolean done = processor.test(JSONParser.parse(resp.toString()));
            if (done) {
              break;
            }
            resp.reset();
          } catch (IllegalStateException e) {
            in.close();
            resp.close();
            error.accept(e);
          }
        }
      }
      in.close();
      resp.close();
    } catch (IOException e) {
      error.accept(e);
    }
  }

  private InputStream getStream(String path) throws IOException {
    URL target = new URL(protocol, host, port, version + path);
    HttpURLConnection conn = (HttpURLConnection) target.openConnection();
    conn.setRequestMethod("POST");
    conn.setRequestProperty("Content-Type", "application/json");
    conn.setReadTimeout(readTimeout);
    return conn.getInputStream();
  }
}
