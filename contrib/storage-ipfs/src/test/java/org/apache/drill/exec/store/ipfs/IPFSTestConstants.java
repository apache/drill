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

import io.ipfs.cid.Cid;
import io.ipfs.multiaddr.MultiAddress;
import io.ipfs.multihash.Multihash;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public interface IPFSTestConstants {
  String MOCK_NODE_ID_STRING = "QmP14kRKf1mR6LAYgfuuMirscgZNYbzMCHQ1ebe4bBKdah";
  Multihash MOCK_NODE_ID_MULTIHASH = Multihash.fromBase58(MOCK_NODE_ID_STRING);
  String MOCK_NODE_ADDR = "127.0.0.1";
  int MOCK_NODE_IPFS_SWARM_PORT = 4001;
  int MOCK_NODE_IPFS_API_PORT = 5001;
  List<String> MOCK_NODE_ADDRS = ImmutableList.of(
      String.format("/ip4/%s/tcp/%d/ipfs/%s", MOCK_NODE_ADDR, MOCK_NODE_IPFS_SWARM_PORT, MOCK_NODE_ID_STRING)
  );
  List<MultiAddress> MOCK_NODE_MULTIADDRS = MOCK_NODE_ADDRS.stream().map(MultiAddress::new).collect(Collectors.toList());

  String SIMPLE_DATASET_HASH_STRING = "QmcbeavnEofA6NjG7vkpe1yLJo6En6ML4JnDooDn1BbKmR";
  Multihash SIMPLE_DATASET_MULTIHASH = Multihash.fromBase58(SIMPLE_DATASET_HASH_STRING);
  Cid SIMPLE_DATASET_CID_V1 = Cid.build(1, Cid.Codec.DagProtobuf, SIMPLE_DATASET_MULTIHASH);
  String SIMPLE_DATASET_CID_V1_STRING = SIMPLE_DATASET_CID_V1.toString();

  /**
   * Chunked dataset layout:
   * top object: QmSeX1YAGWMXoPrgeKBTq2Be6NdRzTVESeeWyt7mQFuvzo
   * +-- 1 QmSmDFd1GcLPyYtscdtkBCj7gbNKiJ8MkaBPEFMz9orPEi chunked-json-1.json (162 bytes)
   * +-- 2 QmQVBWTZ7MZjwHv5q9qG3zLzczsh8PGAVRWhF2gKsrj1hP chunked-json-2.json (159 bytes)
   * +-- 3 QmY8ghdB3mwdUAdBmft3bdgzPVcq8bCvtqTRd9wu3LjyTd chunked-json-3.json (89 bytes)
   */
  String CHUNKED_DATASET_HASH_STRING = "QmSeX1YAGWMXoPrgeKBTq2Be6NdRzTVESeeWyt7mQFuvzo";
  Multihash CHUNKED_DATASET_MULTIHASH = Multihash.fromBase58(CHUNKED_DATASET_HASH_STRING);
  Map<String, Multihash> CHUNKS_MULTIHASH = ImmutableMap.of(
      "chunked-json-1.json", Multihash.fromBase58("QmSmDFd1GcLPyYtscdtkBCj7gbNKiJ8MkaBPEFMz9orPEi"),
      "chunked-json-2.json", Multihash.fromBase58("QmQVBWTZ7MZjwHv5q9qG3zLzczsh8PGAVRWhF2gKsrj1hP"),
      "chunked-json-3.json", Multihash.fromBase58("QmY8ghdB3mwdUAdBmft3bdgzPVcq8bCvtqTRd9wu3LjyTd")
  );

  static String getQueryPath(Multihash dataset) {
    return IPFSTestConstants.getQueryPath(dataset.toString());
  }

  static String getQueryPath(String dataset) {
    return String.format("/ipfs/%s#json", dataset);
  }
}
