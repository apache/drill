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
package org.apache.drill.exec.rpc.data;


import org.apache.drill.exec.proto.BitData.BitClientHandshake;
import org.apache.drill.exec.proto.BitData.BitServerHandshake;
import org.apache.drill.exec.proto.BitData.FragmentRecordBatch;
import org.apache.drill.exec.proto.BitData.RpcType;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.rpc.Acks;
import org.apache.drill.exec.rpc.Response;
import org.apache.drill.exec.rpc.RpcConfig;

public class DataRpcConfig {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DataRpcConfig.class);

  public static RpcConfig MAPPING = RpcConfig.newBuilder("BIT-DATA-RPC-MAPPING") //
      .add(RpcType.HANDSHAKE, BitClientHandshake.class, RpcType.HANDSHAKE, BitServerHandshake.class)
      .add(RpcType.REQ_RECORD_BATCH, FragmentRecordBatch.class, RpcType.ACK, Ack.class)
      .build();

  public static int RPC_VERSION = 2;

  public static final Response OK = new Response(RpcType.ACK, Acks.OK);
}
