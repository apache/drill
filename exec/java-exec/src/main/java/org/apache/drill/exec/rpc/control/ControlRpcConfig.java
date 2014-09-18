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
package org.apache.drill.exec.rpc.control;


import org.apache.drill.exec.proto.BitControl.BitControlHandshake;
import org.apache.drill.exec.proto.BitControl.FinishedReceiver;
import org.apache.drill.exec.proto.BitControl.FragmentStatus;
import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.proto.BitControl.RpcType;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.QueryProfile;
import org.apache.drill.exec.rpc.Acks;
import org.apache.drill.exec.rpc.Response;
import org.apache.drill.exec.rpc.RpcConfig;

public class ControlRpcConfig {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ControlRpcConfig.class);

  public static RpcConfig MAPPING = RpcConfig.newBuilder("BIT-CONTROL-RPC-MAPPING") //
      .add(RpcType.HANDSHAKE, BitControlHandshake.class, RpcType.HANDSHAKE, BitControlHandshake.class)
      .add(RpcType.REQ_INIATILIZE_FRAGMENT, PlanFragment.class, RpcType.ACK, Ack.class)
      .add(RpcType.REQ_CANCEL_FRAGMENT, FragmentHandle.class, RpcType.ACK, Ack.class)
      .add(RpcType.REQ_RECEIVER_FINISHED, FinishedReceiver.class, RpcType.ACK, Ack.class)
      .add(RpcType.REQ_FRAGMENT_STATUS, FragmentStatus.class, RpcType.ACK, Ack.class)
      .add(RpcType.REQ_QUERY_STATUS, QueryId.class, RpcType.RESP_QUERY_STATUS, QueryProfile.class)
      .build();

  public static int RPC_VERSION = 2;

  public static final Response OK = new Response(RpcType.ACK, Acks.OK);
}
