/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.exec.rpc.bit;

import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.ExecProtos.FragmentStatus;
import org.apache.drill.exec.proto.ExecProtos.PlanFragment;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.rpc.DrillRpcFuture;
import org.apache.drill.exec.rpc.RpcBus;

import com.google.common.io.Closeables;

/**
 * Interface provided for communication between two bits.  Provided by both a server and a client implementation.
 */
public class BitTunnel {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BitTunnel.class);

  final RpcBus<?> bus;

  public BitTunnel(RpcBus<?> bus){
    this.bus = bus;
  }
  
  public DrillRpcFuture<Ack> sendRecordBatch(FragmentContext context, RecordBatch batch){
    return null;
  }
  
  public DrillRpcFuture<FragmentHandle> sendFragment(FragmentContext context, PlanFragment fragment){
    return null;
  }
  
  public DrillRpcFuture<Ack> cancelFragment(FragmentContext context, FragmentHandle handle){
    return null;
  }
  
  public DrillRpcFuture<FragmentStatus> getFragmentStatus(FragmentContext context, FragmentHandle handle){
    return null;
  }
  
  public void shutdownIfClient(){
    if(bus.isClient()) Closeables.closeQuietly(bus);
  }

}
