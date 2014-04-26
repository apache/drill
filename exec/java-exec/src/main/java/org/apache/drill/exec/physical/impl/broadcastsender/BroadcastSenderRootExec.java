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
package org.apache.drill.exec.physical.impl.broadcastsender;

import java.util.List;

import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.BroadcastSender;
import org.apache.drill.exec.physical.impl.RootExec;
import org.apache.drill.exec.physical.impl.SendingAccountor;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.ExecProtos;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.GeneralRPCProtos;
import org.apache.drill.exec.record.FragmentWritableBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.rpc.DrillRpcFuture;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.data.DataTunnel;

/**
 * Broadcast Sender broadcasts incoming batches to all receivers (one or more).
 * This is useful in cases such as broadcast join where sending the entire table to join
 * to all nodes is cheaper than merging and computing all the joins in the same node.
 */
public class BroadcastSenderRootExec implements RootExec {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BroadcastSenderRootExec.class);
  private final FragmentContext context;
  private final BroadcastSender config;
  private final DataTunnel[] tunnels;
  private final ExecProtos.FragmentHandle handle;
  private volatile boolean ok;
  private final RecordBatch incoming;
  private final DrillRpcFuture[] responseFutures;

  public BroadcastSenderRootExec(FragmentContext context,
                                 RecordBatch incoming,
                                 BroadcastSender config) {
    this.ok = true;
    this.context = context;
    this.incoming = incoming;
    this.config = config;
    this.handle = context.getHandle();
    List<DrillbitEndpoint> destinations = config.getDestinations();
    this.tunnels = new DataTunnel[destinations.size()];
    for(int i = 0; i < destinations.size(); ++i) {
      FragmentHandle opp = handle.toBuilder().setMajorFragmentId(config.getOppositeMajorFragmentId()).setMinorFragmentId(i).build();
      tunnels[i] = context.getDataTunnel(destinations.get(i), opp);
    }
    responseFutures = new DrillRpcFuture[destinations.size()];
  }

  @Override
  public boolean next() {
    if(!ok) {
      return false;
    }

    RecordBatch.IterOutcome out = incoming.next();
    logger.debug("Outcome of sender next {}", out);
    switch(out){
      case STOP:
      case NONE:
        for (int i = 0; i < tunnels.length; ++i) {
          FragmentWritableBatch b2 = FragmentWritableBatch.getEmptyLast(handle.getQueryId(), handle.getMajorFragmentId(), handle.getMinorFragmentId(), config.getOppositeMajorFragmentId(), i);
          responseFutures[i] = tunnels[i].sendRecordBatch(context, b2);
        }

        waitAllFutures(false);
        return false;

      case OK_NEW_SCHEMA:
      case OK:
        WritableBatch writableBatch = incoming.getWritableBatch();
        for (int i = 0; i < tunnels.length; ++i) {
          FragmentWritableBatch batch = new FragmentWritableBatch(false, handle.getQueryId(), handle.getMajorFragmentId(), handle.getMinorFragmentId(), config.getOppositeMajorFragmentId(), i, writableBatch);
          if(i > 0) {
            writableBatch.retainBuffers();
          }
          responseFutures[i] = tunnels[i].sendRecordBatch(context, batch);
        }

        return waitAllFutures(true);

      case NOT_YET:
      default:
        throw new IllegalStateException();
    }
  }

  private boolean waitAllFutures(boolean haltOnError) {
    for (DrillRpcFuture<?> responseFuture : responseFutures) {
      try {
        GeneralRPCProtos.Ack ack = (GeneralRPCProtos.Ack) responseFuture.checkedGet();
        if(!ack.getOk()) {
          ok = false;
          if (haltOnError) {
            return false;
          }
        }
      } catch (RpcException e) {
        logger.error("Error sending batch to receiver: " + e);
        ok = false;
        if (haltOnError) {
          return false;
        }
      }
    }
    return true;
  }

  @Override
  public void stop() {
      ok = false;
      incoming.kill();
  }
}
