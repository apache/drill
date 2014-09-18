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
package org.apache.drill.exec.work.batch;

import static org.apache.drill.exec.rpc.RpcBus.get;
import io.netty.buffer.ByteBuf;

import java.io.IOException;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.base.FragmentRoot;
import org.apache.drill.exec.proto.BitControl.FinishedReceiver;
import org.apache.drill.exec.proto.BitControl.FragmentStatus;
import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.proto.BitControl.RpcType;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.QueryProfile;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
import org.apache.drill.exec.rpc.Acks;
import org.apache.drill.exec.rpc.Response;
import org.apache.drill.exec.rpc.RpcConstants;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.control.ControlConnection;
import org.apache.drill.exec.rpc.control.ControlTunnel;
import org.apache.drill.exec.rpc.data.DataRpcConfig;
import org.apache.drill.exec.work.WorkManager.WorkerBee;
import org.apache.drill.exec.work.foreman.Foreman;
import org.apache.drill.exec.work.foreman.QueryStatus;
import org.apache.drill.exec.work.fragment.FragmentExecutor;
import org.apache.drill.exec.work.fragment.FragmentManager;
import org.apache.drill.exec.work.fragment.NonRootStatusReporter;

public class ControlHandlerImpl implements ControlMessageHandler {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ControlHandlerImpl.class);

  private final WorkerBee bee;

  public ControlHandlerImpl(WorkerBee bee) {
    super();
    this.bee = bee;
  }

  @Override
  public Response handle(ControlConnection connection, int rpcType, ByteBuf pBody, ByteBuf dBody) throws RpcException {
    if (RpcConstants.EXTRA_DEBUGGING) {
      logger.debug("Received bit com message of type {}", rpcType);
    }

    switch (rpcType) {

    case RpcType.REQ_CANCEL_FRAGMENT_VALUE:
      FragmentHandle handle = get(pBody, FragmentHandle.PARSER);
      cancelFragment(handle);
      return DataRpcConfig.OK;

    case RpcType.REQ_RECEIVER_FINISHED_VALUE:
      FinishedReceiver finishedReceiver = get(pBody, FinishedReceiver.PARSER);
      receivingFragmentFinished(finishedReceiver);
      return DataRpcConfig.OK;

    case RpcType.REQ_FRAGMENT_STATUS_VALUE:
      bee.getContext().getWorkBus().status( get(pBody, FragmentStatus.PARSER));
      // TODO: Support a type of message that has no response.
      return DataRpcConfig.OK;

    case RpcType.REQ_INIATILIZE_FRAGMENT_VALUE:
      PlanFragment fragment = get(pBody, PlanFragment.PARSER);
      try {
        startNewRemoteFragment(fragment);
        return DataRpcConfig.OK;

      } catch (ExecutionSetupException e) {
        logger.error("Failure while attempting to start remote fragment.", fragment);
        return new Response(RpcType.ACK, Acks.FAIL);
      }

    case RpcType.REQ_QUERY_STATUS_VALUE:
      QueryId queryId = get(pBody, QueryId.PARSER);
      Foreman foreman = bee.getForemanForQueryId(queryId);
      QueryProfile profile;
      if (foreman == null) {
        try {
          profile = bee.getContext().getPersistentStoreProvider().getPStore(QueryStatus.QUERY_PROFILE).get(QueryIdHelper.getQueryId(queryId));
        } catch (IOException e) {
          throw new RpcException("Failed to get persistent store", e);
        }
      } else {
        profile = bee.getForemanForQueryId(queryId).getQueryStatus().getAsProfile(true);
      }
      return new Response(RpcType.RESP_QUERY_STATUS, profile);

    default:
      throw new RpcException("Not yet supported.");
    }

  }

  /* (non-Javadoc)
   * @see org.apache.drill.exec.work.batch.BitComHandler#startNewRemoteFragment(org.apache.drill.exec.proto.ExecProtos.PlanFragment)
   */
  @Override
  public void startNewRemoteFragment(PlanFragment fragment) throws ExecutionSetupException{
    logger.debug("Received remote fragment start instruction", fragment);
    FragmentContext context = new FragmentContext(bee.getContext(), fragment, null, bee.getContext().getFunctionImplementationRegistry());
    ControlTunnel tunnel = bee.getContext().getController().getTunnel(fragment.getForeman());

    NonRootStatusReporter listener = new NonRootStatusReporter(context, tunnel);
    try {
      FragmentRoot rootOperator = bee.getContext().getPlanReader().readFragmentOperator(fragment.getFragmentJson());
      FragmentExecutor fr = new FragmentExecutor(context, bee, rootOperator, listener);
      bee.addFragmentRunner(fr);
    } catch (Exception e) {
      listener.fail(fragment.getHandle(), "Failure due to uncaught exception", e);
    } catch (OutOfMemoryError t) {
      if (t.getMessage().startsWith("Direct buffer")) {
        listener.fail(fragment.getHandle(), "Failure due to error", t);
      } else {
        throw t;
      }
    }

  }

  /* (non-Javadoc)
   * @see org.apache.drill.exec.work.batch.BitComHandler#cancelFragment(org.apache.drill.exec.proto.ExecProtos.FragmentHandle)
   */
  @Override
  public Ack cancelFragment(FragmentHandle handle) {
    FragmentManager manager = bee.getContext().getWorkBus().getFragmentManager(handle);
    if (manager != null) {
      // try remote fragment cancel.
      manager.cancel();
    } else {
      // then try local cancel.
      FragmentExecutor runner = bee.getFragmentRunner(handle);
      if (runner != null) {
        runner.cancel();
      }
    }

    return Acks.OK;
  }

  public Ack receivingFragmentFinished(FinishedReceiver finishedReceiver) {
    FragmentManager manager = bee.getContext().getWorkBus().getFragmentManager(finishedReceiver.getSender());

    FragmentExecutor executor;
    if (manager != null) {
      executor = manager.getRunnable();
    } else {
      // then try local cancel.
      executor = bee.getFragmentRunner(finishedReceiver.getSender());
    }

    if (executor != null) {
      executor.receivingFragmentFinished(finishedReceiver.getReceiver());
    }

    return Acks.OK;
  }

}
