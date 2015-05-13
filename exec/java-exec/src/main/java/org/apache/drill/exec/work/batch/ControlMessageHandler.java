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

import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.proto.BitControl.FinishedReceiver;
import org.apache.drill.exec.proto.BitControl.FragmentStatus;
import org.apache.drill.exec.proto.BitControl.InitializeFragments;
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
import org.apache.drill.exec.rpc.UserRpcException;
import org.apache.drill.exec.rpc.control.ControlConnection;
import org.apache.drill.exec.rpc.control.ControlRpcConfig;
import org.apache.drill.exec.rpc.control.ControlTunnel;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.work.WorkManager.WorkerBee;
import org.apache.drill.exec.work.foreman.Foreman;
import org.apache.drill.exec.work.fragment.FragmentExecutor;
import org.apache.drill.exec.work.fragment.FragmentManager;
import org.apache.drill.exec.work.fragment.NonRootFragmentManager;
import org.apache.drill.exec.work.fragment.NonRootStatusReporter;

public class ControlMessageHandler {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ControlMessageHandler.class);
  private final WorkerBee bee;

  public ControlMessageHandler(final WorkerBee bee) {
    this.bee = bee;
  }

  public Response handle(final ControlConnection connection, final int rpcType,
      final ByteBuf pBody, final ByteBuf dBody) throws RpcException {
    if (RpcConstants.EXTRA_DEBUGGING) {
      logger.debug("Received bit com message of type {}", rpcType);
    }

    switch (rpcType) {

    case RpcType.REQ_CANCEL_FRAGMENT_VALUE: {
      final FragmentHandle handle = get(pBody, FragmentHandle.PARSER);
      cancelFragment(handle);
      return ControlRpcConfig.OK;
    }

    case RpcType.REQ_RECEIVER_FINISHED_VALUE: {
      final FinishedReceiver finishedReceiver = get(pBody, FinishedReceiver.PARSER);
      receivingFragmentFinished(finishedReceiver);
      return ControlRpcConfig.OK;
    }

    case RpcType.REQ_FRAGMENT_STATUS_VALUE:
      bee.getContext().getWorkBus().statusUpdate( get(pBody, FragmentStatus.PARSER));
      // TODO: Support a type of message that has no response.
      return ControlRpcConfig.OK;

    case RpcType.REQ_QUERY_CANCEL_VALUE: {
      final QueryId queryId = get(pBody, QueryId.PARSER);
      final Foreman foreman = bee.getForemanForQueryId(queryId);
      if (foreman != null) {
        foreman.cancel();
        return ControlRpcConfig.OK;
      } else {
        return ControlRpcConfig.FAIL;
      }
    }

    case RpcType.REQ_INITIALIZE_FRAGMENTS_VALUE: {
      final InitializeFragments fragments = get(pBody, InitializeFragments.PARSER);
      for(int i = 0; i < fragments.getFragmentCount(); i++) {
        startNewRemoteFragment(fragments.getFragment(i));
      }
      return ControlRpcConfig.OK;
    }

    case RpcType.REQ_QUERY_STATUS_VALUE: {
      final QueryId queryId = get(pBody, QueryId.PARSER);
      final Foreman foreman = bee.getForemanForQueryId(queryId);
      if (foreman == null) {
        throw new RpcException("Query not running on node.");
      }
      final QueryProfile profile = foreman.getQueryManager().getQueryProfile();
      return new Response(RpcType.RESP_QUERY_STATUS, profile);
    }

    case RpcType.REQ_UNPAUSE_FRAGMENT_VALUE: {
      final FragmentHandle handle = get(pBody, FragmentHandle.PARSER);
      resumeFragment(handle);
      return ControlRpcConfig.OK;
    }

    default:
      throw new RpcException("Not yet supported.");
    }
  }

  private void startNewRemoteFragment(final PlanFragment fragment) throws UserRpcException {
    logger.debug("Received remote fragment start instruction", fragment);

    final DrillbitContext drillbitContext = bee.getContext();
    try {
      // we either need to start the fragment if it is a leaf fragment, or set up a fragment manager if it is non leaf.
      if (fragment.getLeafFragment()) {
        final FragmentContext context = new FragmentContext(drillbitContext, fragment,
            drillbitContext.getFunctionImplementationRegistry());
        final ControlTunnel tunnel = drillbitContext.getController().getTunnel(fragment.getForeman());
        final NonRootStatusReporter listener = new NonRootStatusReporter(context, tunnel);
        final FragmentExecutor fr = new FragmentExecutor(context, fragment, listener);
        bee.addFragmentRunner(fr);
      } else {
        // isIntermediate, store for incoming data.
        final NonRootFragmentManager manager = new NonRootFragmentManager(fragment, drillbitContext);
        drillbitContext.getWorkBus().addFragmentManager(manager);
      }

    } catch (final Exception e) {
        throw new UserRpcException(drillbitContext.getEndpoint(),
            "Failure while trying to start remote fragment", e);
    } catch (final OutOfMemoryError t) {
      if (t.getMessage().startsWith("Direct buffer")) {
        throw new UserRpcException(drillbitContext.getEndpoint(),
            "Out of direct memory while trying to start remote fragment", t);
      } else {
        throw t;
      }
    }
  }

  /* (non-Javadoc)
   * @see org.apache.drill.exec.work.batch.BitComHandler#cancelFragment(org.apache.drill.exec.proto.ExecProtos.FragmentHandle)
   */
  private Ack cancelFragment(final FragmentHandle handle) {
    /**
     * For case 1, see {@link org.apache.drill.exec.work.foreman.QueryManager#cancelExecutingFragments}.
     * In comments below, "active" refers to fragment states: SENDING, AWAITING_ALLOCATION, RUNNING and
     * "inactive" refers to FINISHED, CANCELLATION_REQUESTED, CANCELLED, FAILED
     */

    // Case 2: Cancel active intermediate fragment. Such a fragment will be in the work bus. Delegate cancel to the
    // work bus.
    final boolean removed = bee.getContext().getWorkBus().cancelAndRemoveFragmentManagerIfExists(handle);
    if (removed) {
      return Acks.OK;
    }

    // Case 3: Cancel active leaf fragment. Such a fragment will be with the worker bee if and only if it is running.
    // Cancel directly in this case.
    final FragmentExecutor runner = bee.getFragmentRunner(handle);
    if (runner != null) {
      runner.cancel();
      return Acks.OK;
    }

    // Other cases: Fragment completed or does not exist. Currently known cases:
    // (1) Leaf or intermediate fragment that is inactive: although we should not receive a cancellation
    //     request; it is possible that before the fragment state was updated in the QueryManager, this handler
    //     received a cancel signal.
    // (2) Unknown fragment.
    logger.warn("Dropping request to cancel fragment. {} does not exist.", QueryIdHelper.getFragmentId(handle));
    return Acks.OK;
  }

  private Ack resumeFragment(final FragmentHandle handle) {
    // resume a pending fragment
    final FragmentManager manager = bee.getContext().getWorkBus().getFragmentManagerIfExists(handle);
    if (manager != null) {
      manager.unpause();
      return Acks.OK;
    }

    // resume a paused fragment
    final FragmentExecutor runner = bee.getFragmentRunner(handle);
    if (runner != null) {
      runner.unpause();
      return Acks.OK;
    }

    // fragment completed or does not exist
    logger.warn("Dropping request to resume fragment. {} does not exist.", QueryIdHelper.getFragmentId(handle));
    return Acks.OK;
  }

  private Ack receivingFragmentFinished(final FinishedReceiver finishedReceiver) {

    final FragmentManager manager =
        bee.getContext().getWorkBus().getFragmentManagerIfExists(finishedReceiver.getSender());

    FragmentExecutor executor;
    if (manager != null) {
      manager.receivingFragmentFinished(finishedReceiver.getReceiver());
    } else {
      executor = bee.getFragmentRunner(finishedReceiver.getSender());
      if (executor != null) {
        executor.receivingFragmentFinished(finishedReceiver.getReceiver());
      } else {
        logger.warn(
            "Dropping request for early fragment termination for path {} -> {} as path to executor unavailable.",
            QueryIdHelper.getFragmentId(finishedReceiver.getSender()),
            QueryIdHelper.getFragmentId(finishedReceiver.getReceiver()));
      }
    }

    return Acks.OK;
  }
}
