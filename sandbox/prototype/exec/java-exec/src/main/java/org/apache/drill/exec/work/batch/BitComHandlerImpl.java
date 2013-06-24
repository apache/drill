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
package org.apache.drill.exec.work.batch;

import static org.apache.drill.exec.rpc.RpcBus.get;
import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.concurrent.ConcurrentMap;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.exception.FragmentSetupException;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.base.FragmentRoot;
import org.apache.drill.exec.physical.impl.ImplCreator;
import org.apache.drill.exec.physical.impl.RootExec;
import org.apache.drill.exec.proto.ExecProtos.BitHandshake;
import org.apache.drill.exec.proto.ExecProtos.BitStatus;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.ExecProtos.FragmentRecordBatch;
import org.apache.drill.exec.proto.ExecProtos.FragmentStatus;
import org.apache.drill.exec.proto.ExecProtos.PlanFragment;
import org.apache.drill.exec.proto.ExecProtos.RpcType;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.record.RawFragmentBatch;
import org.apache.drill.exec.rpc.Acks;
import org.apache.drill.exec.rpc.RemoteConnection;
import org.apache.drill.exec.rpc.Response;
import org.apache.drill.exec.rpc.RpcConstants;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.bit.BitConnection;
import org.apache.drill.exec.rpc.bit.BitRpcConfig;
import org.apache.drill.exec.rpc.bit.BitTunnel;
import org.apache.drill.exec.work.FragmentRunner;
import org.apache.drill.exec.work.RemotingFragmentRunnerListener;
import org.apache.drill.exec.work.WorkManager.WorkerBee;
import org.apache.drill.exec.work.fragment.IncomingFragmentHandler;
import org.apache.drill.exec.work.fragment.RemoteFragmentHandler;

import com.google.common.collect.Maps;
import com.google.protobuf.MessageLite;

public class BitComHandlerImpl implements BitComHandler {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BitComHandlerImpl.class);
  
  private ConcurrentMap<FragmentHandle, IncomingFragmentHandler> handlers = Maps.newConcurrentMap();
  private final WorkerBee bee;
  
  public BitComHandlerImpl(WorkerBee bee) {
    super();
    this.bee = bee;
  }

  /* (non-Javadoc)
   * @see org.apache.drill.exec.work.batch.BitComHandler#handle(org.apache.drill.exec.rpc.bit.BitConnection, int, io.netty.buffer.ByteBuf, io.netty.buffer.ByteBuf)
   */
  @Override
  public Response handle(BitConnection connection, int rpcType, ByteBuf pBody, ByteBuf dBody) throws RpcException {
    if(RpcConstants.EXTRA_DEBUGGING) logger.debug("Received bit com message of type {}", rpcType);

    switch (rpcType) {
    
    case RpcType.REQ_CANCEL_FRAGMENT_VALUE:
      FragmentHandle handle = get(pBody, FragmentHandle.PARSER);
      cancelFragment(handle);
      return BitRpcConfig.OK;

    case RpcType.REQ_FRAGMENT_STATUS_VALUE:
      connection.getListenerPool().status( get(pBody, FragmentStatus.PARSER));
      // TODO: Support a type of message that has no response.
      return BitRpcConfig.OK;

    case RpcType.REQ_INIATILIZE_FRAGMENT_VALUE:
      PlanFragment fragment = get(pBody, PlanFragment.PARSER);
      startNewRemoteFragment(fragment);
      return BitRpcConfig.OK;
      
    case RpcType.REQ_RECORD_BATCH_VALUE:
      try {
        FragmentRecordBatch header = get(pBody, FragmentRecordBatch.PARSER);
        incomingRecordBatch(connection, header, dBody);
        return BitRpcConfig.OK;
      } catch (FragmentSetupException e) {
        throw new RpcException("Failure receiving record batch.", e);
      }

    default:
      throw new RpcException("Not yet supported.");
    }

  }
  
  
  
  /* (non-Javadoc)
   * @see org.apache.drill.exec.work.batch.BitComHandler#startNewRemoteFragment(org.apache.drill.exec.proto.ExecProtos.PlanFragment)
   */
  @Override
  public void startNewRemoteFragment(PlanFragment fragment){
    logger.debug("Received remote fragment start instruction", fragment);
    FragmentContext context = new FragmentContext(bee.getContext(), fragment.getHandle(), null, null,new FunctionImplementationRegistry(bee.getContext().getConfig()));
    BitTunnel tunnel = bee.getContext().getBitCom().getTunnel(fragment.getForeman());
    RemotingFragmentRunnerListener listener = new RemotingFragmentRunnerListener(context, tunnel);
    try{
      FragmentRoot rootOperator = bee.getContext().getPlanReader().readFragmentOperator(fragment.getFragmentJson());
      RootExec exec = ImplCreator.getExec(context, rootOperator);
      FragmentRunner fr = new FragmentRunner(context, exec, listener);
      bee.addFragmentRunner(fr);

    }catch(IOException e){
      listener.fail(fragment.getHandle(), "Failure while parsing fragment execution plan.", e);
    }catch(ExecutionSetupException e){
      listener.fail(fragment.getHandle(), "Failure while setting up execution plan.", e);
    }
    
  }
  
  /* (non-Javadoc)
   * @see org.apache.drill.exec.work.batch.BitComHandler#cancelFragment(org.apache.drill.exec.proto.ExecProtos.FragmentHandle)
   */
  @Override
  public Ack cancelFragment(FragmentHandle handle){
    IncomingFragmentHandler handler = handlers.get(handle);
    if(handler != null){
      // try remote fragment cancel.
      handler.cancel();
    }else{
      // then try local cancel.
      FragmentRunner runner = bee.getFragmentRunner(handle);
      if(runner != null) runner.cancel();
    }
    
    return Acks.OK;
  }
  
  
  /**
   * Returns a positive Ack if this fragment is accepted.  
   */
  private Ack incomingRecordBatch(RemoteConnection connection, FragmentRecordBatch fragmentBatch, ByteBuf body) throws FragmentSetupException{
    FragmentHandle handle = fragmentBatch.getHandle();
    IncomingFragmentHandler handler = handlers.get(handle);

    // Create a handler if there isn't already one.
    if(handler == null){
      
      PlanFragment fragment = bee.getContext().getCache().getFragment(handle);
      if(fragment == null){
        logger.error("Received batch where fragment was not in cache.");
        return Acks.FAIL;
      }

      IncomingFragmentHandler newHandler = new RemoteFragmentHandler(fragment, bee.getContext(), bee.getContext().getBitCom().getTunnel(fragment.getForeman()));
      
      // since their could be a race condition on the check, we'll use putIfAbsent so we don't have two competing handlers.
      handler = handlers.putIfAbsent(fragment.getHandle(), newHandler);
          
      if(handler == null){
        // we added a handler, inform the bee that we did so.  This way, the foreman can track status. 
        bee.addFragmentPendingRemote(newHandler);
        handler = newHandler;
      }
    }
    
    boolean canRun = handler.handle(connection.getConnectionThrottle(), new RawFragmentBatch(fragmentBatch, body));
    if(canRun){
      logger.debug("Arriving batch means local batch can run, starting local batch.");
      // if we've reached the canRun threshold, we'll proceed.  This expects handler.handle() to only return a single true.
      bee.startFragmentPendingRemote(handler);
    }
    if(fragmentBatch.getIsLastBatch() && !handler.isWaiting()){
      logger.debug("Removing handler.  Is Last Batch {}.  Is Waiting for more {}", fragmentBatch.getIsLastBatch(), handler.isWaiting());
      handlers.remove(handler.getHandle());
    }
    
    return Acks.OK;
  }
  
  /* (non-Javadoc)
   * @see org.apache.drill.exec.work.batch.BitComHandler#registerIncomingFragmentHandler(org.apache.drill.exec.work.fragment.IncomingFragmentHandler)
   */
  @Override
  public void registerIncomingFragmentHandler(IncomingFragmentHandler handler){
    IncomingFragmentHandler old = handlers.putIfAbsent(handler.getHandle(), handler);
    assert old == null : "You can only register a fragment handler if one hasn't been registered already.";
  }
  
  
}
