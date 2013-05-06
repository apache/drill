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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.ExecProtos.FragmentStatus;
import org.apache.drill.exec.proto.ExecProtos.PlanFragment;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.record.FragmentWritableBatch;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.rpc.DrillRpcFuture;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.RpcOutcomeListener;

import com.google.common.util.concurrent.AbstractCheckedFuture;
import com.google.common.util.concurrent.CheckedFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;

/**
 * Interface provided for communication between two bits. Underlying connection may be server or client based. Resilient
 * to connection loss. Right now, this has to jump through some hoops and bridge futures between the connection creation
 * and action. A better approach should be done.
 */
public class BitTunnel {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BitTunnel.class);

  private static final int MAX_ATTEMPTS = 3;

  private final BitConnectionManager manager;
  private final Executor exec;
  

  public BitTunnel(Executor exec, DrillbitEndpoint endpoint, BitComImpl com, BitConnection connection) {
    this.manager = new BitConnectionManager(endpoint, com, connection, null, MAX_ATTEMPTS);
    this.exec = exec;
  }

  public BitTunnel(Executor exec, DrillbitEndpoint endpoint, BitComImpl com,
      CheckedFuture<BitConnection, RpcException> future) {
    this.manager = new BitConnectionManager(endpoint, com, (BitConnection) null, future, MAX_ATTEMPTS);
    this.exec = exec;
  }
  
  public DrillbitEndpoint getEndpoint(){
    return manager.getEndpoint();
  }

  private <T> DrillRpcFuture<T> submit(BitCommand<T> command) {
    exec.execute(command);
    return command;
  }

  public DrillRpcFuture<Ack> sendRecordBatch(FragmentContext context, FragmentWritableBatch batch) {
    return submit(new SendBatch(batch, context));
  }

  public DrillRpcFuture<Ack> sendFragment(PlanFragment fragment) {
    return submit(new SendFragment(fragment));
  }

  public DrillRpcFuture<Ack> cancelFragment(FragmentHandle handle) {
    return submit(new CancelFragment(handle));
  }

  public DrillRpcFuture<Ack> sendFragmentStatus(FragmentStatus status){
    return submit(new SendFragmentStatus(status));
  }

  public class SendBatch extends BitCommand<Ack> {
    final FragmentWritableBatch batch;
    final FragmentContext context;

    public SendBatch(FragmentWritableBatch batch, FragmentContext context) {
      super();
      this.batch = batch;
      this.context = context;
    }

    @Override
    public CheckedFuture<Ack, RpcException> doRpcCall(BitConnection connection) {
      logger.debug("Sending record batch. {}", batch);
      return connection.sendRecordBatch(context, batch);
    }

  }

  public class SendFragmentStatus extends BitCommand<Ack> {
    final FragmentStatus status;

    public SendFragmentStatus(FragmentStatus status) {
      super();
      this.status = status;
    }

    @Override
    public CheckedFuture<Ack, RpcException> doRpcCall(BitConnection connection) {
      return connection.sendFragmentStatus(status);
    }
  }

  public class CancelFragment extends BitCommand<Ack> {
    final FragmentHandle handle;

    public CancelFragment(FragmentHandle handle) {
      super();
      this.handle = handle;
    }

    @Override
    public CheckedFuture<Ack, RpcException> doRpcCall(BitConnection connection) {
      return connection.cancelFragment(handle);
    }

  }

  public class SendFragment extends BitCommand<Ack> {
    final PlanFragment fragment;

    public SendFragment(PlanFragment fragment) {
      super();
      this.fragment = fragment;
    }

    @Override
    public CheckedFuture<Ack, RpcException> doRpcCall(BitConnection connection) {
      return connection.sendFragment(fragment);
    }

  }


  

  private abstract class BitCommand<T> extends AbstractCheckedFuture<T, RpcException> implements Runnable, DrillRpcFuture<T> {

    public void addLightListener(RpcOutcomeListener<T> outcomeListener){
      this.addListener(new RpcOutcomeListenerWrapper(outcomeListener), MoreExecutors.sameThreadExecutor());
    }

    public BitCommand() {
      super(SettableFuture.<T> create());
    }

    public abstract CheckedFuture<T, RpcException> doRpcCall(BitConnection connection);

    public final void run() {
      
      try {
        
        BitConnection connection = manager.getConnection(0);
        assert connection != null : "The connection manager should never return a null connection.  Worse case, it should throw an exception.";
        CheckedFuture<T, RpcException> rpc = doRpcCall(connection);
        rpc.addListener(new FutureBridge<T>((SettableFuture<T>) delegate(), rpc), MoreExecutors.sameThreadExecutor());
      } catch (RpcException ex) {
        ((SettableFuture<T>) delegate()).setException(ex);
      }

    }

    @Override
    protected RpcException mapException(Exception e) {
      Throwable t = e;
      if (e instanceof ExecutionException) {
        t = e.getCause();
      }
      if (t instanceof RpcException) return (RpcException) t;
      return new RpcException(t);
    }

    public class RpcOutcomeListenerWrapper implements Runnable{
      final RpcOutcomeListener<T> inner;
      
      public RpcOutcomeListenerWrapper(RpcOutcomeListener<T> inner) {
        this.inner = inner;
      }

      @Override
      public void run() {
        try{
          inner.success(BitCommand.this.checkedGet());
        }catch(RpcException e){
          inner.failed(e);
        }
      }
    }

    @Override
    public String toString() {
      return "BitCommand ["+this.getClass().getSimpleName()+"]";
    }
    
    
    
  }

  private class FutureBridge<T> implements Runnable {
    final SettableFuture<T> out;
    final CheckedFuture<T, RpcException> in;

    public FutureBridge(SettableFuture<T> out, CheckedFuture<T, RpcException> in) {
      super();
      this.out = out;
      this.in = in;
    }

    @Override
    public void run() {
      try {
        out.set(in.checkedGet());
      } catch (RpcException ex) {
        out.setException(ex);
      }
    }

  }

}
