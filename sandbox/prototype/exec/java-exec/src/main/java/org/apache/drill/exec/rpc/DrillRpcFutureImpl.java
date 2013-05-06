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
package org.apache.drill.exec.rpc;

import java.util.concurrent.ExecutionException;

import com.google.common.util.concurrent.AbstractCheckedFuture;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

class DrillRpcFutureImpl<V> extends AbstractCheckedFuture<V, RpcException> implements DrillRpcFuture<V>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillRpcFutureImpl.class);

  final int coordinationId;
  private final Class<V> clazz;

  public DrillRpcFutureImpl(ListenableFuture<V> delegate, int coordinationId, Class<V> clazz) {
    super(delegate);
    this.coordinationId = coordinationId;
    this.clazz = clazz;
  }

  public Class<V> getOutcomeClass(){
    return clazz;
  }
  
  /**
   * Drill doesn't currently support rpc cancellations since nearly all requests should be either instance of
   * asynchronous. Business level cancellation is managed a separate call (e.g. canceling a query.). Calling this method
   * will result in an UnsupportedOperationException.
   */
  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    throw new UnsupportedOperationException(
        "Drill doesn't currently support rpc cancellations. See javadocs for more detail.");
  }

  @Override
  protected RpcException mapException(Exception ex) {
    Throwable e = ex;
    while(e instanceof ExecutionException){
      e = e.getCause();
    }
    if (e instanceof RpcException)  return (RpcException) e;

    return new RpcException(ex);

  }

  @SuppressWarnings("unchecked")
  void setValue(Object value) {
    assert clazz.isAssignableFrom(value.getClass());
    ((InnerFuture<V>) super.delegate()).setValue((V) value);
  }

  boolean setException(Throwable t) {
    return ((InnerFuture<V>) super.delegate()).setException(t);
  }

  public static class InnerFuture<T> extends AbstractFuture<T> {
    // we rewrite these so that the parent can see them

    void setValue(T value) {
      super.set(value);
    }

    protected boolean setException(Throwable t) {
      return super.setException(t);
    }
  }

  public class RpcOutcomeListenerWrapper implements Runnable{
    final RpcOutcomeListener<V> inner;
    
    public RpcOutcomeListenerWrapper(RpcOutcomeListener<V> inner) {
      super();
      this.inner = inner;
    }

    @Override
    public void run() {
      try{
        inner.success(DrillRpcFutureImpl.this.checkedGet());
      }catch(RpcException e){
        inner.failed(e);
      }
    }
  }
  
  public void addLightListener(RpcOutcomeListener<V> outcomeListener){
    this.addListener(new RpcOutcomeListenerWrapper(outcomeListener), MoreExecutors.sameThreadExecutor());
  }
  
  
  
  public static <V> DrillRpcFutureImpl<V> getNewFuture(int coordinationId, Class<V> clazz) {
    InnerFuture<V> f = new InnerFuture<V>();
    return new DrillRpcFutureImpl<V>(f, coordinationId, clazz);
  }


}