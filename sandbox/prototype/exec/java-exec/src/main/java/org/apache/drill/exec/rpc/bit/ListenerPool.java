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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.ExecProtos.FragmentStatus;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.work.foreman.FragmentStatusListener;
import org.apache.drill.exec.work.fragment.RemoteFragmentHandler;

public class ListenerPool {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ListenerPool.class);
  
  private final ConcurrentMap<FragmentHandle, FragmentStatusListener> listeners;
  
  public ListenerPool(int par){
    listeners = new ConcurrentHashMap<FragmentHandle, FragmentStatusListener>(16, 0.75f, par);
  }
  
  public void removeFragmentStatusListener(FragmentHandle handle) throws RpcException{
    listeners.remove(handle);
  }
  
  public void addFragmentStatusListener(FragmentHandle handle, FragmentStatusListener listener) throws RpcException{
    FragmentStatusListener old = listeners.putIfAbsent(handle, listener);
    if(old != null) throw new RpcException("Failure.  The provided handle already exists in the listener pool.  You need to remove one listener before adding another.");
  }
  
  public void status(FragmentStatus status){
    FragmentStatusListener l = listeners.get(status.getHandle());
    if(l == null){
      logger.info("A fragment message arrived but there was no registered listener for that message.");
      return;
    }else{
      l.statusUpdate(status);
    }
  }
}
