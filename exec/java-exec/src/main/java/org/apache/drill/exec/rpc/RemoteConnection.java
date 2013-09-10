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
package org.apache.drill.exec.rpc;

import io.netty.channel.Channel;

import java.util.concurrent.Semaphore;

public class RemoteConnection{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RemoteConnection.class);
  private final Channel channel;
  
  final Semaphore throttle;
  
  public void acquirePermit() throws InterruptedException{
    if(RpcConstants.EXTRA_DEBUGGING) logger.debug("Acquiring send permit.");
    this.throttle.acquire();
    if(RpcConstants.EXTRA_DEBUGGING) logger.debug("Send permit acquired.");
  }
  
  public void releasePermit() {
    throttle.release();
  }
  
  public RemoteConnection(Channel channel, int maxOutstanding) {
    super();
    this.channel = channel;
    this.throttle  = new Semaphore(maxOutstanding);
  }
  
  public RemoteConnection(Channel channel) {
    this(channel, 100);
  }


  public final Channel getChannel() {
    return channel;
  }


  public ConnectionThrottle getConnectionThrottle(){
    // can't be implemented until we switch to per query sockets.
    return null;
  }
  
  public interface ConnectionThrottle{
    public void disableReceiving();
    public void enableReceiving();
  }
}