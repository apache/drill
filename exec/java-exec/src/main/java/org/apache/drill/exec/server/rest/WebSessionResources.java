/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.drill.exec.server.rest;

import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelPromise;
import org.apache.drill.common.AutoCloseables;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.rpc.ChannelClosedException;
import org.apache.drill.exec.rpc.user.UserSession;

import java.net.SocketAddress;

/**
 * Class holding all the resources required for Web User Session. This class is responsible for the proper cleanup of
 * all the resources.
 */
public class WebSessionResources implements AutoCloseable {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(WebSessionResources.class);

  private BufferAllocator allocator;

  private final SocketAddress remoteAddress;

  private UserSession webUserSession;

  private ChannelPromise closeFuture;

  WebSessionResources(BufferAllocator allocator, SocketAddress remoteAddress, UserSession userSession) {
    this.allocator = allocator;
    this.remoteAddress = remoteAddress;
    this.webUserSession = userSession;
    closeFuture = new DefaultChannelPromise(null);
  }

  public UserSession getSession() {
    return webUserSession;
  }

  public BufferAllocator getAllocator() {
    return allocator;
  }

  public ChannelPromise getCloseFuture() {
    return closeFuture;
  }

  public SocketAddress getRemoteAddress() {
    return remoteAddress;
  }

  @Override
  public void close() {

    try {
      AutoCloseables.close(webUserSession, allocator);
    } catch (Exception ex) {
      logger.error("Failure while closing the session resources", ex);
    }

    // Set the close future associated with this session.
    if (closeFuture != null) {
      closeFuture.setFailure(new ChannelClosedException("Http Session of the user is closed."));
      closeFuture = null;
    }
  }
}
