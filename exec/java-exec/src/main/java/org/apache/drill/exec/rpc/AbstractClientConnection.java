/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.rpc;

import io.netty.channel.socket.SocketChannel;
import org.slf4j.Logger;

import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

import static com.google.common.base.Preconditions.checkState;

public abstract class AbstractClientConnection extends AbstractRemoteConnection implements ClientConnection {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractClientConnection.class);

  private SaslClient saslClient;

  public AbstractClientConnection(SocketChannel channel, String name) {
    super(channel, name);
  }

  protected abstract Logger getLogger();

  @Override
  public void setSaslClient(final SaslClient saslClient) {
    checkState(this.saslClient == null);
    this.saslClient = saslClient;
  }

  @Override
  public SaslClient getSaslClient() {
    checkState(this.saslClient != null);
    return saslClient;
  }

  @Override
  public void close() {
    try {
      if (saslClient != null) {
        saslClient.dispose();
        saslClient = null;
      }
    } catch (final SaslException e) {
      getLogger().warn("Unclean disposal", e);
    }
    super.close();
  }
}
