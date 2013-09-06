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
package org.apache.drill.exec.service;

import io.netty.channel.nio.NioEventLoopGroup;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.DrillbitStartupException;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.rpc.NamedThreadFactory;
import org.apache.drill.exec.rpc.bit.BitCom;
import org.apache.drill.exec.rpc.bit.BitComImpl;
import org.apache.drill.exec.rpc.user.UserServer;
import org.apache.drill.exec.server.BootStrapContext;
import org.apache.drill.exec.work.batch.BitComHandler;
import org.apache.drill.exec.work.user.UserWorker;

import com.google.common.io.Closeables;

public class ServiceEngine implements Closeable{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ServiceEngine.class);
  
  private final UserServer userServer;
  private final BitCom bitCom;
  private final DrillConfig config;
  boolean useIP = false;
  
  public ServiceEngine(BitComHandler bitComWorker, UserWorker userWorker, BootStrapContext context){
    this.userServer = new UserServer(context.getAllocator().getUnderlyingAllocator(), new NioEventLoopGroup(context.getConfig().getInt(ExecConstants.USER_SERVER_RPC_THREADS),
            new NamedThreadFactory("UserServer-")), userWorker);
    this.bitCom = new BitComImpl(context, bitComWorker);
    this.config = context.getConfig();
  }
  
  public DrillbitEndpoint start() throws DrillbitStartupException, InterruptedException, UnknownHostException{
    int userPort = userServer.bind(config.getInt(ExecConstants.INITIAL_USER_PORT));
    String address = useIP ?  InetAddress.getLocalHost().getHostAddress() : InetAddress.getLocalHost().getCanonicalHostName();
    DrillbitEndpoint partialEndpoint = DrillbitEndpoint.newBuilder()
        .setAddress(address)
        //.setAddress("localhost")
        .setUserPort(userPort)
        .build();

    return bitCom.start(partialEndpoint);
  }

  public BitCom getBitCom(){
    return bitCom;
  }
  
  @Override
  public void close() throws IOException {
    Closeables.closeQuietly(userServer);
    Closeables.closeQuietly(bitCom);
  }
}
