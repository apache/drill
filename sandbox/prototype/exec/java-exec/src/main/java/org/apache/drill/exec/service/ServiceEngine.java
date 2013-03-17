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
package org.apache.drill.exec.service;

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.nio.NioEventLoopGroup;

import java.io.Closeable;
import java.io.IOException;

import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.DrillbitStartupException;
import org.apache.drill.exec.rpc.NamedThreadFactory;
import org.apache.drill.exec.rpc.bit.BitCom;
import org.apache.drill.exec.rpc.bit.BitComImpl;
import org.apache.drill.exec.rpc.user.UserServer;
import org.apache.drill.exec.server.DrillbitContext;

import com.google.common.io.Closeables;

public class ServiceEngine implements Closeable{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ServiceEngine.class);
  
  UserServer userServer;
  BitComImpl bitCom;
  int userPort;
  int bitPort;
  DrillbitContext context;
  
  public ServiceEngine(DrillbitContext context){
    ByteBufAllocator allocator = context.getAllocator().getUnderlyingAllocator();
    userServer = new UserServer(allocator, new NioEventLoopGroup(1, new NamedThreadFactory("UserServer-")), context);
    bitCom = new BitComImpl(context);
  }
  
  public void start() throws DrillbitStartupException, InterruptedException{
    userPort = userServer.bind(context.getConfig().getInt(ExecConstants.INITIAL_USER_PORT));
    bitPort = bitCom.start();
  }
  
  public int getBitPort(){
    return bitPort;
  }
  
  public int getUserPort(){
    return userPort;
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
