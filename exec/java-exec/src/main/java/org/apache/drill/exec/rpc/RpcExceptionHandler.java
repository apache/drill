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

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;

public class RpcExceptionHandler implements ChannelHandler{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RpcExceptionHandler.class);

  public RpcExceptionHandler(){
  }


  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {

    if(!ctx.channel().isOpen() || cause.getMessage().equals("Connection reset by peer")){
      logger.warn("Exception with closed channel", cause);
      return;
    }else{
      logger.error("Exception in pipeline.  Closing channel between local " + ctx.channel().localAddress() + " and remote " + ctx.channel().remoteAddress(), cause);
      ctx.close();
    }
  }


  @Override
  public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
  }


  @Override
  public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
  }

}
