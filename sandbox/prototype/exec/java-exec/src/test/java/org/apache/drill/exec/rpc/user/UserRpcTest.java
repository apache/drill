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
package org.apache.drill.exec.rpc.user;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.nio.NioEventLoopGroup;

import java.util.concurrent.TimeUnit;

import org.apache.drill.exec.proto.UserProtos.QueryHandle;
import org.apache.drill.exec.proto.UserProtos.QueryResultsMode;
import org.apache.drill.exec.proto.UserProtos.RunQuery;
import org.apache.drill.exec.rpc.DrillRpcFuture;
import org.apache.drill.exec.rpc.NamedThreadFactory;
import org.junit.Test;

public class UserRpcTest {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UserRpcTest.class);
  
  
  
  
  @Test
  public void doBasicRpcTest() throws Exception {
    final int bufferSize = 25000;
    final int batchSize = 1000;
    final int batchCount = 100;

    
    int sends = 0;
    int receives = 0;
    long nanoSend = 0;
    long nanoReceive = 0;

    
    try {
      ByteBufAllocator bb = new PooledByteBufAllocator(true);
//      ByteBufAllocator bb = UnpooledByteBufAllocator.DEFAULT;
      UserServer s = new UserServer(bb, new NioEventLoopGroup(1, new NamedThreadFactory("Server-")), null);
      s.bind(31515);

      logger.debug("Starting user client.");
      UserClient c = new UserClient(bb, new NioEventLoopGroup(1, new NamedThreadFactory("Client-")));

      logger.debug("Connecting as client to server.");
      c.connectAsClient("localhost", 31515);

      
      @SuppressWarnings("unchecked")
      DrillRpcFuture<QueryHandle>[] handles = new DrillRpcFuture[batchSize];

      for (int x = 0; x < batchCount; x++) {
        long s1 = System.nanoTime();
        for (int i = 0; i < batchSize; i++) {
          sends++;
          ByteBuf rawBody = bb.buffer(bufferSize);
          rawBody.writerIndex(bufferSize);
          if(rawBody.readableBytes() != bufferSize) throw new RuntimeException();
          handles[i] = c.submitQuery(RunQuery.newBuilder().setMode(QueryResultsMode.QUERY_FOR_STATUS).build(), rawBody);
        }
        
        long s2 = System.nanoTime();

        for (int i = 0; i < batchSize; i++) {
          handles[i].checkedGet(2, TimeUnit.SECONDS).getQueryId();
          receives++;
        }

        long s3 = System.nanoTime();
        nanoSend += (s2-s1);
        nanoReceive += (s3-s2);
        logger.debug("Submission time {}ms, return time {}ms", (s2 - s1) / 1000 / 1000, (s3 - s2) / 1000 / 1000);
      }
      // logger.debug("Submitting query.");
      // DrillRpcFuture<QueryHandle> handleFuture =
      // c.submitQuery(RunQuery.newBuilder().setMode(QueryResultsMode.QUERY_FOR_STATUS).build());
      //
      // logger.debug("Got query id handle of {}", handleFuture.get(2, TimeUnit.SECONDS).getQueryId());
    } catch (Exception e) {
      logger.error("Exception of type {} occurred while doing test.", e.getClass().getCanonicalName());
      throw e;
    } finally{
      long mbsTransferred = (1l * bufferSize * batchSize * batchCount)/1024/1024;
      double sSend = nanoSend*1.0d/1000/1000/1000;
      double sReceive = nanoReceive*1.0d/1000/1000/1000;
      logger.info(String.format("Completed %d sends and %d receives.  Total data transferred was %d.  Send bw: %f, Receive bw: %f.", sends, receives, mbsTransferred, mbsTransferred*1.0/sSend, mbsTransferred*1.0/sReceive));
      logger.info("Completed {} sends and {} receives.", sends, receives);
    }
  }
}
