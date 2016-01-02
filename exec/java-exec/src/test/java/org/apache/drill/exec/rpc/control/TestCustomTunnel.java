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
package org.apache.drill.exec.rpc.control;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.DrillBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.util.internal.ThreadLocalRandom;

import java.util.Arrays;
import java.util.Random;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.UserRpcException;
import org.apache.drill.exec.rpc.control.ControlTunnel.CustomFuture;
import org.apache.drill.exec.rpc.control.ControlTunnel.CustomTunnel;
import org.apache.drill.exec.rpc.control.Controller.CustomMessageHandler;
import org.apache.drill.exec.rpc.control.Controller.CustomResponse;
import org.apache.drill.exec.server.DrillbitContext;
import org.junit.Test;

import com.google.protobuf.InvalidProtocolBufferException;

public class TestCustomTunnel extends BaseTestQuery {

  private final QueryId expectedId = QueryId
      .newBuilder()
      .setPart1(ThreadLocalRandom.current().nextLong())
      .setPart2(ThreadLocalRandom.current().nextLong())
      .build();

  private final ByteBuf buf1;
  private final byte[] expected;

  public TestCustomTunnel() {
    buf1 = UnpooledByteBufAllocator.DEFAULT.buffer(1024);
    Random r = new Random();
    this.expected = new byte[1024];
    r.nextBytes(expected);
    buf1.writeBytes(expected);
  }

  @Test
  public void ensureRoundTrip() throws RpcException, InvalidProtocolBufferException {

    final DrillbitContext context = getDrillbitContext();
    final TestCustomMessageHandler handler = new TestCustomMessageHandler(context.getEndpoint(), false);
    context.getController().registerCustomHandler(1001, handler, DrillbitEndpoint.PARSER);
    final ControlTunnel loopbackTunnel = context.getController().getTunnel(context.getEndpoint());
    final CustomTunnel<DrillbitEndpoint, QueryId> tunnel = loopbackTunnel.getCustomTunnel(1001, DrillbitEndpoint.class,
        QueryId.PARSER);
    CustomFuture<QueryId> future = tunnel.send(context.getEndpoint());
    assertEquals(expectedId, future.get());
  }

  @Test
  public void ensureRoundTripBytes() throws RpcException, InvalidProtocolBufferException {
    final DrillbitContext context = getDrillbitContext();
    final TestCustomMessageHandler handler = new TestCustomMessageHandler(context.getEndpoint(), true);
    context.getController().registerCustomHandler(1002, handler, DrillbitEndpoint.PARSER);
    final ControlTunnel loopbackTunnel = context.getController().getTunnel(context.getEndpoint());
    final CustomTunnel<DrillbitEndpoint, QueryId> tunnel = loopbackTunnel.getCustomTunnel(1002, DrillbitEndpoint.class,
        QueryId.PARSER);
    buf1.retain();
    CustomFuture<QueryId> future = tunnel.send(context.getEndpoint(), buf1);
    assertEquals(expectedId, future.get());
    byte[] actual = new byte[1024];
    future.getBuffer().getBytes(0, actual);
    future.getBuffer().release();
    assertTrue(Arrays.equals(expected, actual));
  }

  private class TestCustomMessageHandler implements CustomMessageHandler<DrillbitEndpoint, QueryId> {
    private DrillbitEndpoint expectedValue;
    private final boolean returnBytes;

    public TestCustomMessageHandler(DrillbitEndpoint expectedValue, boolean returnBytes) {
      super();
      this.expectedValue = expectedValue;
      this.returnBytes = returnBytes;
    }

    @Override
    public CustomResponse<QueryId> onMessage(DrillbitEndpoint pBody, DrillBuf dBody) throws UserRpcException {

      if (!expectedValue.equals(pBody)) {
        throw new UserRpcException(expectedValue, "Invalid expected downstream value.", new IllegalStateException());
      }

      if (returnBytes) {
        byte[] actual = new byte[1024];
        dBody.getBytes(0, actual);
        if (!Arrays.equals(expected, actual)) {
          throw new UserRpcException(expectedValue, "Invalid expected downstream value.", new IllegalStateException());
        }
      }

      return new CustomResponse<QueryId>() {

        @Override
        public QueryId getMessage() {
          return expectedId;
        }

        @Override
        public ByteBuf[] getBodies() {
          if (returnBytes) {
            buf1.retain();
            return new ByteBuf[] { buf1 };
          } else {
            return null;
          }
        }

      };
    }
  }
}
