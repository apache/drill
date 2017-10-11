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
package org.apache.drill.exec.server.rest;

import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelPromise;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.rpc.TransportCheck;
import org.apache.drill.exec.rpc.user.UserSession;
import org.junit.Test;

import java.net.SocketAddress;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class WebSessionResourcesTest {
  //private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(WebSessionResourcesTest.class);

  private WebSessionResources webSessionResources;

  private boolean listenerComplete;

  private CountDownLatch latch;

  private EventExecutor executor;


  private class TestClosedListener implements GenericFutureListener<Future<Void>> {
    @Override
    public void operationComplete(Future<Void> future) throws Exception {
      listenerComplete = true;
      latch.countDown();
    }
  }

  @Test
  public void testChannelPromiseWithNullExecutor() throws Exception {
    try {
      ChannelPromise closeFuture = new DefaultChannelPromise(null);
      webSessionResources = new WebSessionResources(mock(BufferAllocator.class), mock(SocketAddress.class), mock
          (UserSession.class), closeFuture);
      webSessionResources.close();
      fail();
    } catch (Exception e) {
      assertTrue(e instanceof NullPointerException);
      verify(webSessionResources.getAllocator()).close();
      verify(webSessionResources.getSession()).close();
    }
  }

  @Test
  public void testChannelPromiseWithValidExecutor() throws Exception {
    try {
      EventExecutor mockExecutor = mock(EventExecutor.class);
      ChannelPromise closeFuture = new DefaultChannelPromise(null, mockExecutor);
      webSessionResources = new WebSessionResources(mock(BufferAllocator.class), mock(SocketAddress.class), mock
          (UserSession.class), closeFuture);
      webSessionResources.close();
      verify(webSessionResources.getAllocator()).close();
      verify(webSessionResources.getSession()).close();
      verify(mockExecutor).inEventLoop();
      verify(mockExecutor).execute(any(Runnable.class));
      assertTrue(webSessionResources.getCloseFuture() == null);
      assertTrue(!listenerComplete);
    } catch (Exception e) {
      fail();
    }
  }

  @Test
  public void testDoubleClose() throws Exception {
    try {
      ChannelPromise closeFuture = new DefaultChannelPromise(null, mock(EventExecutor.class));
      webSessionResources = new WebSessionResources(mock(BufferAllocator.class), mock(SocketAddress.class), mock
          (UserSession.class), closeFuture);
      webSessionResources.close();

      verify(webSessionResources.getAllocator()).close();
      verify(webSessionResources.getSession()).close();
      assertTrue(webSessionResources.getCloseFuture() == null);

      webSessionResources.close();
    } catch (Exception e) {
      fail();
    }
  }

  @Test
  public void testCloseWithListener() throws Exception {
    try {
      // Assign latch, executor and closeListener for this test case
      GenericFutureListener<Future<Void>> closeListener = new TestClosedListener();
      latch = new CountDownLatch(1);
      executor = TransportCheck.createEventLoopGroup(1, "Test-Thread").next();
      ChannelPromise closeFuture = new DefaultChannelPromise(null, executor);

      // create WebSessionResources with above ChannelPromise to notify listener
      webSessionResources = new WebSessionResources(mock(BufferAllocator.class), mock(SocketAddress.class),
          mock(UserSession.class), closeFuture);

      // Add the Test Listener to close future
      assertTrue(!listenerComplete);
      closeFuture.addListener(closeListener);

      // Close the WebSessionResources
      webSessionResources.close();

      // Verify the states
      verify(webSessionResources.getAllocator()).close();
      verify(webSessionResources.getSession()).close();
      assertTrue(webSessionResources.getCloseFuture() == null);

      // Since listener will be invoked so test should not wait forever
      latch.await();
      assertTrue(listenerComplete);
    } catch (Exception e) {
      fail();
    } finally {
      listenerComplete = false;
      executor.shutdownGracefully();
    }
  }
}