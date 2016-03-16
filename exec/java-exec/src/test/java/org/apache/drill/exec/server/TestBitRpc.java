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
package org.apache.drill.exec.server;

import static org.junit.Assert.assertTrue;
import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import mockit.NonStrictExpectations;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.scanner.ClassPathScanner;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.ExecTest;
import org.apache.drill.exec.exception.FragmentSetupException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.record.FragmentWritableBatch;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RawFragmentBatch;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.RpcOutcomeListener;
import org.apache.drill.exec.rpc.control.WorkEventBus;
import org.apache.drill.exec.rpc.data.DataConnectionManager;
import org.apache.drill.exec.rpc.data.DataServer;
import org.apache.drill.exec.rpc.data.DataTunnel;
import org.apache.drill.exec.rpc.data.IncomingDataBatch;
import org.apache.drill.exec.vector.Float8Vector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.work.WorkManager.WorkerBee;
import org.apache.drill.exec.work.fragment.FragmentManager;
import org.junit.Test;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;

public class TestBitRpc extends ExecTest {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestBitRpc.class);

  @Test
  public void testConnectionBackpressure(@Injectable WorkerBee bee, @Injectable final WorkEventBus workBus) throws Exception {

    DrillConfig config1 = DrillConfig.create();
    final BootStrapContext c = new BootStrapContext(config1, ClassPathScanner.fromPrescan(config1));
    DrillConfig config2 = DrillConfig.create();
    BootStrapContext c2 = new BootStrapContext(config2, ClassPathScanner.fromPrescan(config2));

    final FragmentContext fcon = new MockUp<FragmentContext>(){
      BufferAllocator getAllocator(){
        return c.getAllocator();
      }
    }.getMockInstance();

    final FragmentManager fman = new MockUp<FragmentManager>(){
      int v = 0;

      @Mock
      boolean handle(IncomingDataBatch batch) throws FragmentSetupException, IOException {
        try {
          v++;
          if (v % 10 == 0) {
            System.out.println("sleeping.");
            Thread.sleep(3000);
          }
        } catch (InterruptedException e) {

        }
        RawFragmentBatch rfb = batch.newRawFragmentBatch(c.getAllocator());
        rfb.sendOk();
        rfb.release();

        return true;
      }

      public FragmentContext getFragmentContext(){
        return fcon;
      }

    }.getMockInstance();


    new NonStrictExpectations() {{
      workBus.getFragmentManagerIfExists((FragmentHandle) any); result = fman;
      workBus.getFragmentManager( (FragmentHandle) any); result = fman;
    }};

    int port = 1234;

    DataServer server = new DataServer(c, c.getAllocator(), workBus, null);

    port = server.bind(port, true);
    DrillbitEndpoint ep = DrillbitEndpoint.newBuilder().setAddress("localhost").setDataPort(port).build();
    DataConnectionManager manager = new DataConnectionManager(ep, c2);
    DataTunnel tunnel = new DataTunnel(manager);
    AtomicLong max = new AtomicLong(0);
    for (int i = 0; i < 40; i++) {
      long t1 = System.currentTimeMillis();
      tunnel.sendRecordBatch(new TimingOutcome(max), new FragmentWritableBatch(false, QueryId.getDefaultInstance(), 1,
          1, 1, 1, getRandomBatch(c.getAllocator(), 5000)));
      System.out.println(System.currentTimeMillis() - t1);
      // System.out.println("sent.");
    }
    System.out.println(String.format("Max time: %d", max.get()));
    assertTrue(max.get() > 2700);
    Thread.sleep(5000);
  }

  private static WritableBatch getRandomBatch(BufferAllocator allocator, int records) {
    List<ValueVector> vectors = Lists.newArrayList();
    for (int i = 0; i < 5; i++) {
      Float8Vector v = (Float8Vector) TypeHelper.getNewVector(
          MaterializedField.create("a", Types.required(MinorType.FLOAT8)),
          allocator);
      v.allocateNew(records);
      v.getMutator().generateTestData(records);
      vectors.add(v);
    }
    return WritableBatch.getBatchNoHV(records, vectors, false);
  }

  private class TimingOutcome implements RpcOutcomeListener<Ack> {
    private AtomicLong max;
    private Stopwatch watch = Stopwatch.createStarted();

    public TimingOutcome(AtomicLong max) {
      super();
      this.max = max;
    }

    @Override
    public void failed(RpcException ex) {
      ex.printStackTrace();
    }

    @Override
    public void success(Ack value, ByteBuf buffer) {
      long micros = watch.elapsed(TimeUnit.MILLISECONDS);
      System.out.println(String.format("Total time to send: %d, start time %d", micros, System.currentTimeMillis() - micros));
      while (true) {
        long nowMax = max.get();
        if (nowMax < micros) {
          if (max.compareAndSet(nowMax, micros)) {
            break;
          }
        } else {
          break;
        }
      }
    }

    @Override
    public void interrupted(final InterruptedException e) {
      // TODO(We don't have any interrupts in test code)
    }
  }

}
