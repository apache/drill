/*
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
package org.apache.drill.exec.rpc.data;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;
import io.netty.buffer.ByteBuf;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import mockit.NonStrictExpectations;
import org.apache.drill.BaseTestQuery;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.scanner.ClassPathScanner;
import org.apache.drill.common.scanner.persistence.ScanResult;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.DrillbitStartupException;
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
import org.apache.drill.exec.rpc.security.KerberosHelper;
import org.apache.drill.exec.server.BootStrapContext;
import org.apache.drill.exec.vector.Float8Vector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.work.WorkManager.WorkerBee;
import org.apache.drill.exec.work.fragment.FragmentManager;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertTrue;

@Ignore("See DRILL-5387")
public class TestBitBitKerberos extends BaseTestQuery {
  //private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestBitBitKerberos.class);

  private static KerberosHelper krbHelper;
  private static DrillConfig newConfig;

  private static BootStrapContext c1;
  private static FragmentManager manager;
  private int port = 1234;

  @BeforeClass
  public static void setupTest() throws Exception {

    final Config config = DrillConfig.create(cloneDefaultTestConfigProperties());
    krbHelper = new KerberosHelper(TestBitBitKerberos.class.getSimpleName());
    krbHelper.setupKdc();

    newConfig = new DrillConfig(
        config.withValue(ExecConstants.AUTHENTICATION_MECHANISMS,
            ConfigValueFactory.fromIterable(Lists.newArrayList("kerberos")))
        .withValue(ExecConstants.BIT_AUTHENTICATION_ENABLED,
            ConfigValueFactory.fromAnyRef(true))
        .withValue(ExecConstants.BIT_AUTHENTICATION_MECHANISM,
            ConfigValueFactory.fromAnyRef("kerberos"))
        .withValue(ExecConstants.USE_LOGIN_PRINCIPAL,
            ConfigValueFactory.fromAnyRef(true))
        .withValue(BootStrapContext.SERVICE_PRINCIPAL,
            ConfigValueFactory.fromAnyRef(krbHelper.SERVER_PRINCIPAL))
        .withValue(BootStrapContext.SERVICE_KEYTAB_LOCATION,
            ConfigValueFactory.fromAnyRef(krbHelper.serverKeytab.toString())),
        false);

    // Ignore the compile time warning caused by the code below.

    // Config is statically initialized at this point. But the above configuration results in a different
    // initialization which causes the tests to fail. So the following two changes are required.

    // (1) Refresh Kerberos config.
    sun.security.krb5.Config.refresh();
    // (2) Reset the default realm.
    final Field defaultRealm = KerberosName.class.getDeclaredField("defaultRealm");
    defaultRealm.setAccessible(true);
    defaultRealm.set(null, KerberosUtil.getDefaultRealm());

    updateTestCluster(1, newConfig);

    ScanResult result = ClassPathScanner.fromPrescan(newConfig);
    c1 = new BootStrapContext(newConfig, result);
    setupFragmentContextAndManager();
  }

  private static void setupFragmentContextAndManager() {
    final FragmentContext fcontext = new MockUp<FragmentContext>(){
      @SuppressWarnings("unused")
      BufferAllocator getAllocator(){
        return c1.getAllocator();
      }
    }.getMockInstance();

    manager = new MockUp<FragmentManager>(){
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
        RawFragmentBatch rfb = batch.newRawFragmentBatch(c1.getAllocator());
        rfb.sendOk();
        rfb.release();

        return true;
      }

      @SuppressWarnings("unused")
      public FragmentContext getFragmentContext(){
        return fcontext;
      }

    }.getMockInstance();
  }

  private static WritableBatch getRandomBatch(BufferAllocator allocator, int records) {
    List<ValueVector> vectors = Lists.newArrayList();
    for (int i = 0; i < 5; i++) {
      @SuppressWarnings("resource")
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

    TimingOutcome(AtomicLong max) {
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
      System.out.println(String.format("Total time to send: %d, start time %d", micros,
          System.currentTimeMillis() - micros));
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

  @Test
  public void success(@Injectable WorkerBee bee, @Injectable final WorkEventBus workBus) throws Exception {

    new NonStrictExpectations() {{
      workBus.getFragmentManagerIfExists((FragmentHandle) any); result = manager;
      workBus.getFragmentManager( (FragmentHandle) any); result = manager;
    }};

    DataConnectionConfig config = new DataConnectionConfig(c1.getAllocator(), c1,
        new DataServerRequestHandler(workBus, bee));
    DataServer server = new DataServer(config);

    port = server.bind(port, true);
    DrillbitEndpoint ep = DrillbitEndpoint.newBuilder().setAddress("localhost").setDataPort(port).build();
    DataConnectionManager connectionManager = new DataConnectionManager(ep, config);
    DataTunnel tunnel = new DataTunnel(connectionManager);
    AtomicLong max = new AtomicLong(0);
    for (int i = 0; i < 40; i++) {
      long t1 = System.currentTimeMillis();
      tunnel.sendRecordBatch(new TimingOutcome(max), new FragmentWritableBatch(false, QueryId.getDefaultInstance(), 1,
          1, 1, 1, getRandomBatch(c1.getAllocator(), 5000)));
      System.out.println(System.currentTimeMillis() - t1);
      // System.out.println("sent.");
    }
    System.out.println(String.format("Max time: %d", max.get()));
    assertTrue(max.get() > 2700);
    Thread.sleep(5000);
  }

  @Test
  public void successEncryption(@Injectable WorkerBee bee, @Injectable final WorkEventBus workBus) throws Exception {

    newConfig = new DrillConfig(
      config.withValue(ExecConstants.AUTHENTICATION_MECHANISMS,
        ConfigValueFactory.fromIterable(Lists.newArrayList("kerberos")))
        .withValue(ExecConstants.BIT_AUTHENTICATION_ENABLED,
          ConfigValueFactory.fromAnyRef(true))
        .withValue(ExecConstants.BIT_AUTHENTICATION_MECHANISM,
          ConfigValueFactory.fromAnyRef("kerberos"))
        .withValue(ExecConstants.BIT_ENCRYPTION_SASL_ENABLED,
          ConfigValueFactory.fromAnyRef(true))
        .withValue(ExecConstants.USE_LOGIN_PRINCIPAL,
          ConfigValueFactory.fromAnyRef(true))
        .withValue(BootStrapContext.SERVICE_PRINCIPAL,
          ConfigValueFactory.fromAnyRef(krbHelper.SERVER_PRINCIPAL))
        .withValue(BootStrapContext.SERVICE_KEYTAB_LOCATION,
          ConfigValueFactory.fromAnyRef(krbHelper.serverKeytab.toString())),
      false);

    updateTestCluster(1, newConfig);

    new NonStrictExpectations() {{
      workBus.getFragmentManagerIfExists((FragmentHandle) any); result = manager;
      workBus.getFragmentManager( (FragmentHandle) any); result = manager;
    }};

    DataConnectionConfig config = new DataConnectionConfig(c1.getAllocator(), c1,
      new DataServerRequestHandler(workBus, bee));
    DataServer server = new DataServer(config);

    port = server.bind(port, true);
    DrillbitEndpoint ep = DrillbitEndpoint.newBuilder().setAddress("localhost").setDataPort(port).build();
    DataConnectionManager connectionManager = new DataConnectionManager(ep, config);
    DataTunnel tunnel = new DataTunnel(connectionManager);
    AtomicLong max = new AtomicLong(0);
    for (int i = 0; i < 40; i++) {
      long t1 = System.currentTimeMillis();
      tunnel.sendRecordBatch(new TimingOutcome(max), new FragmentWritableBatch(false, QueryId.getDefaultInstance(), 1,
        1, 1, 1, getRandomBatch(c1.getAllocator(), 5000)));
      System.out.println(System.currentTimeMillis() - t1);
    }
    System.out.println(String.format("Max time: %d", max.get()));
    assertTrue(max.get() > 2700);
    Thread.sleep(5000);
  }

  @Test
  public void successEncryptionChunkMode(@Injectable WorkerBee bee, @Injectable final WorkEventBus workBus)
    throws Exception {
    newConfig = new DrillConfig(
      config.withValue(ExecConstants.AUTHENTICATION_MECHANISMS,
        ConfigValueFactory.fromIterable(Lists.newArrayList("kerberos")))
        .withValue(ExecConstants.BIT_AUTHENTICATION_ENABLED,
          ConfigValueFactory.fromAnyRef(true))
        .withValue(ExecConstants.BIT_AUTHENTICATION_MECHANISM,
          ConfigValueFactory.fromAnyRef("kerberos"))
        .withValue(ExecConstants.BIT_ENCRYPTION_SASL_ENABLED,
          ConfigValueFactory.fromAnyRef(true))
        .withValue(ExecConstants.BIT_ENCRYPTION_SASL_MAX_WRAPPED_SIZE,
          ConfigValueFactory.fromAnyRef(100000))
        .withValue(ExecConstants.USE_LOGIN_PRINCIPAL,
          ConfigValueFactory.fromAnyRef(true))
        .withValue(BootStrapContext.SERVICE_PRINCIPAL,
          ConfigValueFactory.fromAnyRef(krbHelper.SERVER_PRINCIPAL))
        .withValue(BootStrapContext.SERVICE_KEYTAB_LOCATION,
          ConfigValueFactory.fromAnyRef(krbHelper.serverKeytab.toString())),
      false);

    updateTestCluster(1, newConfig);

    new NonStrictExpectations() {{
      workBus.getFragmentManagerIfExists((FragmentHandle) any); result = manager;
      workBus.getFragmentManager( (FragmentHandle) any); result = manager;
    }};

    DataConnectionConfig config = new DataConnectionConfig(c1.getAllocator(), c1,
      new DataServerRequestHandler(workBus, bee));
    DataServer server = new DataServer(config);

    port = server.bind(port, true);
    DrillbitEndpoint ep = DrillbitEndpoint.newBuilder().setAddress("localhost").setDataPort(port).build();
    DataConnectionManager connectionManager = new DataConnectionManager(ep, config);
    DataTunnel tunnel = new DataTunnel(connectionManager);
    AtomicLong max = new AtomicLong(0);
    for (int i = 0; i < 40; i++) {
      long t1 = System.currentTimeMillis();
      tunnel.sendRecordBatch(new TimingOutcome(max), new FragmentWritableBatch(false, QueryId.getDefaultInstance(), 1,
        1, 1, 1, getRandomBatch(c1.getAllocator(), 5000)));
      System.out.println(System.currentTimeMillis() - t1);
    }
    System.out.println(String.format("Max time: %d", max.get()));
    assertTrue(max.get() > 2700);
    Thread.sleep(5000);
  }

  @Test
  public void failureEncryptionOnlyPlainMechanism() throws Exception {
    try{
      newConfig = new DrillConfig(
        config.withValue(ExecConstants.AUTHENTICATION_MECHANISMS,
          ConfigValueFactory.fromIterable(Lists.newArrayList("plain")))
          .withValue(ExecConstants.BIT_AUTHENTICATION_ENABLED,
            ConfigValueFactory.fromAnyRef(true))
          .withValue(ExecConstants.BIT_AUTHENTICATION_MECHANISM,
            ConfigValueFactory.fromAnyRef("kerberos"))
          .withValue(ExecConstants.BIT_ENCRYPTION_SASL_ENABLED,
            ConfigValueFactory.fromAnyRef(true))
          .withValue(ExecConstants.USE_LOGIN_PRINCIPAL,
            ConfigValueFactory.fromAnyRef(true))
          .withValue(BootStrapContext.SERVICE_PRINCIPAL,
            ConfigValueFactory.fromAnyRef(krbHelper.SERVER_PRINCIPAL))
          .withValue(BootStrapContext.SERVICE_KEYTAB_LOCATION,
            ConfigValueFactory.fromAnyRef(krbHelper.serverKeytab.toString())),
        false);

      updateTestCluster(1, newConfig);
      fail();
    } catch(Exception ex) {
      assertTrue(ex.getCause() instanceof DrillbitStartupException);
    }
  }

  @AfterClass
  public static void cleanTest() throws Exception {
    krbHelper.stopKdc();
  }
}
