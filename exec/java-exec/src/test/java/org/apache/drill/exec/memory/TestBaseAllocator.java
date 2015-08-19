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
package org.apache.drill.exec.memory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import io.netty.buffer.DrillBuf;

import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.testing.ExecutionControls;
import org.apache.drill.exec.util.Pointer;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;

public class TestBaseAllocator {
  // private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestBaseAllocator.class);

  private final static int MAX_ALLOCATION = 8 * 1024;

/*
  // ---------------------------------------- DEBUG -----------------------------------

  @After
  public void checkBuffers() {
    final int bufferCount = UnsafeDirectLittleEndian.getBufferCount();
    if (bufferCount != 0) {
      UnsafeDirectLittleEndian.logBuffers(logger);
      UnsafeDirectLittleEndian.releaseBuffers();
    }

    assertEquals(0, bufferCount);
  }

//  @AfterClass
//  public static void dumpBuffers() {
//    UnsafeDirectLittleEndian.logBuffers(logger);
//  }

  // ---------------------------------------- DEBUG ------------------------------------
*/

  // Concoct ExecutionControls that won't try to inject anything.
  @Mock private static final OptionManager optionManager = Mockito.mock(OptionManager.class);
  static {
    Mockito.when(optionManager.getOption(Matchers.anyString()))
      .thenReturn(null);
  }

  @Mock private static final ExecutionControls executionControls = new ExecutionControls(optionManager, null);

  private final static class NamedOwner implements AllocatorOwner {
    private final String name;

    public NamedOwner(final String name) {
      this.name = name;
    }

    @Override
    public String toString() {
      return name;
    }

    @Override
    public ExecutionControls getExecutionControls() {
      return executionControls;
    }
  }

  @Test
  public void test_privateMax() throws Exception {
    final AllocatorOwner allocatorOwner = new NamedOwner("noLimits");
    try(final BufferAllocator rootAllocator =
        RootAllocatorFactory.newRoot(RootAllocator.POLICY_LOCAL_MAX, 0, MAX_ALLOCATION, 0)) {
      final DrillBuf drillBuf1 = rootAllocator.buffer(MAX_ALLOCATION / 2);
      assertNotNull("allocation failed", drillBuf1);

      try(final BufferAllocator childAllocator =
          rootAllocator.newChildAllocator(allocatorOwner, 0, MAX_ALLOCATION, 0)) {
        final DrillBuf drillBuf2 = childAllocator.buffer(MAX_ALLOCATION / 2);
        assertNotNull("allocation failed", drillBuf2);
        drillBuf2.release();
      }

      drillBuf1.release();
    }
  }

  @Test(expected=IllegalStateException.class)
  public void testRootAllocator_closeWithOutstanding() throws Exception {
    try {
      try(final BufferAllocator rootAllocator =
          RootAllocatorFactory.newRoot(RootAllocator.POLICY_PER_FRAGMENT, 0, MAX_ALLOCATION, 0)) {
        final DrillBuf drillBuf = rootAllocator.buffer(512);
        assertNotNull("allocation failed", drillBuf);
      }
    } finally {
      /*
       * We expect there to be one unreleased underlying buffer because we're closing
       * without releasing it.
       */
/*
      // ------------------------------- DEBUG ---------------------------------
      final int bufferCount = UnsafeDirectLittleEndian.getBufferCount();
      UnsafeDirectLittleEndian.releaseBuffers();
      assertEquals(1, bufferCount);
      // ------------------------------- DEBUG ---------------------------------
*/
    }
  }

  @Test
  public void testRootAllocator_getEmpty() throws Exception {
    try(final BufferAllocator rootAllocator =
        RootAllocatorFactory.newRoot(RootAllocator.POLICY_PER_FRAGMENT, 0, MAX_ALLOCATION, 0)) {
      final DrillBuf drillBuf = rootAllocator.buffer(0);
      assertNotNull("allocation failed", drillBuf);
      assertEquals("capacity was non-zero", 0, drillBuf.capacity());
      drillBuf.release();
    }
  }

  @Ignore // TODO(DRILL-2740)
  @Test(expected = IllegalStateException.class)
  public void testAllocator_unreleasedEmpty() throws Exception {
    try(final BufferAllocator rootAllocator =
        RootAllocatorFactory.newRoot(RootAllocator.POLICY_PER_FRAGMENT, 0, MAX_ALLOCATION, 0)) {
      @SuppressWarnings("unused")
      final DrillBuf drillBuf = rootAllocator.buffer(0);
    }
  }

  @Test
  public void testAllocator_transferOwnership() throws Exception {
    final AllocatorOwner allocatorOwner = new NamedOwner("changeOwnership");
    try(final BufferAllocator rootAllocator =
        RootAllocatorFactory.newRoot(RootAllocator.POLICY_PER_FRAGMENT, 0, MAX_ALLOCATION, 0)) {
      final BufferAllocator childAllocator1 =
          rootAllocator.newChildAllocator(allocatorOwner, 0, MAX_ALLOCATION, 0);
      final BufferAllocator childAllocator2 =
          rootAllocator.newChildAllocator(allocatorOwner, 0, MAX_ALLOCATION, 0);

      final DrillBuf drillBuf1 = childAllocator1.buffer(MAX_ALLOCATION / 4);
      rootAllocator.verifyAllocator();
      final boolean allocationFit = childAllocator2.takeOwnership(drillBuf1);
      rootAllocator.verifyAllocator();
      assertTrue(allocationFit);

      childAllocator1.close();
      rootAllocator.verifyAllocator();

      drillBuf1.release();
      childAllocator2.close();
    }
  }

  @Test
  public void testAllocator_shareOwnership() throws Exception {
    final AllocatorOwner allocatorOwner = new NamedOwner("shareOwnership");
    try(final BufferAllocator rootAllocator =
        RootAllocatorFactory.newRoot(RootAllocator.POLICY_PER_FRAGMENT, 0, MAX_ALLOCATION, 0)) {
      final BufferAllocator childAllocator1 =
          rootAllocator.newChildAllocator(allocatorOwner, 0, MAX_ALLOCATION, 0);
      final BufferAllocator childAllocator2 =
          rootAllocator.newChildAllocator(allocatorOwner, 0, MAX_ALLOCATION, 0);
      final DrillBuf drillBuf1 = childAllocator1.buffer(MAX_ALLOCATION / 4);
      rootAllocator.verifyAllocator();
      final Pointer<DrillBuf> pDrillBuf = new Pointer<>();
      boolean allocationFit;

      allocationFit = childAllocator2.shareOwnership(drillBuf1, pDrillBuf);
      assertTrue(allocationFit);
      rootAllocator.verifyAllocator();
      final DrillBuf drillBuf2 = pDrillBuf.value;
      assertNotNull(drillBuf2);
      assertNotEquals(drillBuf2, drillBuf1);

      drillBuf1.release();
      rootAllocator.verifyAllocator();
      childAllocator1.close();
      rootAllocator.verifyAllocator();

      final BufferAllocator childAllocator3 =
          rootAllocator.newChildAllocator(allocatorOwner, 0, MAX_ALLOCATION, 0);
      allocationFit = childAllocator3.shareOwnership(drillBuf2, pDrillBuf);
      assertTrue(allocationFit);
      final DrillBuf drillBuf3 = pDrillBuf.value;
      assertNotNull(drillBuf3);
      assertNotEquals(drillBuf3, drillBuf1);
      assertNotEquals(drillBuf3, drillBuf2);
      rootAllocator.verifyAllocator();

      drillBuf2.release();
      rootAllocator.verifyAllocator();
      childAllocator2.close();
      rootAllocator.verifyAllocator();

      drillBuf3.release();
      rootAllocator.verifyAllocator();
      childAllocator3.close();
    }
  }

  @Test
  public void testRootAllocator_createChildAndUse() throws Exception {
    final AllocatorOwner allocatorOwner = new NamedOwner("createChildAndUse");
    try(final BufferAllocator rootAllocator =
        RootAllocatorFactory.newRoot(RootAllocator.POLICY_PER_FRAGMENT, 0, MAX_ALLOCATION, 0)) {
      try(final BufferAllocator childAllocator =
          rootAllocator.newChildAllocator(allocatorOwner, 0, MAX_ALLOCATION, 0)) {
        final DrillBuf drillBuf = childAllocator.buffer(512);
        assertNotNull("allocation failed", drillBuf);
        drillBuf.release();
      }
    }
  }

  @Test(expected=IllegalStateException.class)
  public void testRootAllocator_createChildDontClose() throws Exception {
    try {
      final AllocatorOwner allocatorOwner = new NamedOwner("createChildDontClose");
      try(final BufferAllocator rootAllocator =
          RootAllocatorFactory.newRoot(RootAllocator.POLICY_PER_FRAGMENT, 0, MAX_ALLOCATION, 0)) {
        final BufferAllocator childAllocator =
            rootAllocator.newChildAllocator(allocatorOwner, 0, MAX_ALLOCATION, 0);
        final DrillBuf drillBuf = childAllocator.buffer(512);
        assertNotNull("allocation failed", drillBuf);
      }
    } finally {
      /*
       * We expect one underlying buffer because we closed a child allocator without
       * releasing the buffer allocated from it.
       */
/*
      // ------------------------------- DEBUG ---------------------------------
      final int bufferCount = UnsafeDirectLittleEndian.getBufferCount();
      UnsafeDirectLittleEndian.releaseBuffers();
      assertEquals(1, bufferCount);
      // ------------------------------- DEBUG ---------------------------------
*/
    }
  }

  private static void allocateAndFree(final BufferAllocator allocator) {
    final DrillBuf drillBuf = allocator.buffer(512);
    assertNotNull("allocation failed", drillBuf);
    drillBuf.release();

    final DrillBuf drillBuf2 = allocator.buffer(MAX_ALLOCATION);
    assertNotNull("allocation failed", drillBuf2);
    drillBuf2.release();

    final int nBufs = 8;
    final DrillBuf[] drillBufs = new DrillBuf[nBufs];
    for(int i = 0; i < drillBufs.length; ++i) {
      DrillBuf drillBufi = allocator.buffer(MAX_ALLOCATION / nBufs);
      assertNotNull("allocation failed", drillBufi);
      drillBufs[i] = drillBufi;
    }
    for(DrillBuf drillBufi : drillBufs) {
      drillBufi.release();
    }
  }

  @Test
  public void testAllocator_manyAllocations() throws Exception {
    final AllocatorOwner allocatorOwner = new NamedOwner("manyAllocations");
    try(final BufferAllocator rootAllocator =
        RootAllocatorFactory.newRoot(RootAllocator.POLICY_PER_FRAGMENT, 0, MAX_ALLOCATION, 0)) {
      try(final BufferAllocator childAllocator =
          rootAllocator.newChildAllocator(allocatorOwner, 0, MAX_ALLOCATION, 0)) {
        allocateAndFree(childAllocator);
      }
    }
  }

  @Test
  public void testAllocator_overAllocate() throws Exception {
    final AllocatorOwner allocatorOwner = new NamedOwner("overAllocate");
    try(final BufferAllocator rootAllocator =
        RootAllocatorFactory.newRoot(RootAllocator.POLICY_PER_FRAGMENT, 0, MAX_ALLOCATION, 0)) {
      try(final BufferAllocator childAllocator =
          rootAllocator.newChildAllocator(allocatorOwner, 0, MAX_ALLOCATION, 0)) {
        allocateAndFree(childAllocator);

        try {
          childAllocator.buffer(MAX_ALLOCATION + 1);
          fail("allocated memory beyond max allowed");
        } catch(OutOfMemoryRuntimeException e) {
          // expected
        }
      }
    }
  }

  @Test
  public void testAllocator_overAllocateParent() throws Exception {
    final AllocatorOwner allocatorOwner = new NamedOwner("overAllocateParent");
    try(final BufferAllocator rootAllocator =
        RootAllocatorFactory.newRoot(RootAllocator.POLICY_PER_FRAGMENT, 0, MAX_ALLOCATION, 0)) {
      try(final BufferAllocator childAllocator =
          rootAllocator.newChildAllocator(allocatorOwner, 0, MAX_ALLOCATION, 0)) {
        final DrillBuf drillBuf1 = rootAllocator.buffer(MAX_ALLOCATION / 2);
        assertNotNull("allocation failed", drillBuf1);
        final DrillBuf drillBuf2 = childAllocator.buffer(MAX_ALLOCATION / 2);
        assertNotNull("allocation failed", drillBuf2);

        try {
          childAllocator.buffer(MAX_ALLOCATION / 4);
          fail("allocated memory beyond max allowed");
        } catch(OutOfMemoryRuntimeException e) {
          // expected
        }

        drillBuf1.release();
        drillBuf2.release();
      }
    }
  }

  private static void testAllocator_sliceUpBufferAndRelease(
      final BufferAllocator rootAllocator, final BufferAllocator bufferAllocator) {
    final DrillBuf drillBuf1 = bufferAllocator.buffer(MAX_ALLOCATION / 2);
    rootAllocator.verifyAllocator();

    final DrillBuf drillBuf2 = drillBuf1.slice(16, drillBuf1.capacity() - 32);
    rootAllocator.verifyAllocator();
    final DrillBuf drillBuf3 = drillBuf2.slice(16, drillBuf2.capacity() - 32);
    rootAllocator.verifyAllocator();
    @SuppressWarnings("unused")
    final DrillBuf drillBuf4 = drillBuf3.slice(16, drillBuf3.capacity() - 32);
    rootAllocator.verifyAllocator();

    drillBuf3.release(); // since they share refcounts, one is enough to release them all
    rootAllocator.verifyAllocator();
  }

  @Test
  public void testAllocator_createSlices() throws Exception {
    final AllocatorOwner allocatorOwner = new NamedOwner("createSlices");
    try(final BufferAllocator rootAllocator =
        RootAllocatorFactory.newRoot(RootAllocator.POLICY_PER_FRAGMENT, 0, MAX_ALLOCATION, 0)) {
      testAllocator_sliceUpBufferAndRelease(rootAllocator, rootAllocator);

      try(final BufferAllocator childAllocator =
          rootAllocator.newChildAllocator(allocatorOwner, 0, MAX_ALLOCATION, 0)) {
        testAllocator_sliceUpBufferAndRelease(rootAllocator, childAllocator);
      }
      rootAllocator.verifyAllocator();

      testAllocator_sliceUpBufferAndRelease(rootAllocator, rootAllocator);

      try(final BufferAllocator childAllocator =
          rootAllocator.newChildAllocator(allocatorOwner, 0, MAX_ALLOCATION, 0)) {
        try(final BufferAllocator childAllocator2 =
            childAllocator.newChildAllocator(allocatorOwner, 0, MAX_ALLOCATION, 0)) {
          final DrillBuf drillBuf1 = childAllocator2.buffer(MAX_ALLOCATION / 8);
          @SuppressWarnings("unused")
          final DrillBuf drillBuf2 = drillBuf1.slice(MAX_ALLOCATION / 16, MAX_ALLOCATION / 16);
          testAllocator_sliceUpBufferAndRelease(rootAllocator, childAllocator);
          drillBuf1.release();
          rootAllocator.verifyAllocator();
        }
        rootAllocator.verifyAllocator();

        testAllocator_sliceUpBufferAndRelease(rootAllocator, childAllocator);
      }
      rootAllocator.verifyAllocator();
    }
  }

  @Test
  public void testAllocator_sliceRanges() throws Exception {
//    final AllocatorOwner allocatorOwner = new NamedOwner("sliceRanges");
    try(final BufferAllocator rootAllocator =
        RootAllocatorFactory.newRoot(RootAllocator.POLICY_PER_FRAGMENT, 0, MAX_ALLOCATION, 0)) {
      // Populate a buffer with byte values corresponding to their indices.
      final DrillBuf drillBuf = rootAllocator.buffer(256, 256 + 256);
      assertEquals(256, drillBuf.capacity());
      assertEquals(256 + 256, drillBuf.maxCapacity());
      assertEquals(0, drillBuf.readerIndex());
      assertEquals(0, drillBuf.readableBytes());
      assertEquals(0, drillBuf.writerIndex());
      assertEquals(256, drillBuf.writableBytes());

      final DrillBuf slice3 = (DrillBuf) drillBuf.slice();
      assertEquals(0, slice3.readerIndex());
      assertEquals(0, slice3.readableBytes());
      assertEquals(0, slice3.writerIndex());
//      assertEquals(256, slice3.capacity());
//      assertEquals(256, slice3.writableBytes());

      for(int i = 0; i < 256; ++i) {
        drillBuf.writeByte(i);
      }
      assertEquals(0, drillBuf.readerIndex());
      assertEquals(256, drillBuf.readableBytes());
      assertEquals(256, drillBuf.writerIndex());
      assertEquals(0, drillBuf.writableBytes());

      final DrillBuf slice1 = (DrillBuf) drillBuf.slice();
      assertEquals(0, slice1.readerIndex());
      assertEquals(256, slice1.readableBytes());
      for(int i = 0; i < 10; ++i) {
        assertEquals(i, slice1.readByte());
      }
      assertEquals(256 - 10, slice1.readableBytes());
      for(int i = 0; i < 256; ++i) {
        assertEquals((byte) i, slice1.getByte(i));
      }

      final DrillBuf slice2 = (DrillBuf) drillBuf.slice(25, 25);
      assertEquals(0, slice2.readerIndex());
      assertEquals(25, slice2.readableBytes());
      for(int i = 25; i < 50; ++i) {
        assertEquals(i, slice2.readByte());
      }

/*
      for(int i = 256; i > 0; --i) {
        slice3.writeByte(i - 1);
      }
      for(int i = 0; i < 256; ++i) {
        assertEquals(255 - i, slice1.getByte(i));
      }
*/

      drillBuf.release(); // all the derived buffers share this fate
    }
  }

  @Test
  public void testAllocator_slicesOfSlices() throws Exception {
//    final AllocatorOwner allocatorOwner = new NamedOwner("slicesOfSlices");
    try(final BufferAllocator rootAllocator =
        RootAllocatorFactory.newRoot(RootAllocator.POLICY_PER_FRAGMENT, 0, MAX_ALLOCATION, 0)) {
      // Populate a buffer with byte values corresponding to their indices.
      final DrillBuf drillBuf = rootAllocator.buffer(256, 256 + 256);
      for(int i = 0; i < 256; ++i) {
        drillBuf.writeByte(i);
      }

      // Slice it up.
      final DrillBuf slice0 = drillBuf.slice(0, drillBuf.capacity());
      for(int i = 0; i < 256; ++i) {
        assertEquals((byte) i, drillBuf.getByte(i));
      }

      final DrillBuf slice10 = slice0.slice(10, drillBuf.capacity() - 10);
      for(int i = 10; i < 256; ++i) {
        assertEquals((byte) i, slice10.getByte(i - 10));
      }

      final DrillBuf slice20 = slice10.slice(10, drillBuf.capacity() - 20);
      for(int i = 20; i < 256; ++i) {
        assertEquals((byte) i, slice20.getByte(i - 20));
      }

      final DrillBuf slice30 = slice20.slice(10,  drillBuf.capacity() - 30);
      for(int i = 30; i < 256; ++i) {
        assertEquals((byte) i, slice30.getByte(i - 30));
      }

      drillBuf.release();
    }
  }

  @Test
  public void testAllocator_transferSliced() throws Exception {
    final AllocatorOwner allocatorOwner = new NamedOwner("transferSliced");
    try(final BufferAllocator rootAllocator =
        RootAllocatorFactory.newRoot(RootAllocator.POLICY_PER_FRAGMENT, 0, MAX_ALLOCATION, 0)) {
      final BufferAllocator childAllocator1 =
          rootAllocator.newChildAllocator(allocatorOwner, 0, MAX_ALLOCATION, 0);
      final BufferAllocator childAllocator2 =
          rootAllocator.newChildAllocator(allocatorOwner, 0, MAX_ALLOCATION, 0);

      final DrillBuf drillBuf1 = childAllocator1.buffer(MAX_ALLOCATION / 8);
      final DrillBuf drillBuf2 = childAllocator2.buffer(MAX_ALLOCATION / 8);

      final DrillBuf drillBuf1s = drillBuf1.slice(0, drillBuf1.capacity() / 2);
      final DrillBuf drillBuf2s = drillBuf2.slice(0, drillBuf2.capacity() / 2);

      rootAllocator.verifyAllocator();

      childAllocator1.takeOwnership(drillBuf2s);
      rootAllocator.verifyAllocator();
      childAllocator2.takeOwnership(drillBuf1s);
      rootAllocator.verifyAllocator();

      drillBuf1s.release(); // releases drillBuf1
      drillBuf2s.release(); // releases drillBuf2

      childAllocator1.close();
      childAllocator2.close();
    }
  }

  @Test
  public void testAllocator_shareSliced() throws Exception {
    final AllocatorOwner allocatorOwner = new NamedOwner("transferSliced");
    try(final BufferAllocator rootAllocator =
        RootAllocatorFactory.newRoot(RootAllocator.POLICY_PER_FRAGMENT, 0, MAX_ALLOCATION, 0)) {
      final BufferAllocator childAllocator1 =
          rootAllocator.newChildAllocator(allocatorOwner, 0, MAX_ALLOCATION, 0);
      final BufferAllocator childAllocator2 =
          rootAllocator.newChildAllocator(allocatorOwner, 0, MAX_ALLOCATION, 0);

      final DrillBuf drillBuf1 = childAllocator1.buffer(MAX_ALLOCATION / 8);
      final DrillBuf drillBuf2 = childAllocator2.buffer(MAX_ALLOCATION / 8);

      final DrillBuf drillBuf1s = drillBuf1.slice(0, drillBuf1.capacity() / 2);
      final DrillBuf drillBuf2s = drillBuf2.slice(0, drillBuf2.capacity() / 2);

      rootAllocator.verifyAllocator();

      final Pointer<DrillBuf> pDrillBuf = new Pointer<>();
      childAllocator1.shareOwnership(drillBuf2s, pDrillBuf);
      final DrillBuf drillBuf2s1 = pDrillBuf.value;
      childAllocator2.shareOwnership(drillBuf1s, pDrillBuf);
      final DrillBuf drillBuf1s2 = pDrillBuf.value;
      rootAllocator.verifyAllocator();

      drillBuf1s.release(); // releases drillBuf1
      drillBuf2s.release(); // releases drillBuf2
      rootAllocator.verifyAllocator();

      drillBuf2s1.release(); // releases the shared drillBuf2 slice
      drillBuf1s2.release(); // releases the shared drillBuf1 slice

      childAllocator1.close();
      childAllocator2.close();
    }
  }

  @Test
  public void testAllocator_transferShared() throws Exception {
    final AllocatorOwner allocatorOwner = new NamedOwner("transferShared");
    try(final BufferAllocator rootAllocator =
        RootAllocatorFactory.newRoot(RootAllocator.POLICY_PER_FRAGMENT, 0, MAX_ALLOCATION, 0)) {
      final BufferAllocator childAllocator1 =
          rootAllocator.newChildAllocator(allocatorOwner, 0, MAX_ALLOCATION, 0);
      final BufferAllocator childAllocator2 =
          rootAllocator.newChildAllocator(allocatorOwner, 0, MAX_ALLOCATION, 0);
      final BufferAllocator childAllocator3 =
          rootAllocator.newChildAllocator(allocatorOwner, 0, MAX_ALLOCATION, 0);

      final DrillBuf drillBuf1 = childAllocator1.buffer(MAX_ALLOCATION / 8);

      final Pointer<DrillBuf> pDrillBuf = new Pointer<>();
      boolean allocationFit;

      allocationFit = childAllocator2.shareOwnership(drillBuf1, pDrillBuf);
      assertTrue(allocationFit);
      rootAllocator.verifyAllocator();
      final DrillBuf drillBuf2 = pDrillBuf.value;
      assertNotNull(drillBuf2);
      assertNotEquals(drillBuf2, drillBuf1);

      allocationFit = childAllocator3.takeOwnership(drillBuf1);
      assertTrue(allocationFit);
      rootAllocator.verifyAllocator();

      // Since childAllocator3 now has childAllocator1's buffer, 1, can close
      childAllocator1.close();
      rootAllocator.verifyAllocator();

      drillBuf2.release();
      childAllocator2.close();
      rootAllocator.verifyAllocator();

      final BufferAllocator childAllocator4 =
          rootAllocator.newChildAllocator(allocatorOwner, 0, MAX_ALLOCATION, 0);
      allocationFit = childAllocator4.takeOwnership(drillBuf1);
      assertTrue(allocationFit);
      rootAllocator.verifyAllocator();

      childAllocator3.close();
      rootAllocator.verifyAllocator();

      drillBuf1.release();
      childAllocator4.close();
      rootAllocator.verifyAllocator();
    }
  }

  @Test
  public void testAllocator_unclaimedReservation() throws Exception {
    final AllocatorOwner allocatorOwner = new NamedOwner("unclaimedReservation");
    try(final BufferAllocator rootAllocator =
        RootAllocatorFactory.newRoot(RootAllocator.POLICY_PER_FRAGMENT, 0, MAX_ALLOCATION, 0)) {
      try(final BufferAllocator childAllocator1 =
            rootAllocator.newChildAllocator(allocatorOwner, 0, MAX_ALLOCATION, 0)) {
        try(final AllocationReservation reservation = childAllocator1.newReservation()) {
          assertTrue(reservation.add(64));
        }
        rootAllocator.verifyAllocator();
      }
    }
  }

  @Test
  public void testAllocator_claimedReservation() throws Exception {
    final AllocatorOwner allocatorOwner = new NamedOwner("claimedReservation");
    try(final BufferAllocator rootAllocator =
        RootAllocatorFactory.newRoot(RootAllocator.POLICY_PER_FRAGMENT, 0, MAX_ALLOCATION, 0)) {
      try(final BufferAllocator childAllocator1 =
            rootAllocator.newChildAllocator(allocatorOwner, 0, MAX_ALLOCATION, 0)) {
        try(final AllocationReservation reservation = childAllocator1.newReservation()) {
          assertTrue(reservation.add(32));
          assertTrue(reservation.add(32));

          final DrillBuf drillBuf = reservation.buffer();
          assertEquals(64, drillBuf.capacity());
          rootAllocator.verifyAllocator();

          drillBuf.release();
          rootAllocator.verifyAllocator();
        }
        rootAllocator.verifyAllocator();
      }
    }
  }
}
