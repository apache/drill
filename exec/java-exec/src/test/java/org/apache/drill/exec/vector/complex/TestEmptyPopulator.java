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
package org.apache.drill.exec.vector.complex;

import java.nio.ByteBuffer;

import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.ExecTest;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.vector.UInt4Vector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TestEmptyPopulator extends ExecTest {
  private static final int BUF_SIZE = 10000;

  @Mock
  private BufferAllocator allocator;
  private UInt4Vector offsets;
  private UInt4Vector.Accessor accessor;
  private UInt4Vector.Mutator mutator;
  private EmptyValuePopulator populator;

  private final ByteBuffer buffer = ByteBuffer.allocateDirect(BUF_SIZE);


  @Before
  public void initialize() {
    Mockito.when(allocator.buffer(Mockito.anyInt())).thenReturn(DrillBuf.wrapByteBuffer(buffer));
    offsets = new UInt4Vector(null, allocator);
    offsets.allocateNewSafe();
    accessor = offsets.getAccessor();
    mutator = offsets.getMutator();
    mutator.set(0, 0);
    mutator.setValueCount(1);
    Assert.assertTrue("offsets must have one value", accessor.getValueCount() == 1);
    populator = new EmptyValuePopulator(offsets);
  }

  @Test(expected = IndexOutOfBoundsException.class)
  public void testNegativeValuesThrowException() {
    populator.populate(-1);
  }

  @Test
  public void testZeroHasNoEffect() {
    populator.populate(0);
    Assert.assertTrue("offset must have one value", accessor.getValueCount() == 1);
  }

  @Test
  public void testEmptyPopulationWorks() {
    populator.populate(1);
    Assert.assertEquals("offset must have valid size", 2, accessor.getValueCount());
    Assert.assertEquals("value must match", 0, accessor.get(1));

    mutator.set(1, 10);
    populator.populate(2);
    Assert.assertEquals("offset must have valid size", 3, accessor.getValueCount());
    Assert.assertEquals("value must match", 10, accessor.get(1));

    mutator.set(2, 20);
    populator.populate(5);
    Assert.assertEquals("offset must have valid size", 6, accessor.getValueCount());
    for (int i=2; i<=5;i++) {
      Assert.assertEquals(String.format("value at index[%s] must match", i), 20, accessor.get(i));
    }

    populator.populate(0);
    Assert.assertEquals("offset must have valid size", 1, accessor.getValueCount());
    Assert.assertEquals("value must match", 0, accessor.get(0));
  }
}
