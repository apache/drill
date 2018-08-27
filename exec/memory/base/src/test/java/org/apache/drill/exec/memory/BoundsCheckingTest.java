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
package org.apache.drill.exec.memory;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.buffer.DrillBuf;
import io.netty.util.IllegalReferenceCountException;

import static org.junit.Assert.fail;

public class BoundsCheckingTest
{
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BoundsCheckingTest.class);

  private static boolean old;

  private RootAllocator allocator;

  private static boolean setBoundsChecking(boolean enabled) throws Exception
  {
    Field field = BoundsChecking.class.getDeclaredField("BOUNDS_CHECKING_ENABLED");
    Field modifiersField = Field.class.getDeclaredField("modifiers");
    modifiersField.setAccessible(true);
    modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
    boolean old = field.getBoolean(null);
    field.setAccessible(true);
    field.set(null, enabled);
    return old;
  }

  @BeforeClass
  public static void setBoundsCheckingEnabled() throws Exception
  {
    old = setBoundsChecking(true);
  }

  @AfterClass
  public static void restoreBoundsChecking() throws Exception
  {
    setBoundsChecking(old);
  }

  @Before
  public void setupAllocator()
  {
    allocator = new RootAllocator(Integer.MAX_VALUE);
  }

  @After
  public void closeAllocator()
  {
    allocator.close();
  }

  @Test
  public void testLengthCheck()
  {
    try {
      BoundsChecking.lengthCheck(null, 0, 0);
      fail("expecting NullPointerException");
    } catch (NullPointerException e) {
      logger.debug("", e);
    }

    try (DrillBuf buffer = allocator.buffer(1)) {
      try {
        BoundsChecking.lengthCheck(buffer, 0, -1);
        fail("expecting IllegalArgumentException");
      } catch (IllegalArgumentException e) {
        logger.debug("", e);
      }
      BoundsChecking.lengthCheck(buffer, 0, 0);
      BoundsChecking.lengthCheck(buffer, 0, 1);
      try {
        BoundsChecking.lengthCheck(buffer, 0, 2);
        fail("expecting IndexOutOfBoundsException");
      } catch (IndexOutOfBoundsException e) {
        logger.debug("", e);
      }
      try {
        BoundsChecking.lengthCheck(buffer, 2, 0);
        fail("expecting IndexOutOfBoundsException");
      } catch (IndexOutOfBoundsException e) {
        logger.debug("", e);
      }
      try {
        BoundsChecking.lengthCheck(buffer, -1, 0);
        fail("expecting IndexOutOfBoundsException");
      } catch (IndexOutOfBoundsException e) {
        logger.debug("", e);
      }
    }

    DrillBuf buffer = allocator.buffer(1);
    buffer.release();
    try {
      BoundsChecking.lengthCheck(buffer, 0, 0);
      fail("expecting IllegalReferenceCountException");
    } catch (IllegalReferenceCountException e) {
      logger.debug("", e);
    }
  }
}
