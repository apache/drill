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
package org.apache.drill.exec.fn.impl;

import static org.junit.Assert.assertTrue;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecTest;
import org.apache.drill.exec.expr.fn.impl.ByteFunctionHelpers;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.memory.TopLevelAllocator;
import org.apache.drill.exec.vector.ValueHolderHelper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestByteComparisonFunctions extends ExecTest{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestByteComparisonFunctions.class);

  static TopLevelAllocator allocator;
  static VarCharHolder hello;
  static VarCharHolder goodbye;
  static VarCharHolder helloLong;
  static VarCharHolder goodbyeLong;

  @BeforeClass
  public static void setup(){
    DrillConfig c= DrillConfig.create();
    allocator = new TopLevelAllocator(c);
    hello = ValueHolderHelper.getVarCharHolder(allocator, "hello");
    goodbye = ValueHolderHelper.getVarCharHolder(allocator, "goodbye");
    helloLong = ValueHolderHelper.getVarCharHolder(allocator, "hellomyfriend");
    goodbyeLong = ValueHolderHelper.getVarCharHolder(allocator, "goodbyemyenemy");
  }

  @AfterClass
  public static void teardown(){
    hello.buffer.release();
    helloLong.buffer.release();
    goodbye.buffer.release();
    goodbyeLong.buffer.release();
    allocator.close();
  }

  @Test
  public void testAfter(){
    VarCharHolder left = hello;
    VarCharHolder right = goodbye;
    assertTrue(ByteFunctionHelpers.compare(left.buffer, left.start, left.end, right.buffer, right.start, right.end) == 1);
  }

  @Test
  public void testBefore(){
    VarCharHolder left = goodbye;
    VarCharHolder right = hello;
    assertTrue(ByteFunctionHelpers.compare(left.buffer, left.start, left.end, right.buffer, right.start, right.end) == -1);
  }

  @Test
  public void testEqualCompare(){
    VarCharHolder left = hello;
    VarCharHolder right = hello;
    assertTrue(ByteFunctionHelpers.compare(left.buffer, left.start, left.end, right.buffer, right.start, right.end) == 0);
  }

  @Test
  public void testEqual(){
    VarCharHolder left = hello;
    VarCharHolder right = hello;
    assertTrue(ByteFunctionHelpers.equal(left.buffer, left.start, left.end, right.buffer, right.start, right.end) == 1);
  }

  @Test
  public void testNotEqual(){
    VarCharHolder left = hello;
    VarCharHolder right = goodbye;
    assertTrue(ByteFunctionHelpers.equal(left.buffer, left.start, left.end, right.buffer, right.start, right.end) == 0);
  }

  @Test
  public void testAfterLong(){
    VarCharHolder left = helloLong;
    VarCharHolder right = goodbyeLong;
    assertTrue(ByteFunctionHelpers.compare(left.buffer, left.start, left.end, right.buffer, right.start, right.end) == 1);
  }

  @Test
  public void testBeforeLong(){
    VarCharHolder left = goodbyeLong;
    VarCharHolder right = helloLong;
    assertTrue(ByteFunctionHelpers.compare(left.buffer, left.start, left.end, right.buffer, right.start, right.end) == -1);
  }

  @Test
  public void testEqualCompareLong(){
    VarCharHolder left = helloLong;
    VarCharHolder right = helloLong;
    assertTrue(ByteFunctionHelpers.compare(left.buffer, left.start, left.end, right.buffer, right.start, right.end) == 0);
  }

  @Test
  public void testEqualLong(){
    VarCharHolder left = helloLong;
    VarCharHolder right = helloLong;
    assertTrue(ByteFunctionHelpers.equal(left.buffer, left.start, left.end, right.buffer, right.start, right.end) == 1);
  }

  @Test
  public void testNotEqualLong(){
    VarCharHolder left = helloLong;
    VarCharHolder right = goodbyeLong;
    assertTrue(ByteFunctionHelpers.equal(left.buffer, left.start, left.end, right.buffer, right.start, right.end) == 0);
  }
}
