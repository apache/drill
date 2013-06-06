package org.apache.drill.exec.ref.rops;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ref.TestUtils;
import org.apache.drill.exec.ref.UnbackedRecord;
import org.apache.drill.exec.ref.values.DataValue;
import org.apache.drill.exec.ref.values.ScalarValues.LongScalar;
import org.junit.Test;

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

public class UnionROPTest {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UnionROPTest.class);
  
  
  @Test
  public void checkDistinct() throws Exception{
    TestUtils.assertProduceCount("/union/distinct.json", 5);
  }

  @Test
  public void checkNonDistinct() throws Exception{
    TestUtils.assertProduceCount("/union/nondistinct.json", 10);
  }

}
