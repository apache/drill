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
package org.apache.drill.exec.ref.rops;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ref.TestUtils;
import org.apache.drill.exec.ref.UnbackedRecord;
import org.apache.drill.exec.ref.values.DataValue;
import org.apache.drill.exec.ref.values.ScalarValues.LongScalar;
import org.junit.Test;

public class CollapsingAggregateTest {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CollapsingAggregateTest.class);
  
  
  @Test
  public void checkNullsHandling() throws Exception{
    List<UnbackedRecord> records = TestUtils.getResultAsUnbackedRecords("/collapse/test1.json");

    DataValue[] depts = {DataValue.NULL_VALUE, new LongScalar(31), new LongScalar(33), new LongScalar(34)};
    DataValue[] cnts = {new LongScalar(1), new LongScalar(1), new LongScalar(2), new LongScalar(2)};
    SchemaPath typeCount = new SchemaPath("typeCount", ExpressionPosition.UNKNOWN);
    SchemaPath dept = new SchemaPath("deptId", ExpressionPosition.UNKNOWN);
    for(int i =0; i < depts.length; i++){
      UnbackedRecord r = records.get(i);
      assertEquals(String.format("Invalid dept value for record %d.", i), depts[i], r.getField(dept));
      assertEquals(String.format("Invalid type count value for record %d with deptId %s.", i, depts[i]), cnts[i], r.getField(typeCount));
    }
    
  }
}
