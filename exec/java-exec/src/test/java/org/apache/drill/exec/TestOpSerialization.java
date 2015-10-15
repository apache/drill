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

package org.apache.drill.exec;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.config.LogicalPlanPersistence;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.logical.PlanProperties;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.config.Filter;
import org.apache.drill.exec.physical.config.Screen;
import org.apache.drill.exec.physical.config.UnionExchange;
import org.apache.drill.exec.planner.PhysicalPlanReader;
import org.apache.drill.exec.planner.PhysicalPlanReaderTestFactory;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.store.mock.MockSubScanPOP;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestOpSerialization {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestOpSerialization.class);

  @Test
  public void testSerializedDeserialize() throws Throwable {
    DrillConfig c = DrillConfig.create();
    PhysicalPlanReader reader = PhysicalPlanReaderTestFactory.defaultPhysicalPlanReader(c);
    MockSubScanPOP s = new MockSubScanPOP("abc", null);
    s.setOperatorId(3);
    Filter f = new Filter(s, new ValueExpressions.BooleanExpression("true", ExpressionPosition.UNKNOWN), 0.1f);
    f.setOperatorId(2);
    UnionExchange e = new UnionExchange(f);
    e.setOperatorId(1);
    Screen screen = new Screen(e, CoordinationProtos.DrillbitEndpoint.getDefaultInstance());
    screen.setOperatorId(0);

    boolean reversed = false;
    while (true) {

      List<PhysicalOperator> pops = Lists.newArrayList();
      pops.add(s);
      pops.add(e);
      pops.add(f);
      pops.add(screen);

      if (reversed) {
        pops = Lists.reverse(pops);
      }
      PhysicalPlan plan1 = new PhysicalPlan(PlanProperties.builder().build(), pops);
      LogicalPlanPersistence logicalPlanPersistence = PhysicalPlanReaderTestFactory.defaultLogicalPlanPersistence(c);
      String json = plan1.unparse(logicalPlanPersistence.getMapper().writer());

      PhysicalPlan plan2 = reader.readPhysicalPlan(json);

      PhysicalOperator root = plan2.getSortedOperators(false).iterator().next();
      assertEquals(0, root.getOperatorId());
      PhysicalOperator o1 = root.iterator().next();
      assertEquals(1, o1.getOperatorId());
      PhysicalOperator o2 = o1.iterator().next();
      assertEquals(2, o2.getOperatorId());
      if(reversed) {
        break;
      }
      reversed = !reversed;
    }

  }

}
