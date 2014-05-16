package org.apache.drill.exec;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.logical.PlanProperties;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.config.Filter;
import org.apache.drill.exec.physical.config.Screen;
import org.apache.drill.exec.physical.config.UnionExchange;
import org.apache.drill.exec.planner.PhysicalPlanReader;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.store.mock.MockSubScanPOP;
import org.junit.Test;

import com.google.hive12.common.collect.Lists;

public class TestOpSerialization {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestOpSerialization.class);

  @Test
  public void testSerializedDeserialize() throws Throwable {
    DrillConfig c = DrillConfig.create();
    PhysicalPlanReader reader = new PhysicalPlanReader(c, c.getMapper(), CoordinationProtos.DrillbitEndpoint.getDefaultInstance());
    MockSubScanPOP s = new MockSubScanPOP("abc", null);
    s.setOperatorId(3);
    Filter f = new Filter(s, new ValueExpressions.BooleanExpression("true", ExpressionPosition.UNKNOWN), 0.1f);
    f.setOperatorId(2);
    UnionExchange e = new UnionExchange(f);
    e.setOperatorId(1);
    Screen screen = new Screen(e, CoordinationProtos.DrillbitEndpoint.getDefaultInstance());
    screen.setOperatorId(0);

    boolean reversed = false;
    while(true){

      List<PhysicalOperator> pops = Lists.newArrayList();
      pops.add(s);
      pops.add(e);
      pops.add(f);
      pops.add(screen);

      if(reversed) pops = Lists.reverse(pops);
      PhysicalPlan plan1 = new PhysicalPlan(PlanProperties.builder().build(), pops);
      String json = plan1.unparse(c.getMapper().writer());
      System.out.println(json);

      PhysicalPlan plan2 = reader.readPhysicalPlan(json);
      System.out.println("++++++++");
      System.out.println(plan2.unparse(c.getMapper().writer()));

      PhysicalOperator root = plan2.getSortedOperators(false).iterator().next();
      assertEquals(0, root.getOperatorId());
      PhysicalOperator o1 = root.iterator().next();
      assertEquals(1, o1.getOperatorId());
      PhysicalOperator o2 = o1.iterator().next();
      assertEquals(2, o2.getOperatorId());
      if(reversed) break;
      reversed = !reversed;
    }




  }
}
