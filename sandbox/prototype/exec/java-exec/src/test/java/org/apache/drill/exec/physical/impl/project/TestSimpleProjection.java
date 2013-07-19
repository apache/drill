package org.apache.drill.exec.physical.impl.project;

import static org.junit.Assert.assertEquals;
import mockit.Injectable;
import mockit.NonStrictExpectations;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.FragmentRoot;
import org.apache.drill.exec.physical.impl.ImplCreator;
import org.apache.drill.exec.physical.impl.SimpleRootExec;
import org.apache.drill.exec.planner.PhysicalPlanReader;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.rpc.user.UserServer.UserClientConnection;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.vector.BigIntVector;
import org.junit.After;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.yammer.metrics.MetricRegistry;

public class TestSimpleProjection {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestSimpleProjection.class);
  DrillConfig c = DrillConfig.create();
  
  
  @Test
  public void project(@Injectable final DrillbitContext bitContext, @Injectable UserClientConnection connection) throws Exception{


    new NonStrictExpectations(){{
      bitContext.getMetrics(); result = new MetricRegistry("test");
      bitContext.getAllocator(); result = BufferAllocator.getAllocator(c);
    }};
    
    
    PhysicalPlanReader reader = new PhysicalPlanReader(c, c.getMapper(), CoordinationProtos.DrillbitEndpoint.getDefaultInstance());
    PhysicalPlan plan = reader.readPhysicalPlan(Files.toString(FileUtils.getResourceAsFile("/project/test1.json"), Charsets.UTF_8));
    FunctionImplementationRegistry registry = new FunctionImplementationRegistry(c);
    FragmentContext context = new FragmentContext(bitContext, FragmentHandle.getDefaultInstance(), connection, null, registry);
    SimpleRootExec exec = new SimpleRootExec(ImplCreator.getExec(context, (FragmentRoot) plan.getSortedOperators(false).iterator().next()));
    while(exec.next()){
      BigIntVector c1 = exec.getValueVectorById(new SchemaPath("col1", ExpressionPosition.UNKNOWN), BigIntVector.class);
      BigIntVector c2 = exec.getValueVectorById(new SchemaPath("col2", ExpressionPosition.UNKNOWN), BigIntVector.class);
      int x = 0;
      BigIntVector.Accessor a1, a2;
      a1 = c1.getAccessor();
      a2 = c2.getAccessor();
      
      for(int i =0; i < c1.getAccessor().getValueCount(); i++){
        assertEquals(a1.get(i)+1, a2.get(i));
        x += a1.get(i);
      }
      
      System.out.println(x);
    }
  }
  
  @After
  public void tearDown() throws Exception{
    // pause to get logger to catch up.
    Thread.sleep(1000);
  }
}
