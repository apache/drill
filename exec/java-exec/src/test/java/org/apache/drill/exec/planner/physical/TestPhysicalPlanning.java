package org.apache.drill.exec.planner.physical;

import mockit.NonStrictExpectations;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.FunctionRegistry;
import org.apache.drill.exec.memory.TopLevelAllocator;
import org.apache.drill.exec.planner.sql.DrillSqlWorker;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.junit.Test;

import com.codahale.metrics.MetricRegistry;

public class TestPhysicalPlanning {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestPhysicalPlanning.class);
  
  @Test
  public void testSimpleQuery(final DrillbitContext bitContext) throws Exception{
    
    final DrillConfig c = DrillConfig.create();
    new NonStrictExpectations() {
      {
        bitContext.getMetrics();
        result = new MetricRegistry();
        bitContext.getAllocator();
        result = new TopLevelAllocator();
        bitContext.getConfig();
        result = c;
      }
    };
    
    FunctionRegistry reg = new FunctionRegistry(c);
    StoragePluginRegistry registry = new StoragePluginRegistry(bitContext);
    DrillSqlWorker worker = new DrillSqlWorker(registry.getSchemaFactory(), reg);
    worker.getPhysicalPlan("select * from cp.`employee.json`");
    
  }
}
