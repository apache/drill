package org.apache.drill.exec.store;

import mockit.NonStrictExpectations;
import net.hydromatic.optiq.SchemaPlus;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.memory.TopLevelAllocator;
import org.apache.drill.exec.server.DrillbitContext;
import org.junit.Test;

import com.codahale.metrics.MetricRegistry;

public class TestOrphanSchema {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestOrphanSchema.class);
  
  
  @Test
  public void test(final DrillbitContext bitContext){
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
    
    StoragePluginRegistry r = new StoragePluginRegistry(bitContext);
    SchemaPlus plus = r.getSchemaFactory().getOrphanedRootSchema();
    
    printSchema(plus, 0);
        
  }
  
  private static void t(final int t){
    for(int i =0; i < t; i++) System.out.print('\t');
  }
  private static void printSchema(SchemaPlus s, int indent){
    t(indent);
    System.out.print("Schema: ");
    System.out.println(s.getName().equals("") ? "root" : s.getName());
    for(String table : s.getTableNames()){
      t(indent + 1);
      System.out.print("Table: ");
      System.out.println(table);
    }
    
    for(String schema : s.getSubSchemaNames()){
      SchemaPlus p = s.getSubSchema(schema);
      printSchema(p, indent + 1);
    }
    
  }
}
