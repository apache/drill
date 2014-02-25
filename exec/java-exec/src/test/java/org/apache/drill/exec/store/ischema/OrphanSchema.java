package org.apache.drill.exec.store.ischema;


import static org.mockito.Mockito.*;
import net.hydromatic.optiq.SchemaPlus;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.memory.TopLevelAllocator;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.junit.Test;


import com.codahale.metrics.MetricRegistry;
/**
 * OrphanSchema is a stand alone schema tree which is not connected to Optiq.
 * This class is a refactoring of exec.store.TestOrphanSchema.java. The primary
 * change is to package a "create()" method for providing a test schema. 
 * For convenient testing, it mocks up the Drillbit context.
 */
public class OrphanSchema {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(OrphanSchema.class);
  
  /**
   * Create an orphan schema to be used for testing.
   * @return root node of the created schema.
   */
  public static SchemaPlus create(){
    
    final DrillConfig c = DrillConfig.create();
    
    // Mock up a context which will allow us to create a schema.
    final DrillbitContext bitContext = mock(DrillbitContext.class);
    when(bitContext.getMetrics()).thenReturn(new MetricRegistry());
    when(bitContext.getAllocator()).thenReturn(new TopLevelAllocator());
    when(bitContext.getConfig()).thenReturn(c);
    
    // Using the mock context, get the orphan schema.
    StoragePluginRegistry r = new StoragePluginRegistry(bitContext);
    SchemaPlus plus = r.getSchemaFactory().getOrphanedRootSchema();

    return plus;
  }
  
  
  /**
   * This test replicates the one in org.apache.drill.exec.server,
   * but it is refactored to provide a standalone "create()" method.
   */
  
  @Test
  public void test() {
    printSchema(create(), 0);
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
