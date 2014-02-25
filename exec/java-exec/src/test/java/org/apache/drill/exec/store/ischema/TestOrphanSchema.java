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
package org.apache.drill.exec.store.ischema;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import net.hydromatic.optiq.SchemaPlus;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.TopLevelAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.ischema.FixedTable;
import org.apache.drill.exec.store.ischema.InfoSchemaTable;
import org.apache.drill.exec.store.ischema.OptiqProvider;
import org.apache.drill.exec.store.ischema.RowProvider;
import org.apache.drill.exec.store.ischema.RowRecordReader;
import org.apache.drill.exec.vector.ValueVector;
import org.junit.Assert;
import org.junit.Test;

/**
 * Using an orphan schema, create and display the various information schema tables.
 * An "orphan schema" is a stand alone schema which is not (yet?) connected to Optiq.
 */
public class TestOrphanSchema {
  SchemaPlus root = OrphanSchema.create();

  @Test
  public void testTables() {
    displayTable(new InfoSchemaTable.Tables(), new OptiqProvider.Tables(root));
  }
  
  @Test
  public void testSchemata() {
    displayTable(new InfoSchemaTable.Schemata(), new OptiqProvider.Schemata(root));
  }
  
  
  @Test
  public void testViews() {
    displayTable(new InfoSchemaTable.Views(), new OptiqProvider.Views(root));
  }
  
  @Test
  public void testCatalogs() {
    displayTable(new InfoSchemaTable.Catalogs(), new OptiqProvider.Catalogs(root));
  }
  
  @Test
  public void testColumns() {
    displayTable(new InfoSchemaTable.Columns(), new OptiqProvider.Columns(root));
  }
  
  
  private void displayTable(FixedTable table, RowProvider provider) {

    // Set up a mock context
    FragmentContext context = mock(FragmentContext.class);
    when(context.getAllocator()).thenReturn(new TopLevelAllocator());
    
    // Create a RecordReader which reads from the test table.
    RecordReader reader = new RowRecordReader(context, table, provider);
    
    // Create an dummy output mutator for the RecordReader.
    TestOutput output = new TestOutput();
    try {reader.setup(output);}
    catch (ExecutionSetupException e) {Assert.fail("reader threw an exception");}
    
    // print out headers
    System.out.printf("\n%20s\n", table.getName());
    System.out.printf("%10s", "RowNumber");
    for (ValueVector v: table.getValueVectors()) {
      System.out.printf(" | %16s", v.getField().getName());
    }
    System.out.println();

    // Do for each record batch
    int rowNumber = 0;
    for (;;) {
      int count = reader.next();
      if (count == 0) break;
      
      // Do for each row in the batch
      for (int row=0; row<count; row++, rowNumber++) {
       
        // Display the row
        System.out.printf("%10d", rowNumber);
        for (ValueVector v: table.getValueVectors()) {
          System.out.printf(" | %16s", v.getAccessor().getObject(row));
        }
        System.out.println();
        
      }
    }
  }

  
  /** 
   * A dummy OutputMutator so we can examine the contents of the current batch 
   */
  static class TestOutput implements OutputMutator {
    List<ValueVector> vectors = new ArrayList<ValueVector>();

    public void addField(ValueVector vector) throws SchemaChangeException {
      vectors.add(vector); 
    }
    
    public Object get(int column, int row) {
      return vectors.get(column).getAccessor().getObject(row);
    }
     
    public void removeField(MaterializedField field) {}
    public void removeAllFields() {}
    public void setNewSchema() {}
  }
  
 
}
