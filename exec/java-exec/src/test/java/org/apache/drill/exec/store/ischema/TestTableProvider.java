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

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.TopLevelAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.vector.ValueVector;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Using a test table with two columns, create data and verify the values are in the record batch.
 */
public class TestTableProvider {
  
  @Test
  public void zeroRead() {
    readTestTable(0);
  }
  
  @Test
  public void oneRead() {
    readTestTable(1);
  }
  
  @Test
  public void smallRead() {
    readTestTable(10);
  }
  
  @Test
  @Ignore // due to out of heap space
  public void largeRead() {
    readTestTable(1024*1024);
  }
  
  
  /**
   * Read record batches from the test table and verify the contents.
   * @param nrRows - the total number of rows expected.
   */
  private void readTestTable(int nrRows) {
    
    // Mock up a context with a BufferAllocator
    FragmentContext context = mock(FragmentContext.class);
    when(context.getAllocator()).thenReturn(new TopLevelAllocator());
    
    // Create a RecordReader which reads from the test table.
    RecordReader reader = new RowRecordReader(context, new TestTable(), new TestProvider(nrRows));
    
    // Create an dummy output mutator for the RecordReader.
    TestOutput output = new TestOutput();
    try {reader.setup(output);}
    catch (ExecutionSetupException e) {Assert.fail("reader threw an exception");}

    // Do for each record batch
    int rowNumber = 0;
    for (;;) {
      int count = reader.next();
      if (count == 0) break;
      
      // Do for each row in the batch
      for (int row=0; row<count; row++, rowNumber++) {
        
        // Verify the row has an integer and string containing the row number
        int intValue = (int)output.get(1, row);
        String strValue = (String)output.get(0, row);
        Assert.assertEquals(rowNumber, intValue);
        Assert.assertEquals(rowNumber, Integer.parseInt(strValue));
      }
    }
  
  // Verify we read the correct number of rows.
  Assert.assertEquals(nrRows, rowNumber);
  }

  
  /**
   * Class to define the table we want to create. Two columns - string, integer
   */
  static class TestTable extends FixedTable {
    static final String tableName = "MOCK_TABLE";
    static final String fieldNames[] = {"STRING_COLUMM", "INTEGER_COLUMN"};
    static final MajorType fieldTypes[] = {VARCHAR, INT};
    TestTable() {
      super(tableName, fieldNames, fieldTypes);
    }
  }
  
  
  /**
   * Class to generate data for the table
   */
  static class TestProvider extends PipeProvider {
    int maxRows;
    TestProvider(int maxRows) {
      this.maxRows = maxRows;
    }
    void generateRows() {
      for (int rowNumber=0; rowNumber<maxRows; rowNumber++) {
        writeRow(Integer.toString(rowNumber), rowNumber);
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
