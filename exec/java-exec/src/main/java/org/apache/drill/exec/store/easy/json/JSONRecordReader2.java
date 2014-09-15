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
package org.apache.drill.exec.store.easy.json;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.memory.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.vector.BaseValueVector;
import org.apache.drill.exec.vector.complex.fn.JsonReaderWithState;
import org.apache.drill.exec.vector.complex.fn.JsonRecordSplitter;
import org.apache.drill.exec.vector.complex.fn.UTF8JsonRecordSplitter;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.fasterxml.jackson.core.JsonParseException;

public class JSONRecordReader2 extends AbstractRecordReader {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JSONRecordReader2.class);

  private OutputMutator mutator;
  private VectorContainerWriter writer;
  private Path hadoopPath;
  private FileSystem fileSystem;
  private InputStream stream;
  private JsonReaderWithState jsonReader;
  private int recordCount;
  private FragmentContext fragmentContext;
  private OperatorContext operatorContext;
  private List<SchemaPath> columns;
  private boolean enableAllTextMode;

  public JSONRecordReader2(FragmentContext fragmentContext, String inputPath, FileSystem fileSystem,
                          List<SchemaPath> columns) throws OutOfMemoryException {
    this.hadoopPath = new Path(inputPath);
    this.fileSystem = fileSystem;
    this.fragmentContext = fragmentContext;
    this.columns = columns;
    enableAllTextMode = fragmentContext.getDrillbitContext().getOptionManager().getOption("store.json.all_text_mode").bool_val;
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    try{
      stream = fileSystem.open(hadoopPath);
      JsonRecordSplitter splitter = new UTF8JsonRecordSplitter(stream);
      this.writer = new VectorContainerWriter(output);
      this.mutator = output;
      jsonReader = new JsonReaderWithState(splitter, fragmentContext.getManagedBuffer(), columns, enableAllTextMode);
    }catch(Exception e){
      handleAndRaise("Failure reading JSON file.", e);
    }
  }

  protected void handleAndRaise(String msg, Exception e) {
    StringBuilder sb = new StringBuilder();
    sb.append(msg).append(" - Parser was at record: ").append(recordCount+1);
    if (e instanceof JsonParseException) {
      JsonParseException ex = JsonParseException.class.cast(e);
      sb.append(" column: ").append(ex.getLocation().getColumnNr());
    }
    throw new DrillRuntimeException(sb.toString(), e);
  }


  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  public void setOperatorContext(OperatorContext operatorContext) {
    this.operatorContext = operatorContext;
  }

  @Override
  public int next() {
    writer.allocate();
    writer.reset();

    recordCount = 0;

    try{
      outside: while(true && recordCount < BaseValueVector.INITIAL_VALUE_ALLOCATION){
        writer.setPosition(recordCount);

        switch(jsonReader.write(writer)){
        case WRITE_SUCCEED:
          recordCount++;
          break;

        case NO_MORE:
          break outside;

        case WRITE_FAILED:
          if (recordCount == 0) {
            throw new DrillRuntimeException("Record is too big to fit into allocated ValueVector");
          }
          break outside;
        };
      }
      for (SchemaPath sp :jsonReader.getNullColumns() ) {
        PathSegment root = sp.getRootSegment();
        BaseWriter.MapWriter fieldWriter = writer.rootAsMap();
        while (root.getChild() != null && ! root.getChild().isArray()) {
          fieldWriter = fieldWriter.map(root.getNameSegment().getPath());
          root = root.getChild();
        }
        fieldWriter.integer(root.getNameSegment().getPath());
      }

      writer.setValueCount(recordCount);
      return recordCount;

    } catch (JsonParseException e) {
      handleAndRaise("Error parsing JSON.", e);
    } catch (IOException e) {
      handleAndRaise("Error reading JSON.", e);
    }
    // this is never reached
    return 0;
  }

  @Override
  public void cleanup() {
    try {
      stream.close();
    } catch (IOException e) {
      logger.warn("Failure while closing stream.", e);
    }
  }

}
