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
package org.apache.drill.exec.store.text;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.RepeatedVarCharVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.List;

public class DrillTextRecordReader implements RecordReader {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillTextRecordReader.class);

  private org.apache.hadoop.mapred.RecordReader<LongWritable, Text> reader;
  private List<ValueVector> vectors = Lists.newArrayList();
  private byte delimiter;
  private int targetRecordCount;
  private FieldReference ref = new FieldReference("columns");
  private FragmentContext context;
  private RepeatedVarCharVector vector;
  private List<Integer> columnIds = Lists.newArrayList();
  private LongWritable key;
  private Text value;
  private int numCols = 0;
  private boolean redoRecord = false;

  public DrillTextRecordReader(FileSplit split, FragmentContext context, char delimiter, List<SchemaPath> columns) {
    this.context = context;
    this.delimiter = (byte) delimiter;
    if(columns != null) {
      for (SchemaPath path : columns) {
        assert path.getRootSegment().isNamed();
        Preconditions.checkArgument(path.getRootSegment().getChild().isArray(),"Selected column must be an array index");
        int index = path.getRootSegment().getChild().getArraySegment().getIndex();
        columnIds.add(index);
      }
    }
    targetRecordCount = context.getConfig().getInt(ExecConstants.TEXT_LINE_READER_BATCH_SIZE);
    numCols = columnIds.size();
    TextInputFormat inputFormat = new TextInputFormat();
    JobConf job = new JobConf();
    job.setInt("io.file.buffer.size", context.getConfig().getInt(ExecConstants.TEXT_LINE_READER_BUFFER_SIZE));
    job.setInputFormat(inputFormat.getClass());
    try {
      reader = inputFormat.getRecordReader(split, job, Reporter.NULL);
      key = reader.createKey();
      value = reader.createValue();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    output.removeAllFields();
    MaterializedField field = MaterializedField.create(ref, Types.repeated(TypeProtos.MinorType.VARCHAR));
    vector = new RepeatedVarCharVector(field, context.getAllocator());
    try {
      output.addField(vector);
      output.setNewSchema();
    } catch (SchemaChangeException e) {
      throw new ExecutionSetupException(e);
    }
  }

  @Override
  public int next() {
    AllocationHelper.allocate(vector, targetRecordCount, 50);
    try {
      int recordCount = 0;
      while (redoRecord || (recordCount < targetRecordCount && reader.next(key, value))) {
        redoRecord = false;
        int start;
        int end = -1;
        int p = 0;
        int i = 0;
        vector.getMutator().startNewGroup(recordCount);
        while (end < value.getLength() - 1) {
          if(numCols > 0 && p >= numCols) {
            break;
          }
          start = end;
          end = find(value, delimiter, start + 1);
          if (end == -1) {
            end = value.getLength();
          }
          if (numCols > 0 && i++ < columnIds.get(p)) {
            if (!vector.getMutator().addSafe(recordCount, value.getBytes(), start + 1, start + 1)) {
              redoRecord = true;
              vector.getMutator().setValueCount(recordCount);
              return recordCount;
            }
            continue;
          }
          p++;
          if (!vector.getMutator().addSafe(recordCount, value.getBytes(), start + 1, end - start - 1)) {
            redoRecord = true;
            vector.getMutator().setValueCount(recordCount);
            return recordCount;
          }
        }
        recordCount++;
      }
      for (ValueVector v : vectors) {
        v.getMutator().setValueCount(recordCount);
      }
      vector.getMutator().setValueCount(recordCount);
      return recordCount;
    } catch (IOException e) {
      cleanup();
      throw new DrillRuntimeException(e);
    }
  }

  public int find(Text text, byte what, int start) {
    int len = text.getLength();
    int p = start;
    byte[] bytes = text.getBytes();
    while (p < len) {
      if (bytes[p] == what) {
        return p;
      }
      p++;
    }
    return -1;
  }

  @Override
  public void cleanup() {
    try {
    reader.close();
    } catch (IOException e) {
      logger.warn("Exception closing reader: {}", e);
    }
  }
}
