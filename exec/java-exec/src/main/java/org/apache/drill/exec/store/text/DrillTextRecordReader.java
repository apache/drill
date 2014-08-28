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

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.RepeatedVarCharVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class DrillTextRecordReader extends AbstractRecordReader {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillTextRecordReader.class);

  static final String COL_NAME = "columns";

  private org.apache.hadoop.mapred.RecordReader<LongWritable, Text> reader;
  private List<ValueVector> vectors = Lists.newArrayList();
  private byte delimiter;
  private int targetRecordCount;
  private FieldReference ref = new FieldReference(COL_NAME);
  private FragmentContext fragmentContext;
  private OperatorContext operatorContext;
  private RepeatedVarCharVector vector;
  private List<Integer> columnIds = Lists.newArrayList();
  private LongWritable key;
  private Text value;
  private int numCols = 0;
  private boolean redoRecord = false;

  public DrillTextRecordReader(FileSplit split, FragmentContext context, char delimiter, List<SchemaPath> columns) {
    this.fragmentContext = context;
    this.delimiter = (byte) delimiter;
    setColumns(columns);

    if (!isStarQuery()) {
      String pathStr;
      for (SchemaPath path : columns) {
        assert path.getRootSegment().isNamed();
        pathStr = path.getRootSegment().getPath();
        Preconditions.checkArgument(pathStr.equals(COL_NAME) || (pathStr.equals("*") && path.getRootSegment().getChild() == null),
            "Selected column(s) must have name 'columns' or must be plain '*'");

        if (path.getRootSegment().getChild() != null) {
          Preconditions.checkArgument(path.getRootSegment().getChild().isArray(), "Selected column must be an array index");
          int index = path.getRootSegment().getChild().getArraySegment().getIndex();
          columnIds.add(index);
        }
      }
      Collections.sort(columnIds);
      numCols = columnIds.size();
    }
    targetRecordCount = context.getConfig().getInt(ExecConstants.TEXT_LINE_READER_BATCH_SIZE);

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

  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  public void setOperatorContext(OperatorContext operatorContext) {
    this.operatorContext = operatorContext;
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    MaterializedField field = MaterializedField.create(ref, Types.repeated(TypeProtos.MinorType.VARCHAR));
    try {
      vector = output.addField(field, RepeatedVarCharVector.class);
    } catch (SchemaChangeException e) {
      throw new ExecutionSetupException(e);
    }
  }

  @Override
  public int next() {
    logger.debug("vector value capacity {}", vector.getValueCapacity());
    logger.debug("vector byte capacity {}", vector.getByteCapacity());
    int batchSize = 0;
    try {
      int recordCount = 0;
      while (redoRecord || (batchSize < 200*1000 && reader.next(key, value))) {
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
          if (delimiter == '\n') {
            end = value.getLength();
          } else {
            end = find(value, delimiter, start + 1);
            if (end == -1) {
              end = value.getLength();
            }
          }
          if (numCols > 0 && i++ < columnIds.get(p)) {
            if (!vector.getMutator().addSafe(recordCount, value.getBytes(), start + 1, 0)) {
              redoRecord = true;
              vector.getMutator().setValueCount(recordCount);
              logger.debug("text scan batch size {}", batchSize);
              return recordCount;
            }
            continue;
          }
          p++;
          if (!vector.getMutator().addSafe(recordCount, value.getBytes(), start + 1, end - start - 1)) {
            redoRecord = true;
            vector.getMutator().setValueCount(recordCount);
            logger.debug("text scan batch size {}", batchSize);
            return recordCount;
          }
          batchSize += end - start;
        }
        recordCount++;
      }
      for (ValueVector v : vectors) {
        v.getMutator().setValueCount(recordCount);
      }
      vector.getMutator().setValueCount(recordCount);
      logger.debug("text scan batch size {}", batchSize);
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
    boolean inQuotes = false;
    while (p < len) {
      if ('\"' == bytes[p]) {
        inQuotes = !inQuotes;
      }
      if (!inQuotes && bytes[p] == what) {
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
