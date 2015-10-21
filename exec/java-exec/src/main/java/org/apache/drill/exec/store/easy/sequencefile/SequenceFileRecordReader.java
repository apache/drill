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
package org.apache.drill.exec.store.easy.sequencefile;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import com.google.common.base.Stopwatch;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.vector.NullableVarBinaryVector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileAsBinaryInputFormat;


public class SequenceFileRecordReader extends AbstractRecordReader {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SequenceFileRecordReader.class);

  private static final int PER_BATCH_RECORD_COUNT = 4096;
  private static final int PER_BATCH_BYTES = 256*1024;

  private static final MajorType KEY_TYPE = Types.optional(TypeProtos.MinorType.VARBINARY);
  private static final MajorType VALUE_TYPE = Types.optional(TypeProtos.MinorType.VARBINARY);

  private final SchemaPath keySchema = SchemaPath.getSimplePath("binary_key");
  private final SchemaPath valueSchema = SchemaPath.getSimplePath("binary_value");

  private NullableVarBinaryVector keyVector;
  private NullableVarBinaryVector valueVector;
  private FileSplit split;
  private org.apache.hadoop.mapred.RecordReader<BytesWritable, BytesWritable> reader;
  private BytesWritable key = new BytesWritable();
  private BytesWritable value = new BytesWritable();

  public SequenceFileRecordReader(final FileSplit split,
                                  final Configuration fsConf) {
    final List<SchemaPath> columns = new ArrayList();
    columns.add(keySchema);
    columns.add(valueSchema);
    setColumns(columns);
    SequenceFileAsBinaryInputFormat inputFormat = new SequenceFileAsBinaryInputFormat();
    this.split = split;
    JobConf jobConf = new JobConf(fsConf);
    jobConf.setInputFormat(inputFormat.getClass());
    try {
      this.reader = inputFormat.getRecordReader(split, jobConf, Reporter.NULL);
    } catch (IOException ioe) {
      throw new DrillRuntimeException(
        String.format("Error in creating sequencefile reader for file: %s, start: %d, length: %d",
          split.getPath(), split.getStart(), split.getLength()), ioe);
    }
  }

  @Override
  protected boolean isSkipQuery() {
    return false;
  }

  @Override
  public void setup(OperatorContext context, OutputMutator output) throws ExecutionSetupException {
    MaterializedField keyField = MaterializedField.create(keySchema, KEY_TYPE);
    MaterializedField valueField = MaterializedField.create(valueSchema, VALUE_TYPE);
    try {
      keyVector = output.addField(keyField, NullableVarBinaryVector.class);
      valueVector = output.addField(valueField, NullableVarBinaryVector.class);
    } catch (Exception e) {
      throw new DrillRuntimeException(String.format("Error in setting up sequencefile reader."), e);
    }
  }

  @Override
  public int next() {
    Stopwatch watch = new Stopwatch();
    watch.start();
    if (keyVector != null) {
      keyVector.clear();
      keyVector.allocateNew();
    }
    if (valueVector != null) {
      valueVector.clear();
      valueVector.allocateNew();
    }
    int recordCount = 0;
    int batchSize = 0;
    try {
      while (recordCount < PER_BATCH_RECORD_COUNT && batchSize < PER_BATCH_BYTES && reader.next(key, value)) {
        keyVector.getMutator().setSafe(recordCount, key.getBytes(), 0, key.getLength());
        valueVector.getMutator().setSafe(recordCount, value.getBytes(), 0, value.getLength());
        batchSize += (key.getLength() + value.getLength());
        ++recordCount;
      }
      keyVector.getMutator().setValueCount(recordCount);
      valueVector.getMutator().setValueCount(recordCount);
      return recordCount;
    } catch (IOException ioe) {
      close();
      throw new DrillRuntimeException(String.format("Error parsing record from sequence file %s", split.getPath()),
        ioe);
    }
  }

  @Override
  public void close() {
    try {
      if (reader != null) {
        reader.close();
        reader = null;
      }
    } catch (IOException e) {
      logger.warn("Exception closing reader: {}", e);
    }
  }
}