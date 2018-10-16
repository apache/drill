/*
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
import java.util.concurrent.TimeUnit;
import java.security.PrivilegedExceptionAction;

import org.apache.drill.shaded.guava.com.google.common.base.Stopwatch;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.util.ImpersonationUtil;
import org.apache.drill.exec.vector.NullableVarBinaryVector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileAsBinaryInputFormat;
import org.apache.hadoop.security.UserGroupInformation;


public class SequenceFileRecordReader extends AbstractRecordReader {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SequenceFileRecordReader.class);

  private static final int PER_BATCH_RECORD_COUNT = 4096;
  private static final int PER_BATCH_BYTES = 256*1024;

  private static final MajorType KEY_TYPE = Types.optional(TypeProtos.MinorType.VARBINARY);
  private static final MajorType VALUE_TYPE = Types.optional(TypeProtos.MinorType.VARBINARY);

  private final String keySchema = "binary_key";
  private final String valueSchema = "binary_value";

  private NullableVarBinaryVector keyVector;
  private NullableVarBinaryVector valueVector;
  private final FileSplit split;
  private org.apache.hadoop.mapred.RecordReader<BytesWritable, BytesWritable> reader;
  private final BytesWritable key = new BytesWritable();
  private final BytesWritable value = new BytesWritable();
  private final DrillFileSystem dfs;
  private final String queryUserName;
  private final String opUserName;

  public SequenceFileRecordReader(final FileSplit split,
                                  final DrillFileSystem dfs,
                                  final String queryUserName,
                                  final String opUserName) {
    final List<SchemaPath> columns = new ArrayList<>();
    columns.add(SchemaPath.getSimplePath(keySchema));
    columns.add(SchemaPath.getSimplePath(valueSchema));
    setColumns(columns);
    this.dfs = dfs;
    this.split = split;
    this.queryUserName = queryUserName;
    this.opUserName = opUserName;
  }

  @Override
  protected boolean isSkipQuery() {
    return false;
  }

  private org.apache.hadoop.mapred.RecordReader<BytesWritable, BytesWritable> getRecordReader(
    final InputFormat<BytesWritable, BytesWritable> inputFormat,
    final JobConf jobConf) throws ExecutionSetupException {
    try {
      final UserGroupInformation ugi = ImpersonationUtil.createProxyUgi(this.opUserName, this.queryUserName);
      return ugi.doAs(new PrivilegedExceptionAction<org.apache.hadoop.mapred.RecordReader<BytesWritable, BytesWritable>>() {
        @Override
        public org.apache.hadoop.mapred.RecordReader<BytesWritable, BytesWritable> run() throws Exception {
          return inputFormat.getRecordReader(split, jobConf, Reporter.NULL);
        }
      });
    } catch (IOException | InterruptedException e) {
      throw new ExecutionSetupException(
        String.format("Error in creating sequencefile reader for file: %s, start: %d, length: %d",
          split.getPath(), split.getStart(), split.getLength()), e);
    }
  }

  @Override
  public void setup(OperatorContext context, OutputMutator output) throws ExecutionSetupException {
    final SequenceFileAsBinaryInputFormat inputFormat = new SequenceFileAsBinaryInputFormat();
    final JobConf jobConf = new JobConf(dfs.getConf());
    jobConf.setInputFormat(inputFormat.getClass());
    reader = getRecordReader(inputFormat, jobConf);
    final MaterializedField keyField = MaterializedField.create(keySchema, KEY_TYPE);
    final MaterializedField valueField = MaterializedField.create(valueSchema, VALUE_TYPE);
    try {
      keyVector = output.addField(keyField, NullableVarBinaryVector.class);
      valueVector = output.addField(valueField, NullableVarBinaryVector.class);
    } catch (SchemaChangeException sce) {
      throw new ExecutionSetupException("Error in setting up sequencefile reader.", sce);
    }
  }

  @Override
  public int next() {
    final Stopwatch watch = Stopwatch.createStarted();
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
      logger.debug("Read {} records in {} ms", recordCount, watch.elapsed(TimeUnit.MILLISECONDS));
      return recordCount;
    } catch (IOException ioe) {
      close();
      throw UserException.dataReadError(ioe).addContext("File Path", split.getPath().toString()).build(logger);
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

  @Override
  public String toString() {
    long position = -1L;
    try {
      if (reader != null) {
        position = reader.getPos();
      }
    } catch (IOException e) {
      logger.trace("Unable to obtain reader position.", e);
    }
    return "SequenceFileRecordReader[File=" + split.getPath()
        + ", Position=" + position
        + "]";
  }
}
