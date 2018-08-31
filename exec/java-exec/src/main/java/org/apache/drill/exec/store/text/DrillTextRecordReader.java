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
package org.apache.drill.exec.store.text;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.drill.shaded.guava.com.google.common.base.Predicate;
import org.apache.drill.shaded.guava.com.google.common.collect.Iterables;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.vector.RepeatedVarCharVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;

import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;

public class DrillTextRecordReader extends AbstractRecordReader {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillTextRecordReader.class);

  private static final String COL_NAME = "columns";

  private org.apache.hadoop.mapred.RecordReader<LongWritable, Text> reader;
  private final List<ValueVector> vectors = Lists.newArrayList();
  private byte delimiter;
  private FieldReference ref = new FieldReference(COL_NAME);
  private RepeatedVarCharVector vector;
  private List<Integer> columnIds = Lists.newArrayList();
  private LongWritable key;
  private Text value;
  private int numCols = 0;
  private FileSplit split;
  private long totalRecordsRead;

  public DrillTextRecordReader(FileSplit split, Configuration fsConf, FragmentContext context,
      char delimiter, List<SchemaPath> columns) {
    this.delimiter = (byte) delimiter;
    this.split = split;
    setColumns(columns);

    if (!isStarQuery()) {
      String pathStr;
      for (SchemaPath path : columns) {
        assert path.getRootSegment().isNamed();
        pathStr = path.getRootSegment().getPath();
        Preconditions.checkArgument(COL_NAME.equals(pathStr) || (SchemaPath.DYNAMIC_STAR.equals(pathStr) && path.getRootSegment().getChild() == null),
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

    TextInputFormat inputFormat = new TextInputFormat();
    JobConf job = new JobConf(fsConf);
    job.setInt("io.file.buffer.size", context.getConfig().getInt(ExecConstants.TEXT_LINE_READER_BUFFER_SIZE));
    job.setInputFormat(inputFormat.getClass());
    try {
      reader = inputFormat.getRecordReader(split, job, Reporter.NULL);
      key = reader.createKey();
      value = reader.createValue();
      totalRecordsRead = 0;
    } catch (Exception e) {
      handleAndRaise("Failure in creating record reader", e);
    }
  }

  @Override
  protected List<SchemaPath> getDefaultColumnsToRead() {
    return DEFAULT_TEXT_COLS_TO_READ;
  }

  @Override
  public boolean isStarQuery() {
    return super.isStarQuery() || Iterables.tryFind(getColumns(), new Predicate<SchemaPath>() {
      private final SchemaPath COLUMNS = SchemaPath.getSimplePath("columns");
      @Override
      public boolean apply(@Nullable SchemaPath path) {
        return path.equals(COLUMNS);
      }
    }).isPresent();
  }

  @Override
  public void setup(OperatorContext context, OutputMutator output) throws ExecutionSetupException {
    MaterializedField field = MaterializedField.create(ref.getAsNamePart().getName(), Types.repeated(TypeProtos.MinorType.VARCHAR));
    try {
      vector = output.addField(field, RepeatedVarCharVector.class);
    } catch (Exception e) {
      handleAndRaise("Failure in setting up reader", e);
    }
  }

  protected void handleAndRaise(String s, Exception e) {
    String message = "Error in text record reader.\nMessage: " + s +
      "\nSplit information:\n\tPath: " + split.getPath() +
      "\n\tStart: " + split.getStart() +
      "\n\tLength: " + split.getLength();
    throw new DrillRuntimeException(message, e);
  }

  @Override
  public int next() {
//    logger.debug("vector value capacity {}", vector.getValueCapacity());
//    logger.debug("vector byte capacity {}", vector.getByteCapacity());
    int batchSize = 0;
    try {
      int recordCount = 0;
      final RepeatedVarCharVector.Mutator mutator = vector.getMutator();
      while (recordCount < Character.MAX_VALUE && batchSize < 200*1000 && reader.next(key, value)) {
        int start;
        int end = -1;

        // index of the scanned field
        int p = 0;
        int i = 0;
        mutator.startNewValue(recordCount);
        // Process each field in this line
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
            mutator.addSafe(recordCount, value.getBytes(), start + 1, 0);
            continue;
          }
          p++;
          mutator.addSafe(recordCount, value.getBytes(), start + 1, end - start - 1);
          batchSize += end - start;
        }
        recordCount++;
        totalRecordsRead++;
      }
      for (final ValueVector v : vectors) {
        v.getMutator().setValueCount(recordCount);
      }
      mutator.setValueCount(recordCount);
      // logger.debug("text scan batch size {}", batchSize);
      return recordCount;
    } catch(Exception e) {
      close();
      handleAndRaise("Failure while parsing text. Parser was at record: " + (totalRecordsRead + 1), e);
    }

    // this is never reached
    return 0;
  }

  /**
   * Returns the index within the text of the first occurrence of delimiter, starting the search at the specified index.
   *
   * @param  text  the text being searched
   * @param  delimiter the delimiter
   * @param  start the index to start searching
   * @return      the first occurrence of delimiter, starting the search at the specified index
   */
  public int find(Text text, byte delimiter, int start) {
    int len = text.getLength();
    int p = start;
    byte[] bytes = text.getBytes();
    boolean inQuotes = false;
    while (p < len) {
      if ('\"' == bytes[p]) {
        inQuotes = !inQuotes;
      }
      if (!inQuotes && bytes[p] == delimiter) {
        return p;
      }
      p++;
    }
    return -1;
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
    return "DrillTextRecordReader[File=" + split.getPath()
        + ", Record=" + (totalRecordsRead + 1)
        + ", Start=" + split.getStart()
        + ", Length=" + split.getLength()
        + "]";
  }
}
