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
package org.apache.drill.exec.store.ltsv;

import io.netty.buffer.DrillBuf;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.drill.shaded.guava.com.google.common.collect.Sets;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.Collection;
import java.util.List;
import java.util.Set;

public class LTSVRecordReader extends AbstractRecordReader {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LTSVRecordReader.class);

  private static final int MAX_RECORDS_PER_BATCH = 8096;

  private String inputPath;

  private FSDataInputStream fsStream;

  private BufferedReader reader;

  private DrillBuf buffer;

  private VectorContainerWriter writer;

  private LTSVFormatPluginConfig config;

  private int lineCount;

  public LTSVRecordReader(FragmentContext fragmentContext, Path path, DrillFileSystem fileSystem,
                          List<SchemaPath> columns, LTSVFormatPluginConfig config) throws OutOfMemoryException {
    try {
      this.fsStream = fileSystem.open(path);
      this.inputPath = path.toString();
      this.lineCount = 0;
      this.reader = new BufferedReader(new InputStreamReader(fsStream.getWrappedStream(), StandardCharsets.UTF_8));
      this.config = config;
      this.buffer = fragmentContext.getManagedBuffer();
      setColumns(columns);

    } catch (IOException e) {
      logger.debug("LTSV Plugin: " + e.getMessage());
    }
  }

  @Override
  protected Collection<SchemaPath> transformColumns(Collection<SchemaPath> projected) {
    Set<SchemaPath> transformed = Sets.newLinkedHashSet();
    if (!isStarQuery()) {
      for (SchemaPath column : projected) {
        transformed.add(column);
      }
    } else {
      transformed.add(SchemaPath.STAR_COLUMN);
    }
    return transformed;
  }

  public void setup(final OperatorContext context, final OutputMutator output) throws ExecutionSetupException {
    this.writer = new VectorContainerWriter(output);
  }

  public int next() {
    this.writer.allocate();
    this.writer.reset();

    int recordCount = 0;

    try {
      BaseWriter.MapWriter map = this.writer.rootAsMap();
      String line = null;

      while (recordCount < MAX_RECORDS_PER_BATCH && (line = this.reader.readLine()) != null) {
        lineCount++;

        // Skip empty lines
        if (line.trim().length() == 0) {
          continue;
        }

        List<String[]> fields = Lists.newArrayList();
        for (String field : line.split("\t")) {
          int index = field.indexOf(":");
          if (index <= 0) {
            throw new ParseException("Invalid LTSV format: " + inputPath + "\n" + lineCount + ":" + line, 0);
          }

          String fieldName = field.substring(0, index);
          String fieldValue = field.substring(index + 1);
          if (selectedColumn(fieldName)) {
            fields.add(new String[]{fieldName, fieldValue});
          }
        }

        if (fields.size() == 0) {
          continue;
        }

        this.writer.setPosition(recordCount);
        map.start();

        for (String[] field : fields) {
          byte[] bytes = field[1].getBytes(StandardCharsets.UTF_8);
          this.buffer = this.buffer.reallocIfNeeded(bytes.length);
          this.buffer.setBytes(0, bytes, 0, bytes.length);
          map.varChar(field[0]).writeVarChar(0, bytes.length, buffer);
        }

        map.end();
        recordCount++;
      }

      this.writer.setValueCount(recordCount);
      return recordCount;

    } catch (final Exception e) {
      String msg = "Failure while reading messages from LTSV. Recordreader was at record: " + lineCount;
      throw UserException.dataReadError(e).message(msg).build(logger);
    }
  }

  private boolean selectedColumn(String fieldName) {
    for (SchemaPath col : getColumns()) {
      if (col.equals(SchemaPath.STAR_COLUMN) || col.getRootSegment().getPath().equals(fieldName)) {
        return true;
      }
    }
    return false;
  }

  public void close() throws Exception {
    this.reader.close();
    this.fsStream.close();
  }

}
