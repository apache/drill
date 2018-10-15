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
package org.apache.drill.exec.store.msgpack;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.msgpack.BaseMsgpackReader.ReadState;
import org.apache.drill.exec.store.msgpack.MsgpackFormatPlugin.MsgpackFormatConfig;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.hadoop.fs.Path;

public class MsgpackRecordReader extends AbstractRecordReader {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MsgpackRecordReader.class);

  public static final long DEFAULT_ROWS_PER_BATCH = 0x4000;

  private VectorContainerWriter writer;

  private InputStream stream;
  private final DrillFileSystem fileSystem;
  private final FragmentContext fragmentContext;
  private final boolean learnSchema;
  private final boolean useSchema;
  private Path schemaLocation;
  private ReadState write = null;
  private MsgpackSchemaWriter schemaWriter = new MsgpackSchemaWriter();

  private MsgpackReaderContext context = new MsgpackReaderContext();
  private BaseMsgpackReader messageReader;

  private boolean unionEnabled = false; // ????

  /**
   * Create a msgpack Record Reader that uses a file based input stream.
   *
   * @param fragmentContext
   * @param inputPath
   * @param fileSystem
   * @param columns         pathnames of columns/subfields to read
   * @throws OutOfMemoryException
   */
  public MsgpackRecordReader(MsgpackFormatConfig config, final FragmentContext fragmentContext, final String inputPath,
      final DrillFileSystem fileSystem, final List<SchemaPath> columns) {

    Preconditions.checkArgument((inputPath != null), "InputPath must be set.");
    context.hadoopPath = new Path(inputPath);

    this.fileSystem = fileSystem;
    this.fragmentContext = fragmentContext;
    this.context.readBinaryAsString = config.isReadBinaryAsString();
    this.context.lenient = config.isLenient();
    this.context.printToConsole = config.isPrintToConsole();
    this.learnSchema = config.isLearnSchema();
    this.useSchema = config.isUseSchema();
    this.schemaLocation = new Path(context.hadoopPath.getParent(), ".schema.proto");
    setColumns(columns);
  }

  @Override
  public String toString() {
    return super.toString() + context.toString() + ", ...]";
  }

  @Override
  public void setup(final OperatorContext operatorContext, final OutputMutator output) throws ExecutionSetupException {
    try {
      this.stream = fileSystem.openPossiblyCompressedStream(context.hadoopPath);
      this.writer = new VectorContainerWriter(output, unionEnabled);
      if (isSkipQuery()) {
        this.messageReader = new CountingMsgpackReader(stream, context);
      } else {
        this.messageReader = new MsgpackReader(stream, context, fragmentContext.getManagedBuffer(),
            Lists.newArrayList(getColumns()));
      }
    } catch (final Exception e) {
      context.handleAndRaise("Failure reading mgspack file", e);
    }
  }

  @Override
  protected List<SchemaPath> getDefaultColumnsToRead() {
    return ImmutableList.of();
  }

  @Override
  public int next() {

    MsgpackSchema msgpackSchema = new MsgpackSchema(fileSystem);
    MaterializedField schema = null;
    try {
      if (!this.learnSchema && this.useSchema) {
        schema = msgpackSchema.load(schemaLocation);
      }
    } catch (IOException e) {
      context.warn("Failed to load schema file: " + schemaLocation + " " + e.getMessage());
    }

    writer.allocate();
    writer.reset();
    context.recordCount = 0;
    if (write == ReadState.MSG_RECORD_PARSE_EOF_ERROR) {
      return context.recordCount;
    }
    outside: while (context.recordCount < DEFAULT_ROWS_PER_BATCH) {
      try {
        writer.setPosition(context.recordCount);
        write = messageReader.write(writer, schema);
        if (write == ReadState.WRITE_SUCCEED) {
          context.recordCount++;
        } else if (write == ReadState.MSG_RECORD_PARSE_ERROR || write == ReadState.MSG_RECORD_PARSE_EOF_ERROR) {
          if (context.lenient == false) {
            context.handleAndRaise("Error parsing msgpack", new Exception("Failed to parse record."));
          }
          ++context.parseErrorCount;
          context.parseWarn();
          if (write == ReadState.MSG_RECORD_PARSE_EOF_ERROR) {
            break outside;
          }
        } else { // END_OF_STREAM
          break outside;
        }
      } catch (IOException ex) {
        context.handleAndRaise("Error parsing msgpack file.", ex);
      }
    }

    if (learnSchema) {
      learnSchema();
    }
    // Since we know the schema of the msgpack records we will create
    // all the fields even if that means they are empty.
    if (learnSchema || useSchema) {
      applySchemaIfAny();
    } else {
      messageReader.ensureAtLeastOneField(writer);
    }

    writer.setValueCount(context.recordCount);
    updateRunningCount();
    return context.recordCount;

  }

  private void applySchemaIfAny() {
    try {
      MsgpackSchema msgpackSchema = new MsgpackSchema(fileSystem);
      MaterializedField schema = msgpackSchema.load(schemaLocation);
      if (schema != null) {
        schemaWriter.applySchema(schema, writer);
      }
    } catch (Exception e) {
      context.handleAndRaise("Error applying msgpack schema to writer.", e);
    }
  }

  private void learnSchema() {
    if (this.isStarQuery()) {
      MsgpackSchema msgpackSchema = new MsgpackSchema(fileSystem);
      try {
        MaterializedField previous = msgpackSchema.load(schemaLocation);
        if (previous != null) {
          MaterializedField current = writer.getMapVector().getField();
          MaterializedField merged = msgpackSchema.merge(previous, current);
          msgpackSchema.save(merged, schemaLocation);
        } else {
          MaterializedField current = writer.getMapVector().getField();
          msgpackSchema.save(current, schemaLocation);
        }
      } catch (Exception e) {
        context.handleAndRaise("Error merging msgpack schema", e);
      }
    } else {
      context.warn("Msgpack reader is in learning mode but the query is not a select star. Learning skipped.");
    }
  }

  private void updateRunningCount() {
    context.runningRecordCount += context.recordCount;
  }

  @Override
  public void close() throws Exception {
    if (stream != null) {
      stream.close();
    }
  }
}
