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
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.msgpack.MsgpackFormatPlugin.MsgpackFormatConfig;
import org.apache.drill.exec.store.msgpack.schema.MsgpackSchema;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.hadoop.fs.Path;

public class MsgpackRecordReader extends AbstractRecordReader {

  @SuppressWarnings("unused")
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MsgpackRecordReader.class);

  public static final long DEFAULT_ROWS_PER_BATCH = 0x4000;
  private VectorContainerWriter writer;
  private InputStream stream;
  private final DrillFileSystem fileSystem;
  private final FragmentContext fragmentContext;
  private final boolean learnSchema;
  private final boolean useSchema;
  private final MsgpackReaderContext context = new MsgpackReaderContext();
  private MsgpackReader messageReader;
  private boolean hasMore = true;
  private final MsgpackSchema msgpackSchema;

  private boolean unionEnabled = false; // jccote, not sure what enabling union means for msgpack reader?

  /**
   * Create a msgpack Record Reader that uses a file based input stream.
   *
   * @param fragmentContext
   * @param inputPath
   * @param fileSystem
   * @param columns
   *                          pathnames of columns/subfields to read
   * @throws OutOfMemoryException
   */
  public MsgpackRecordReader(MsgpackFormatConfig config, final FragmentContext fragmentContext, final String inputPath,
      final DrillFileSystem fileSystem, final List<SchemaPath> columns) {

    Preconditions.checkArgument((inputPath != null), "InputPath must be set.");
    this.fileSystem = fileSystem;
    this.fragmentContext = fragmentContext;
    context.hadoopPath = new Path(inputPath);
    context.readBinaryAsString = config.isReadBinaryAsString();
    context.lenient = config.isLenient();
    context.printToConsole = config.isPrintToConsole();
    learnSchema = config.isLearnSchema();
    useSchema = config.isUseSchema();
    msgpackSchema = new MsgpackSchema(fileSystem, context.hadoopPath.getParent());
    setColumns(columns);
  }

  @Override
  public String toString() {
    return super.toString() + context.toString() + ", ...]";
  }

  @Override
  public void setup(final OperatorContext operatorContext, final OutputMutator output) throws ExecutionSetupException {
    try {
      stream = fileSystem.openPossiblyCompressedStream(context.hadoopPath);
      writer = new VectorContainerWriter(output, unionEnabled);
      messageReader = new MsgpackReader(stream, Lists.newArrayList(getColumns()), isSkipQuery());
      context.workBuf = fragmentContext.getManagedBuffer();
      messageReader.setup(context);
      if (useSchema) {
        msgpackSchema.load();
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

    writer.allocate();
    writer.reset();
    context.recordCount = 0;
    if (!hasMore) {
      return context.recordCount;
    }
    while (context.recordCount < DEFAULT_ROWS_PER_BATCH) {
      try {
        writer.setPosition(context.recordCount);
        hasMore = messageReader.write(writer, msgpackSchema.getSchema());
        if (!hasMore) {
          break;
        } else {
          context.recordCount++;
        }
      } catch (MsgpackParsingException e) {
        if (!context.lenient) {
          context.handleAndRaise("Error parsing msgpack", e);
        }
        ++context.parseErrorCount;
        context.parseWarn(e);
      } catch (IOException e) {
        context.handleAndRaise("Error parsing msgpack", e);
      }
    }

    if (learnSchema) {
      if (isStarQuery()) {
        try {
          msgpackSchema.learnSchema(writer);
        } catch (Exception e) {
          context.handleAndRaise("Error merging msgpack schema", e);
        }
      } else {
        context.warn("Msgpack reader is in learning mode but the query is not a select star. Learning skipped.");
      }
    }

    if (useSchema) {
      // Since we know the schema of the msgpack records we will create
      // all the fields even if that means they are empty.
      try {
        msgpackSchema.applySchemaIfAny(writer);
      } catch (Exception e) {
        context.handleAndRaise("Error applying msgpack schema to writer.", e);
      }
    } else {
      messageReader.ensureAtLeastOneField(writer);
    }

    writer.setValueCount(context.recordCount);
    context.updateRunningCount();
    return context.recordCount;
  }

  @Override
  public void close() throws Exception {
    if (stream != null) {
      stream.close();
    }
  }
}
