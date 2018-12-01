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
  private final MsgpackFormatConfig config;
  private final Path filePath;
  private final DrillFileSystem fileSystem;
  private final FragmentContext fragmentContext;
  private final MsgpackSchema msgpackSchema;
  private VectorContainerWriter writer;
  private InputStream stream;
  private MsgpackReaderContext context;
  private MsgpackReader msgpackReader;
  private boolean hasMore = true;

  private boolean unionEnabled = false; // TODO: not sure what enabling union means for msgpack reader?

  /**
   * Create a msgpack Record Reader that uses a file based input stream.
   *
   *
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
    this.filePath = new Path(inputPath);
    this.fileSystem = fileSystem;
    this.fragmentContext = fragmentContext;
    this.config = config;
    this.msgpackSchema = new MsgpackSchema(fileSystem, filePath.getParent());
    setColumns(columns);
  }

  @Override
  public void setup(final OperatorContext operatorContext, final OutputMutator output) throws ExecutionSetupException {
    try {
      stream = fileSystem.openPossiblyCompressedStream(filePath);
      writer = new VectorContainerWriter(output, unionEnabled);
      boolean hasSchema = false;
      if (config.isUseSchema()) {
        msgpackSchema.load();
        if (msgpackSchema.getSchema() != null) {
          hasSchema = true;
        }
      }
      context = new MsgpackReaderContext(filePath, config, hasSchema);
      msgpackReader = new MsgpackReader(stream, Lists.newArrayList(getColumns()), isSkipQuery());
      msgpackReader.setup(context, fragmentContext.getManagedBuffer());
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
    context.resetRecordCount();
    if (!hasMore) {
      return context.getRecordCount();
    }
    while (context.getRecordCount() < DEFAULT_ROWS_PER_BATCH) {
      try {
        writer.setPosition(context.getRecordCount());
        context.getFieldPathTracker().reset();
        hasMore = msgpackReader.write(writer, msgpackSchema.getTupleMetadata());
        if (!hasMore) {
          break;
        } else {
          context.incrementRecordCount();
        }
      } catch (MsgpackParsingException e) {
        if (!context.isLenient()) {
          context.handleAndRaise("Error parsing msgpack", e);
        }
        context.incrementParseErrorCount();
        context.parseWarn(e);
      } catch (IOException e) {
        context.handleAndRaise("Error parsing msgpack", e);
      }
    }

    if (config.isLearnSchema()) {
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

    if (config.isUseSchema()) {
      // Since we know the schema of the msgpack records we will create
      // all the fields even if that means they are empty.
      try {
        msgpackSchema.applySchema(writer);
      } catch (Exception e) {
        context.handleAndRaise("Error applying msgpack schema to writer.", e);
      }
    } else {
      msgpackReader.ensureAtLeastOneField(writer);
    }

    writer.setValueCount(context.getRecordCount());
    context.updateRunningCount();
    return context.getRecordCount();
  }

  @Override
  public void close() throws Exception {
    if (stream != null) {
      stream.close();
    }
  }
}
