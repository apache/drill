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
package org.apache.drill.exec.store.httpd;

import java.io.IOException;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import nl.basjes.parse.core.exceptions.DissectionFailure;
import nl.basjes.parse.core.exceptions.InvalidDissectorException;
import nl.basjes.parse.core.exceptions.MissingDissectorsException;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.proto.UserBitShared.CoreOperatorType;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.RecordWriter;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.easy.EasyFormatPlugin;
import org.apache.drill.exec.store.dfs.easy.EasyWriter;
import org.apache.drill.exec.store.dfs.easy.FileWork;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;

import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.drill.shaded.guava.com.google.common.collect.Maps;
import java.util.Map;
import org.apache.drill.exec.store.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpdLogFormatPlugin extends EasyFormatPlugin<HttpdLogFormatPlugin.HttpdLogFormatConfig> {

  private static final Logger LOG = LoggerFactory.getLogger(HttpdLogFormatPlugin.class);
  private static final String PLUGIN_EXTENSION = "httpd";
  private static final int VECTOR_MEMORY_ALLOCATION = 4095;

  public HttpdLogFormatPlugin(final String name, final DrillbitContext context, final Configuration fsConf,
      final StoragePluginConfig storageConfig, final HttpdLogFormatConfig formatConfig) {

    super(name, context, fsConf, storageConfig, formatConfig, true, false, true, true,
        Lists.newArrayList(PLUGIN_EXTENSION), PLUGIN_EXTENSION);
  }

  /**
   * This class is a POJO to hold the configuration for the HttpdLogFormat Parser. This is automatically
   * serialized/deserialized from JSON format.
   */
  @JsonTypeName(PLUGIN_EXTENSION) @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public static class HttpdLogFormatConfig implements FormatPluginConfig {

    public String logFormat;
    public String timestampFormat;

    /**
     * @return the logFormat
     */
    public String getLogFormat() {
      return logFormat;
    }

    /**
     * @return the timestampFormat
     */
    public String getTimestampFormat() {
      return timestampFormat;
    }

    @Override
    public int hashCode() {
      int result = logFormat != null ? logFormat.hashCode() : 0;
      result = 31 * result + (timestampFormat != null ? timestampFormat.hashCode() : 0);
      return result;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      HttpdLogFormatConfig that = (HttpdLogFormatConfig) o;

      if (logFormat != null ? !logFormat.equals(that.logFormat) : that.logFormat != null) {
        return false;
      }
      return timestampFormat != null ? timestampFormat.equals(that.timestampFormat) : that.timestampFormat == null;
    }
  }

  /**
   * This class performs the work for the plugin. This is where all logic goes to read records. In this case httpd logs
   * are lines terminated with a new line character.
   */
  private class HttpdLogRecordReader extends AbstractRecordReader {

    private final DrillFileSystem fs;
    private final FileWork work;
    private final FragmentContext fragmentContext;
    private ComplexWriter writer;
    private HttpdParser parser;
    private LineRecordReader lineReader;
    private LongWritable lineNumber;

    public HttpdLogRecordReader(final FragmentContext context, final DrillFileSystem fs, final FileWork work, final List<SchemaPath> columns) {
      this.fs = fs;
      this.work = work;
      this.fragmentContext = context;
      setColumns(columns);
    }

    /**
     * The query fields passed in are formatted in a way that Drill requires. Those must be cleaned up to work with the
     * parser.
     *
     * @return Map with Drill field names as a key and Parser Field names as a value
     */
    private Map<String, String> makeParserFields() {
      final Map<String, String> fieldMapping = Maps.newHashMap();
      for (final SchemaPath sp : getColumns()) {
        final String drillField = sp.getRootSegment().getPath();
        final String parserField = HttpdParser.parserFormattedFieldName(drillField);
        fieldMapping.put(drillField, parserField);
      }
      return fieldMapping;
    }

    @Override
    public void setup(final OperatorContext context, final OutputMutator output) throws ExecutionSetupException {
      try {
        /**
         * Extract the list of field names for the parser to use if it is NOT a star query. If it is a star query just
         * pass through an empty map, because the parser is going to have to build all possibilities.
         */
        final Map<String, String> fieldMapping = !isStarQuery() ? makeParserFields() : null;
        writer = new VectorContainerWriter(output);
        parser = new HttpdParser(writer.rootAsMap(), context.getManagedBuffer(),
            HttpdLogFormatPlugin.this.getConfig().getLogFormat(),
            HttpdLogFormatPlugin.this.getConfig().getTimestampFormat(),
            fieldMapping);

        final Path path = fs.makeQualified(new Path(work.getPath()));
        FileSplit split = new FileSplit(path, work.getStart(), work.getLength(), new String[]{""});
        TextInputFormat inputFormat = new TextInputFormat();
        JobConf job = new JobConf(fs.getConf());
        job.setInt("io.file.buffer.size", fragmentContext.getConfig().getInt(ExecConstants.TEXT_LINE_READER_BUFFER_SIZE));
        job.setInputFormat(inputFormat.getClass());
        lineReader = (LineRecordReader) inputFormat.getRecordReader(split, job, Reporter.NULL);
        lineNumber = lineReader.createKey();
      }
      catch (NoSuchMethodException | MissingDissectorsException | InvalidDissectorException e) {
        throw handleAndGenerate("Failure creating HttpdParser", e);
      }
      catch (IOException e) {
        throw handleAndGenerate("Failure creating HttpdRecordReader", e);
      }
    }

    private RuntimeException handleAndGenerate(final String s, final Exception e) {
      throw UserException.dataReadError(e)
          .message(s + "\n%s", e.getMessage())
          .addContext("Path", work.getPath())
          .addContext("Split Start", work.getStart())
          .addContext("Split Length", work.getLength())
          .addContext("Local Line Number", lineNumber.get())
          .build(LOG);
    }

    /**
     * This record reader is given a batch of records (lines) to read. Next acts upon a batch of records.
     *
     * @return Number of records in this batch.
     */
    @Override
    public int next() {
      try {
        final Text line = lineReader.createValue();

        writer.allocate();
        writer.reset();

        int recordCount = 0;
        while (recordCount < VECTOR_MEMORY_ALLOCATION && lineReader.next(lineNumber, line)) {
          writer.setPosition(recordCount);
          parser.parse(line.toString());
          recordCount++;
        }
        writer.setValueCount(recordCount);

        return recordCount;
      }
      catch (DissectionFailure | InvalidDissectorException | MissingDissectorsException | IOException e) {
        throw handleAndGenerate("Failure while parsing log record.", e);
      }
    }

    @Override
    public void close() throws Exception {
      try {
        if (lineReader != null) {
          lineReader.close();
        }
      }
      catch (IOException e) {
        LOG.warn("Failure while closing Httpd reader.", e);
      }
    }

    @Override
    public String toString() {
      return "HttpdLogRecordReader[Path=" + work.getPath()
          + ", Start=" + work.getStart()
          + ", Length=" + work.getLength()
          + ", Line=" + lineNumber.get()
          + "]";
    }
  }

  /**
   * This plugin supports pushing down into the parser. Only fields specifically asked for within the configuration will
   * be parsed. If no fields are asked for then all possible fields will be returned.
   *
   * @return true
   */
  @Override
  public boolean supportsPushDown() {
    return true;
  }

  @Override
  public RecordReader getRecordReader(final FragmentContext context, final DrillFileSystem dfs, final FileWork fileWork, final List<SchemaPath> columns, final String userName) throws ExecutionSetupException {
    return new HttpdLogRecordReader(context, dfs, fileWork, columns);
  }

  @Override
  public RecordWriter getRecordWriter(final FragmentContext context, final EasyWriter writer) throws IOException {
    throw new UnsupportedOperationException("Drill doesn't currently support writing HTTPd logs");
  }

  @Override
  public int getReaderOperatorType() {
    return CoreOperatorType.HTPPD_LOG_SUB_SCAN_VALUE;
  }

  @Override
  public int getWriterOperatorType() {
    throw new UnsupportedOperationException();
  }
}
