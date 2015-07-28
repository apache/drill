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
package org.apache.drill.exec.store.httpd;

import io.netty.buffer.DrillBuf;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import nl.basjes.parse.core.Parser;
import nl.basjes.parse.core.exceptions.DissectionFailure;
import nl.basjes.parse.core.exceptions.InvalidDissectorException;
import nl.basjes.parse.core.exceptions.MissingDissectorsException;
import nl.basjes.parse.httpdlog.ApacheHttpdLogFormatDissector;
import nl.basjes.parse.httpdlog.dissectors.HttpFirstLineDissector;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.RecordWriter;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.easy.EasyFormatPlugin;
import org.apache.drill.exec.store.dfs.easy.EasyWriter;
import org.apache.drill.exec.store.dfs.easy.FileWork;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.drill.exec.vector.complex.writer.BigIntWriter;
import org.apache.drill.exec.vector.complex.writer.Float8Writer;
import org.apache.drill.exec.vector.complex.writer.VarCharWriter;
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
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class HttpdFormatPlugin extends EasyFormatPlugin<HttpdFormatPlugin.HttpdLogFormatConfig> {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HttpdFormatPlugin.class);

  private static final String DEFAULT_EXTENSION = "httpd";

  public HttpdFormatPlugin(
      String name,
      DrillbitContext context,
      Configuration fsConf,
      StoragePluginConfig storageConfig,
      HttpdLogFormatConfig formatConfig) {
    super(name, context, fsConf, storageConfig, formatConfig, true, false, true, true,
        Lists.newArrayList(DEFAULT_EXTENSION), DEFAULT_EXTENSION);
  }

  @JsonTypeName("httpd")
  public static class HttpdLogFormatConfig implements FormatPluginConfig {
    public String format;
  }

  private class RecordReader extends AbstractRecordReader {

    private final DrillFileSystem fs;
    private final FileWork work;
    private final FragmentContext fragmentContext;

    private ComplexWriter writer;
    private Parser<ComplexWriterFacade> parser;
    private LineRecordReader lineReader;
    private LongWritable lineNumber;
    private ComplexWriterFacade record;
    private DrillBuf managedBuffer;

    public RecordReader(FragmentContext context, DrillFileSystem fs, FileWork work) {
      this.fs = fs;
      this.work = work;
      fragmentContext = context;
      managedBuffer = context.getManagedBuffer();
    }

    @Override
    public void setup(OperatorContext context, OutputMutator output) throws ExecutionSetupException {

      try {

        parser = new PartiallyDissectedParser<ComplexWriterFacade>(
            ComplexWriterFacade.class,
            HttpdFormatPlugin.this.getConfig().format);
        writer = new VectorContainerWriter(output);
        record = new ComplexWriterFacade(writer);
        record.addAsParseTarget(parser);

        final Path path = fs.makeQualified(new Path(work.getPath()));
        FileSplit split = new FileSplit(path, work.getStart(), work.getLength(), new String[] { "" });
        TextInputFormat inputFormat = new TextInputFormat();
        JobConf job = new JobConf(fs.getConf());
        job.setInt("io.file.buffer.size", fragmentContext.getConfig()
            .getInt(ExecConstants.TEXT_LINE_READER_BUFFER_SIZE));
        job.setInputFormat(inputFormat.getClass());
        lineReader = (LineRecordReader) inputFormat.getRecordReader(split, job, Reporter.NULL);
        lineNumber = lineReader.createKey();
      } catch (Exception e) {
        throw handleAndGenerate("Failure in creating record reader", e);
      }


    }

    private DrillBuf buf(int size) {
      if (managedBuffer.capacity() < size) {
        managedBuffer = managedBuffer.reallocIfNeeded(size);
      }
      return managedBuffer;
    }

    protected RuntimeException handleAndGenerate(String s, Exception e) {
      throw UserException.dataReadError(e)
          .message(s + "\n%s", e.getMessage())
          .addContext("Path", work.getPath())
          .addContext("Split Start", work.getStart())
          .addContext("Split Length", work.getLength())
          .addContext("Local Line Number", lineNumber.get())
          .build(logger);
    }

    @Override
    public int next() {
      try {
        final Text currentText = lineReader.createValue();

        writer.allocate();
        writer.reset();
        int recordCount = 0;

        for (; recordCount < 4095 && lineReader.next(lineNumber, currentText); recordCount++) {
          writer.setPosition(recordCount);
          parser.parse(record, currentText.toString());
        }

        writer.setValueCount(recordCount);
        return recordCount;
      } catch (DissectionFailure | InvalidDissectorException | MissingDissectorsException | IOException e) {
        throw handleAndGenerate("Failure while reading httpd log record.", e);
      }
    }

    @Override
    public void cleanup() {

      try {
        if (lineReader != null) {
          lineReader.close();
        }
      } catch (IOException e) {
        logger.warn("Failure while closing Httpd reader.", e);
      }
    }

    /**
     * Maps Httpd Log Libraries calls to Drills ComplexWriter interface.
     */
    public class ComplexWriterFacade {
      private final ComplexWriter writer;
      private final Map<String, VarCharWriter> stringWriters = Maps.newHashMap();
      private final Map<String, BigIntWriter> longWriters = Maps.newHashMap();
      private final Map<String, Float8Writer> doubleWriters = Maps.newHashMap();

      private ComplexWriterFacade(ComplexWriter writer) {
        this.writer = writer;
      }

      @SuppressWarnings("unused")
      public void set(final String name, final String value) {
        if (value != null) {
          final byte[] stringBytes = value.getBytes(Charsets.UTF_8);
          final DrillBuf stringBuffer = buf(stringBytes.length);
          stringBuffer.clear();
          stringBuffer.writeBytes(stringBytes);
          final VarCharWriter writer = stringWriters.get(name);
          if (writer != null) {
            writer.writeVarChar(0, stringBytes.length, stringBuffer);
          } else {
            logger.warn("Dropped string.  Name: {}, Value: {}", name, value);
          }
        }
      }

      @SuppressWarnings("unused")
      public void set(String name, Long value) {
        if (value != null) {
          longWriters.get(name).writeBigInt(value);
        }
      }

      @SuppressWarnings("unused")
      public void set(String name, Double value) {
        if (value != null) {
          doubleWriters.get(name).writeFloat8(value);
        }
      }

      private void add(Parser<ComplexWriterFacade> parser, String path, VarCharWriter writer)
          throws NoSuchMethodException,
          SecurityException {
        stringWriters.put(path, writer);
        parser.addParseTarget(
            ComplexWriterFacade.class.getMethod("set", new Class[] { String.class, String.class }),
            path);
      }

      @SuppressWarnings("unused")
      private void add(Parser<ComplexWriterFacade> parser, String path, Float8Writer writer)
          throws NoSuchMethodException,
          SecurityException {
        doubleWriters.put(path, writer);
        parser.addParseTarget(
            ComplexWriterFacade.class.getMethod("set", new Class[] { String.class, Double.class }),
            path);
      }

      private void add(Parser<ComplexWriterFacade> parser, String path, BigIntWriter writer)
          throws NoSuchMethodException,
          SecurityException {
        longWriters.put(path, writer);
        parser.addParseTarget(
            ComplexWriterFacade.class.getMethod("set", new Class[] { String.class, Long.class }),
            path);
      }

      public void addAsParseTarget(Parser<ComplexWriterFacade> parser) {
        try {

          for (final String path : parser.getPossiblePaths()) {
            switch (path) {
            case "IP:connection.client.ip":
              add(parser, path, writer.rootAsMap().map("client").varChar("ip"));
              break;
            case "IP:connection.client.peerip":
              add(parser, path, writer.rootAsMap().map("client").varChar("peer_ip"));
              break;
            case "IP:connection.server.ip":
              add(parser, path, writer.rootAsMap().map("server").varChar("ip"));
              break;
            case "BYTES:response.body.bytes":
              add(parser, path, writer.rootAsMap().map("response").bigInt("bytes"));
              break;
            case "BYTES:response.body.bytesclf":
              add(parser, path, writer.rootAsMap().map("response").bigInt("bytes"));
              break;
            case "HTTP.COOKIE:request.cookies.":
              add(parser, path, writer.rootAsMap().map("request").varChar("cookies"));
              break;
            case "MICROSECONDS:server.process.time":
              add(parser, path, writer.rootAsMap().map("response").bigInt("process_time"));
              break;
            case "FILENAME:server.filename":
              add(parser, path, writer.rootAsMap().map("response").varChar("filename"));
              break;
            case "IP:connection.client.host":
              add(parser, path, writer.rootAsMap().map("client").varChar("host"));
              break;
            case "PROTOCOL:request.protocol":
              add(parser, path, writer.rootAsMap().map("request").varChar("protocol"));
              break;
            case "HTTP.HEADER:request.header.":
              add(parser, path, writer.rootAsMap().map("request").varChar("header"));
              break;
            case "NUMBER:connection.keepalivecount":
              add(parser, path, writer.rootAsMap().map("client").bigInt("keepalivecount"));
              break;
            case "NUMBER:connection.client.logname":
              add(parser, path, writer.rootAsMap().map("request").bigInt("logname"));
              break;
            case "STRING:request.errorlogid":
              add(parser, path, writer.rootAsMap().map("request").varChar("errorlogid"));
              break;
            case "HTTP.METHOD:request.method":
              add(parser, path, writer.rootAsMap().map("request").varChar("method"));
              break;
            case "PORT:request.server.port.canonical":
              add(parser, path, writer.rootAsMap().map("server").bigInt("canonical_port"));
              break;
            case "PORT:connection.server.port.canonical":
              add(parser, path, writer.rootAsMap().map("server").bigInt("canonical_port"));
              break;
            case "PORT:connection.client.port":
              add(parser, path, writer.rootAsMap().map("client").bigInt("port"));
              break;
            case "NUBMER:connection.server.child.processid":
              add(parser, path, writer.rootAsMap().map("server").bigInt("process_id"));
              break;
            case "NUMBER:connection.server.child.threadid":
              add(parser, path, writer.rootAsMap().map("server").bigInt("thread_id"));
              break;
            case "STRING:connection.server.child.hexthreadid":
              add(parser, path, writer.rootAsMap().map("connection").varChar("hex_thread_id"));
              break;
            case "HTTP.QUERYSTRING:request.querystring":
              add(parser, path, writer.rootAsMap().map("").varChar(""));
              break;
            case "HTTP.FIRSTLINE:request.firstline":
              add(parser, path, writer.rootAsMap().map("").varChar(""));
              break;
            case "STRING:request.handler":
              add(parser, path, writer.rootAsMap().map("request").varChar("handler"));
              break;
            case "STRING:request.status.original":
              add(parser, path, writer.rootAsMap().map("request").varChar("status_original"));
              break;
            case "STRING:request.status.last":
              add(parser, path, writer.rootAsMap().map("request").varChar("status_last"));
              break;
            case "TIME.STAMP:request.receive.time":
              add(parser, path, writer.rootAsMap().map("request").varChar("timestamp"));
              break;
            case "TIME.EPOCH:request.receive.time.begin.msec":
              add(parser, path, writer.rootAsMap().map("request").bigInt("begin_msec"));
              break;
            case "TIME.EPOCH:request.receive.time.end.msec":
              add(parser, path, writer.rootAsMap().map("request").bigInt("end_msec"));
              break;
            case "TIME.EPOCH.USEC:request.receive.time.begin.usec":
              add(parser, path, writer.rootAsMap().map("request").bigInt("begin_usec"));
              break;
            case "TIME.EPOCH.USEC:request.receive.time.end.usec":
              add(parser, path, writer.rootAsMap().map("request").bigInt("end_usec"));
              break;
            case "TIME.EPOCH:request.receive.time.begin.msec_frac":
              add(parser, path, writer.rootAsMap().map("request").bigInt("begin_msec_frac"));
              break;
            case "TIME.EPOCH:request.receive.time.end.msec_frac":
              add(parser, path, writer.rootAsMap().map("request").varChar("end_msec_frac"));
              break;
            case "TIME.EPOCH.USEC_FRAC:request.receive.time.begin.usec_frac":
              add(parser, path, writer.rootAsMap().map("request").varChar("begin_usec_frac"));
              break;
            case "TIME.EPOCH.USEC_FRAC:request.receive.time.end.usec_frac":
              add(parser, path, writer.rootAsMap().map("request").varChar("end_usec_frac"));
              break;
            case "SECONDS:response.server.processing.time":
              add(parser, path, writer.rootAsMap().map("response").varChar("processing_time"));
              break;
            case "STRING:connection.client.user":
              add(parser, path, writer.rootAsMap().map("client").varChar("user"));
              break;
            case "URI:request.urlpath":
              add(parser, path, writer.rootAsMap().map("request").varChar("url"));
              break;
            case "STRING:connection.server.name.canonical":
              add(parser, path, writer.rootAsMap().map("server").varChar("canonical_name"));
              break;
            case "STRING:connection.server.name":
              add(parser, path, writer.rootAsMap().map("server").varChar("name"));
              break;
            case "HTTP.CONNECTSTATUS:response.connection.status":
              add(parser, path, writer.rootAsMap().map("response").varChar("connection_status"));
              break;
            case "BYTES:request.bytes":
              add(parser, path, writer.rootAsMap().map("request").varChar("bytes"));
              break;
            case "BYTES:response.bytes":
              add(parser, path, writer.rootAsMap().map("response").bigInt("bytes"));
              break;
            case "HTTP.COOKIES:request.cookies":
              add(parser, path, writer.rootAsMap().map("request").varChar("cookies"));
              break;
            case "HTTP.SETCOOKIES:response.cookies":
              add(parser, path, writer.rootAsMap().map("response").varChar("cookies"));
              break;
            case "HTTP.USERAGENT:request.user-agent":
              add(parser, path, writer.rootAsMap().map("request").varChar("useragent"));
              break;
            case "HTTP.URI:request.referer":
              add(parser, path, writer.rootAsMap().map("request").varChar("referer"));
              break;
            case "HTTP.METHOD:method":
              add(parser, path, writer.rootAsMap().map("request").varChar("method"));
              break;
            case "HTTP.URI:uri":
              add(parser, path, writer.rootAsMap().map("request").varChar("uri"));
              break;
            case "HTTP.PROTOCOL:protocol":
              add(parser, path, writer.rootAsMap().map("request").varChar("protocol"));
              break;
            case "HTTP.PROTOCOL.VERSION:protocol.version":
              add(parser, path, writer.rootAsMap().map("request").varChar("protocol_version"));
              break;
            case "HTTP.METHOD:request.firstline.method":
              add(parser, path, writer.rootAsMap().map("request").varChar("method"));
              break;
            case "HTTP.URI:request.firstline.uri":
              add(parser, path, writer.rootAsMap().map("request").varChar("uri"));
              break;
            case "HTTP.PROTOCOL:request.firstline.protocol":
              add(parser, path, writer.rootAsMap().map("request").varChar("protocol"));
              break;
            case "HTTP.PROTOCOL.VERSION:request.firstline.protocol.version":
              add(parser, path, writer.rootAsMap().map("request").varChar("protocol_version"));
              break;
            default:

              // if we don't know what to do, just write the raw value.
              parser.addParseTarget(
                  ComplexWriterFacade.class.getMethod("set", new Class[] { String.class, String.class }),
                  path);
              final String noPeriodPath = path.replace(".", "_");
              stringWriters.put(path, writer.rootAsMap().varChar(noPeriodPath));
              break;

            }
          }


        } catch (MissingDissectorsException | SecurityException | NoSuchMethodException | InvalidDissectorException e) {
          throw handleAndGenerate("Failure while setting up log mappings.", e);
        }
      }
    }
  }

  @Override
  public boolean supportsPushDown() {
    return true;
  }


  @Override
  public RecordReader getRecordReader(FragmentContext context, DrillFileSystem dfs,
      FileWork fileWork, List<SchemaPath> columns) throws ExecutionSetupException {
    return new RecordReader(context, dfs, fileWork);
  }

  @Override
  public RecordWriter getRecordWriter(FragmentContext context, EasyWriter writer) throws IOException {
    throw new UnsupportedOperationException("Drill doesn't currently support writing to HTTPD logs.");
  }

  @Override
  public int getReaderOperatorType() {
    return -1;
  }

  @Override
  public int getWriterOperatorType() {
    return -1;
  }

  private class PartiallyDissectedParser<RECORD> extends Parser<RECORD> {
    public PartiallyDissectedParser(Class<RECORD> clazz, final String logformat) {
      super(clazz);

      addDissector(new ApacheHttpdLogFormatDissector(logformat));
      addDissector(new HttpFirstLineDissector());
      setRootType(ApacheHttpdLogFormatDissector.INPUT_TYPE);
    }

  }
}
