package org.apache.drill.exec.store.fixedwidth;

import org.apache.drill.common.AutoCloseables;
import org.apache.drill.common.exceptions.CustomErrorContext;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.physical.impl.scan.v3.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.v3.file.FileSchemaNegotiator;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.shaded.guava.com.google.common.base.Charsets;
import org.apache.hadoop.mapred.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.epam.parso.impl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class FixedWidthBatchReader implements ManagedReader {

  private final int maxRecords;  // Do we need this?
  private final FixedWidthFormatConfig config;
  private InputStream fsStream;
  private ResultSetLoader loader;
  private FileSplit split;
  private CustomErrorContext errorContext;
  private static final Logger logger = LoggerFactory.getLogger(FixedWidthBatchReader.class);
  private BufferedReader reader;

  public FixedWidthBatchReader(FileSchemaNegotiator negotiator, FixedWidthFormatConfig config, int maxRecords) {
    this.loader = open(negotiator);
    this.config = config;
    this.maxRecords = maxRecords;
  }

  @Override
  public boolean next() {
    return true;
  }

  @Override
  public void close() {
    if (fsStream != null){
      AutoCloseables.closeSilently(fsStream);
      fsStream = null;
    }
  }

  private ResultSetLoader open(FileSchemaNegotiator negotiator) {
    this.split = (FileSplit) negotiator.split();
    this.errorContext = negotiator.parentErrorContext();
    openFile(negotiator);

    try {
      negotiator.tableSchema(buildSchema(), true);
      this.loader = negotiator.build();
    } catch (Exception e) {
      throw UserException
        .dataReadError(e)
        .message("Failed to open input file: {}", this.split.getPath().toString())
        .addContext(this.errorContext)
        .addContext(e.getMessage())
        .build(FixedWidthBatchReader.logger);
    }
    this.reader = new BufferedReader(new InputStreamReader(this.fsStream, Charsets.UTF_8));
    return this.loader;
  }

  private void openFile(FileSchemaNegotiator negotiator) {
    try {
      this.fsStream = negotiator.file().fileSystem().openPossiblyCompressedStream(this.split.getPath());
      sasFileReader = new SasFileReaderImpl(this.fsStream);
      firstRow = sasFileReader.readNext();
    } catch (IOException e) {
      throw UserException
        .dataReadError(e)
        .message("Unable to open Fixed Width File %s", this.split.getPath())
        .addContext(e.getMessage())
        .addContext(this.errorContext)
        .build(FixedWidthBatchReader.logger);
    }
  }

  private TupleMetadata buildSchema() {
    SchemaBuilder builder = new SchemaBuilder();
    for (FixedWidthFieldConfig field : config.getFields()) {
      if (field.getType() == TypeProtos.MinorType.VARDECIMAL){
        builder.addNullable(field.getName(), TypeProtos.MinorType.VARDECIMAL,38,4);
        //revisit this
      } else {
        builder.addNullable(field.getName(), field.getType());
      }
    }
    return builder.buildSchema();
  }
}
