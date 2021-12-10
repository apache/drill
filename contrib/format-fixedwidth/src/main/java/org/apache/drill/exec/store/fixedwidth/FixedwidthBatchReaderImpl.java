package org.apache.drill.exec.store.fixedwidth;

import org.apache.drill.common.AutoCloseables;
import org.apache.drill.common.exceptions.CustomErrorContext;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.scan.v3.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.v3.SchemaNegotiator;
import org.apache.drill.exec.physical.impl.scan.v3.file.FileScanLifecycle;
import org.apache.drill.exec.physical.impl.scan.v3.file.FileSchemaNegotiator;
import org.apache.drill.exec.physical.impl.scan.v3.lifecycle.ReaderLifecycle;
import org.apache.drill.exec.physical.impl.scan.v3.lifecycle.SchemaNegotiatorImpl;
import org.apache.drill.exec.physical.impl.scan.v3.schema.ProjectedColumn;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.shaded.guava.com.google.common.base.Charsets;
import org.apache.hadoop.mapred.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class FixedwidthBatchReaderImpl implements ManagedReader {

  private final int maxRecords;
  private final FixedwidthFormatConfig config;
  private InputStream fsStream;
  private ResultSetLoader loader;
  private FileSplit split;
  private CustomErrorContext errorContext;
  private static final Logger logger = LoggerFactory.getLogger(FixedwidthBatchReader.class);

  public FixedwidthBatchReaderImpl (SchemaNegotiator negotiator, FixedwidthFormatConfig config, int maxRecords) {
    this.loader = open(negotiator);
    this.config = config;
    this.maxRecords = maxRecords;
  }

  @Override
  public boolean next() {

  }

  @Override
  public void close() {
    if (fsStream != null){
      AutoCloseables.closeSilently(fsStream);
      fsStream = null;
    }
  }

  private ResultSetLoader open(SchemaNegotiator negotiator) {
    split = (FileSplit) negotiator.split();
    errorContext = negotiator.parentErrorContext();
    openFile(negotiator);

    try {
      negotiator.tableSchema(buildSchema(), true);
      loader = negotiator.build();
    } catch (Exception e) {
      throw UserException
        .dataReadError(e)
        .message("Failed to open input file: {}", split.getPath().toString())
        .addContext(errorContext)
        .addContext(e.getMessage())
        .build(logger);
    }
    reader = new BufferedReader(new InputStreamReader(fsStream, Charsets.UTF_8));
    return loader;
  }

  private void openFile(FileSchemaNegotiator negotiator) {
    try {
      fsStream = negotiator.file().fileSystem().openPossiblyCompressedStream(split.getPath());
      sasFileReader = new SasFileReaderImpl(fsStream);
      firstRow = sasFileReader.readNext();
    } catch (IOException e) {
      throw UserException
        .dataReadError(e)
        .message("Unable to open Fixed Width File %s", split.getPath())
        .addContext(e.getMessage())
        .addContext(errorContext)
        .build(logger);
    }
  }
}
