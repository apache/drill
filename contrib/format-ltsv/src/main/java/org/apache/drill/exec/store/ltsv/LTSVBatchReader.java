package org.apache.drill.exec.store.ltsv;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework.FileSchemaNegotiator;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.MetadataUtils;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;
import org.apache.hadoop.mapred.FileSplit;
import org.slf4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

public class LTSVBatchReader implements ManagedReader<FileScanFramework.FileSchemaNegotiator> {

  private static final Logger logger = org.slf4j.LoggerFactory.getLogger(LTSVBatchReader.class);

  private final LTSVFormatPluginConfig formatConfig;

  private InputStream fsStream;

  private BufferedReader reader;

  private ResultSetLoader loader;

  private RowSetLoader rowWriter;

  private FileSplit split;

  private Iterator fileIterator;

  public LTSVBatchReader(LTSVFormatPluginConfig formatConfig) {
    this.formatConfig = formatConfig;
  }

  @Override
  public boolean open(FileSchemaNegotiator negotiator) {
    split = negotiator.split();
    loader = negotiator.build();
    rowWriter = loader.writer();
    try {
      this.fsStream = negotiator.fileSystem().openPossiblyCompressedStream(split.getPath());
      this.reader = new BufferedReader(new InputStreamReader(fsStream, StandardCharsets.UTF_8));
      fileIterator = new LTSVRecordIterator(rowWriter, reader);
    } catch (IOException e) {
      throw UserException
        .dataReadError(e)
        .message(String.format("Failed to open input file: %s", split.getPath()))
        .build(logger);
    }
    return true;
  }

  @Override
  public boolean next() {
    RowSetLoader rowWriter = loader.writer();
    while (! rowWriter.isFull()) {
      if (!fileIterator.hasNext()) {
        return false;
      }
      fileIterator.next();
    }
    return true;
  }

  @Override
  public void close() {
    try {
      if (reader != null) {
        reader.close();
        reader = null;
      }
      if (fsStream != null) {
        fsStream.close();
        fsStream = null;
      }
    } catch (IOException e) {
      logger.warn("Error closing LTSV Batch Record Reader.");
    }
  }

  public static void writeStringColumn(TupleWriter rowWriter, String name, String value) {
    int index = rowWriter.tupleSchema().index(name);
    if (index == -1) {
      ColumnMetadata colSchema = MetadataUtils.newScalar(name, TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL);
      index = rowWriter.addColumn(colSchema);
    }
    ScalarWriter colWriter = rowWriter.scalar(index);
    colWriter.setString(value);
  }
}
