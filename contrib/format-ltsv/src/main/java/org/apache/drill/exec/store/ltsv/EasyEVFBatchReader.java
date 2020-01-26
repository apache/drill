package org.apache.drill.exec.store.ltsv;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos;
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
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

public abstract class EasyEVFBatchReader implements ManagedReader<FileSchemaNegotiator> {

  private static final Logger logger = LoggerFactory.getLogger(EasyEVFBatchReader.class);

  public FileSplit split;

  public Iterator fileIterator;

  public ResultSetLoader loader;

  private RowSetLoader rowWriter;

  public InputStream fsStream;

  public BufferedReader reader;


  public EasyEVFBatchReader() {
  }

  public RowSetLoader getRowWriter() {
    return rowWriter;
  }

  @Override
  public boolean open(FileSchemaNegotiator negotiator) {
    this.split = negotiator.split();
    this.loader = negotiator.build();
    this.rowWriter = loader.writer();
    try {
      this.fsStream = negotiator.fileSystem().openPossiblyCompressedStream(split.getPath());
      this.reader = new BufferedReader(new InputStreamReader(fsStream, StandardCharsets.UTF_8));
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
    while (!rowWriter.isFull()) {
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
      logger.warn("Error closing batch Record Reader.");
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

  public static void writeIntColumn(TupleWriter rowWriter, String name, int value) {
    int index = rowWriter.tupleSchema().index(name);
    if (index == -1) {
      ColumnMetadata colSchema = MetadataUtils.newScalar(name, TypeProtos.MinorType.INT, TypeProtos.DataMode.OPTIONAL);
      index = rowWriter.addColumn(colSchema);
    }
    ScalarWriter colWriter = rowWriter.scalar(index);
    colWriter.setInt(value);
  }

}
