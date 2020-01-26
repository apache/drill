package org.apache.drill.exec.store.ltsv;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework.FileSchemaNegotiator;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.hadoop.mapred.FileSplit;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

public abstract class EasyEVFBatchReader implements ManagedReader<FileSchemaNegotiator> {

  private FileSplit split;

  private Iterator fileIterator;

  private ResultSetLoader loader;

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

}
