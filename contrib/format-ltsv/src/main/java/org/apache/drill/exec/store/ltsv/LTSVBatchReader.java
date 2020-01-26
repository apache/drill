package org.apache.drill.exec.store.ltsv;

import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework.FileSchemaNegotiator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LTSVBatchReader extends EasyEVFBatchReader {

  private static final Logger logger = LoggerFactory.getLogger(LTSVBatchReader.class);

  public LTSVBatchReader(LTSVFormatPluginConfig formatConfig) {
    super();
  }

  public boolean open(FileSchemaNegotiator negotiator) {
    super.open(negotiator);
    super.fileIterator = new LTSVRecordIterator(getRowWriter(), reader);
    return true;
  }
}
