package org.apache.drill.exec.store.msgpack.valuewriter;

import org.apache.drill.exec.store.msgpack.MsgpackReaderContext;

public abstract class AbstractValueWriter implements ValueWriter {
  protected static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractValueWriter.class);

  protected MsgpackReaderContext context;

  public AbstractValueWriter() {
    super();
  }

  public void setup(MsgpackReaderContext context) {
    this.context = context;
  }

}
