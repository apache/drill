package org.apache.drill.exec.vector;

import io.netty.buffer.ByteBuf;

public class ByteHolder {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ByteHolder.class);
  
  public ByteBuf buffer;
  public int start;
  public int length;
  
}
