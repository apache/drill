package org.apache.drill.exec.expr.holders;

import io.netty.buffer.ByteBuf;

public class VarBinaryHolder {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(VarBinaryHolder.class);
  
  public ByteBuf buffer;
  public int start;
  public int length;
  
}
