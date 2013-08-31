package org.apache.drill.exec.vector;

import io.netty.buffer.UnpooledByteBufAllocator;

import org.apache.drill.exec.expr.holders.VarCharHolder;

import com.google.common.base.Charsets;


public class ValueHolderHelper {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ValueHolderHelper.class);
  
  public static VarCharHolder getVarCharHolder(String s){
    VarCharHolder vch = new VarCharHolder();
    
    byte[] b = s.getBytes(Charsets.UTF_8);
    vch.start = 0;
    vch.end = b.length;
    vch.buffer = UnpooledByteBufAllocator.DEFAULT.buffer();
    vch.buffer.setBytes(0, b);
    return vch;
  }
}
