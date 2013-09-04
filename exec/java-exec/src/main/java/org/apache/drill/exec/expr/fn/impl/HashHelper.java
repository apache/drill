package org.apache.drill.exec.expr.fn.impl;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class HashHelper {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HashHelper.class);
  
  
  /** taken from mahout **/
  public static int hash(ByteBuffer buf, int seed) {
    // save byte order for later restoration

    int m = 0x5bd1e995;
    int r = 24;

    int h = seed ^ buf.remaining();

    while (buf.remaining() >= 4) {
      int k = buf.getInt();

      k *= m;
      k ^= k >>> r;
      k *= m;

      h *= m;
      h ^= k;
    }

    if (buf.remaining() > 0) {
      ByteBuffer finish = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
      // for big-endian version, use this first:
      // finish.position(4-buf.remaining());
      finish.put(buf).rewind();
      h ^= finish.getInt();
      h *= m;
    }

    h ^= h >>> 13;
    h *= m;
    h ^= h >>> 15;

    return h;
  }

}
