package org.apache.drill.exec.vector;

import io.netty.buffer.ByteBuf;

public interface RepeatedFixedWidthVector extends ValueVector{
  /**
   * Allocate a new memory space for this vector.  Must be called prior to using the ValueVector.
   *
   * @param parentValueCount   Number of separate repeating groupings.
   * @param childValueCount   Number of supported values in the vector.
   */
  public void allocateNew(int parentValueCount, int childValueCount);
  
  /**
   * Load the records in the provided buffer based on the given number of values.
   * @param parentValueCount   Number of separate repeating groupings.
   * @param valueCount Number atomic values the buffer contains.
   * @param buf Incoming buffer.
   * @return The number of bytes of the buffer that were consumed.
   */
  public int load(int parentValueCount, int childValueCount, ByteBuf buf);
}
