package org.apache.drill.exec.vector;

import io.netty.buffer.ByteBuf;

public interface RepeatedVariableWidthVector extends ValueVector{
  /**
   * Allocate a new memory space for this vector.  Must be called prior to using the ValueVector.
   *
   * @param totalBytes   Desired size of the underlying data buffer.
   * @param parentValueCount   Number of separate repeating groupings.
   * @param childValueCount   Number of supported values in the vector.
   */
  public void allocateNew(int totalBytes, int parentValueCount, int childValueCount);
  
  /**
   * Load the records in the provided buffer based on the given number of values.
   * @param dataBytes   The number of bytes associated with the data array.
   * @param parentValueCount   Number of separate repeating groupings.
   * @param childValueCount   Number of supported values in the vector.
   * @param buf Incoming buffer.
   * @return The number of bytes of the buffer that were consumed.
   */
  public int load(int dataBytes, int parentValueCount, int childValueCount, ByteBuf buf);
}
