package org.apache.drill.exec.vector;

import io.netty.buffer.ByteBuf;

public interface VariableWidthVector extends ValueVector{

  /**
   * Allocate a new memory space for this vector.  Must be called prior to using the ValueVector.
   *
   * @param totalBytes   Desired size of the underlying data buffer.
   * @param valueCount   Number of values in the vector.
   */
  public void allocateNew(int totalBytes, int valueCount);
  
  /**
   * Provide the maximum amount of variable width bytes that can be stored int his vector.
   * @return
   */
  public int getByteCapacity();
  
  /**
   * Load the records in the provided buffer based on the given number of values.
   * @param dataBytes   The number of bytes associated with the data array.
   * @param valueCount Number of values the buffer contains.
   * @param buf Incoming buffer.
   * @return The number of bytes of the buffer that were consumed.
   */
  public int load(int dataBytes, int valueCount, ByteBuf buf);
  
  public abstract NonRepeatedMutator getMutator();
}
