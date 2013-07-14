<@pp.dropOutputFile />
<#list types as type>
<#list type.minor as minor>

<#if type.major == "VarLen">
<@pp.changeOutputFile name="${minor.class}Vector.java" />
package org.apache.drill.exec.vector;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import io.netty.buffer.ByteBuf;

import java.io.Closeable;
import java.util.Random;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.SchemaDefProtos;
import org.apache.drill.exec.proto.UserBitShared.FieldMetadata;
import org.apache.drill.exec.record.DeadBuf;
import org.apache.drill.exec.record.MaterializedField;

/**
 * ${minor.class}Vector implements a vector of variable width values.  Elements in the vector
 * are accessed by position from the logical start of the vector.  A fixed width lengthVector
 * is used to convert an element's position to it's offset from the start of the (0-based)
 * ByteBuf.  Size is inferred by adjacent elements.
 *   The width of each element is ${type.width} byte(s)
 *   The equivalent Java primitive is '${minor.javaType!type.javaType}'
 *
 * NB: this class is automatically generated from ValueVectorTypes.tdd using FreeMarker.
 */
@SuppressWarnings("unused")
public final class ${minor.class}Vector extends ValueVector {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(${minor.class}Vector.class);

  private final UInt${type.width}Vector lengthVector;
  private final UInt${type.width}Vector.Mutator lengthVectorMutator;

  public ${minor.class}Vector(MaterializedField field, BufferAllocator allocator) {
    super(field, allocator);
    this.lengthVector = new UInt${type.width}Vector(null, allocator);
    this.lengthVectorMutator = lengthVector.getMutator();
  }

  public byte[] get(int index) {
    checkArgument(index >= 0);
    int startIdx = 0;
    int size = 0;
    if (index == 0) {
      size = lengthVector.get(1);
    } else {
      startIdx = lengthVector.get(index);
      size = lengthVector.get(index + 1) - startIdx;
    }
    checkState(size >= 0);
    byte[] dst = new byte[size];
    data.getBytes(startIdx, dst, 0, size);
    return dst;
  }

  @Override
  public int getAllocatedSize() {
    return lengthVector.getAllocatedSize() + totalBytes;
  }

  /**
   * Get the size requirement (in bytes) for the given number of values.  Only accurate
   * for fixed width value vectors.
   */
  public int getSizeFromCount(int valueCount) {
    return valueCount * ${type.width};
  }

  @Override
  protected void clear() {
    super.clear();
    lengthVector.clear();
  }

  /**
   * Allocate a new memory space for this vector.  Must be called prior to using the ValueVector.
   *
   * @param valueCount
   *          The number of values which can be contained within this vector.
   */
  public void allocateNew(int totalBytes, ByteBuf sourceBuffer, int valueCount) {
    super.allocateNew(totalBytes, sourceBuffer, valueCount);
    lengthVector.allocateNew(valueCount);
  }

  @Override
  public ByteBuf[] getBuffers() {
    return new ByteBuf[]{lengthVector.data, data};
  }

  public Object getObject(int index) {
    return get(index);
  }

  public Mutator getMutator() {
    return new Mutator();
  }
  
  
  /**
   * Mutable${minor.class} implements a vector of variable width values.  Elements in the vector
   * are accessed by position from the logical start of the vector.  A fixed width lengthVector
   * is used to convert an element's position to it's offset from the start of the (0-based)
   * ByteBuf.  Size is inferred by adjacent elements.
   *   The width of each element is ${type.width} byte(s)
   *   The equivalent Java primitive is '${minor.javaType!type.javaType}'
   *
   * NB: this class is automatically generated from ValueVectorTypes.tdd using FreeMarker.
   */
  public class Mutator implements ValueVector.Mutator{

    /**
     * Set the variable length element at the specified index to the supplied byte array.
     *
     * @param index   position of the bit to set
     * @param bytes   array of bytes to write
     */
    public void set(int index, byte[] bytes) {
      checkArgument(index >= 0);
      if (index == 0) {
        lengthVectorMutator.set(0, 0);
        lengthVectorMutator.set(1, bytes.length);
        data.setBytes(0, bytes);
      } else {
        int currentOffset = lengthVector.get(index);
        // set the end offset of the buffer
        lengthVectorMutator.set(index + 1, currentOffset + bytes.length);
        data.setBytes(currentOffset, bytes);
      }
    }

    @Override
    public void setRecordCount(int recordCount) {
      ${minor.class}Vector.this.setRecordCount(recordCount);
      lengthVector.setRecordCount(recordCount);
    }

    @Override
    public void randomizeData(){}
  }
  
}


</#if> <#-- type.major -->
</#list>
</#list>