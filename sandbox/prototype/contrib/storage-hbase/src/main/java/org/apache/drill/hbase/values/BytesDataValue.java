package org.apache.drill.hbase.values;

import org.apache.drill.common.expression.types.DataType;
import org.apache.drill.exec.ref.rops.DataWriter;
import org.apache.drill.exec.ref.values.BaseDataValue;
import org.apache.drill.exec.ref.values.BytesValue;
import org.apache.drill.exec.ref.values.DataValue;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author David Alves
 */
public class BytesDataValue extends BaseDataValue implements BytesValue {

  private final byte[] bytes;

  public BytesDataValue(byte[] bytes) {
    this.bytes = bytes;
  }

  @Override
  public void write(DataWriter writer) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public DataType getDataType() {
    return DataType.BYTES;
  }

  @Override
  public boolean equals(DataValue other) {
    if (other instanceof BytesDataValue) {
      return Arrays.equals(bytes, ((BytesDataValue) other).getAsArray());
    }
    return false;
  }

  @Override
  public int hashCode() {
    return bytes.hashCode();
  }

  @Override
  public DataValue copy() {
    return null;
  }

  @Override
  public byte[] getAsArray() {
    return bytes;
  }

  @Override
  public int getLength() {
    return bytes.length;
  }

  @Override
  public byte get(int pos) {
    return bytes[pos];
  }

  @Override
  public boolean supportsCompare(DataValue other) {
    return false;
  }

  @Override
  public int compareTo(DataValue other) {
    return 0;
  }
}
