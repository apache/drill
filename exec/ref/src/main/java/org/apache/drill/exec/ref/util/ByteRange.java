package org.apache.drill.exec.ref.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;


/**
 * Facade interface for ByteBuffer and byte[]
 */
public interface ByteRange {
  public byte getByte(int index);
  public int getLength();
  public void copyTo(byte[] buffer, int offset);
  public void copyTo(InputStream is) throws IOException;
  public void copyTo(ByteBuffer bb);
}
