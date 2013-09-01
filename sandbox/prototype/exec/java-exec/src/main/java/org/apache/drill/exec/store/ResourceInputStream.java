package org.apache.drill.exec.store;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;

import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

public class ResourceInputStream extends ByteArrayInputStream implements Seekable, PositionedReadable {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ResourceInputStream.class);


  public ResourceInputStream(byte[] bytes) {
    super(bytes);
  }

  
  

  @Override
  public void readFully(long position, byte[] buffer) throws IOException {
    int l = read(position, buffer, 0, buffer.length);
    if (l < buffer.length) {
      throw new EOFException();
    }
  }

  public int read(long position, byte b[], int off, int len) {
    int start = (int) position;
    if (b == null) {
        throw new NullPointerException();
    } else if (off < 0 || len < 0 || len > b.length - off) {
        throw new IndexOutOfBoundsException();
    }

    if (start >= count) {
        return -1;
    }

    int avail = count - start;
    if (len > avail) {
        len = avail;
    }
    if (len <= 0) {
        return 0;
    }
    System.arraycopy(buf, start, b, off, len);
    return len;
}
  
  @Override
  public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
    int l = read(position, buffer, offset, length);
    if (l < length) throw new EOFException();
  }

  @Override
  public long getPos() throws IOException {
    return pos;
  }

  
  @Override
  public int read(byte[] b) throws IOException {
    int l = read(pos, b, 0, b.length);
    pos += l;
    return l;
  }

  @Override
  public boolean seekToNewSource(long arg0) throws IOException {
    seek(arg0);
    return true;
  }




  @Override
  public void seek(long arg0) throws IOException {
    if(buf.length > arg0){
      pos = (int) arg0;
    }else{
      throw new EOFException();
    }
  }

}
