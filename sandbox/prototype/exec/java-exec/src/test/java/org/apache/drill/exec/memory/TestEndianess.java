package org.apache.drill.exec.memory;

import static org.junit.Assert.assertEquals;
import io.netty.buffer.ByteBuf;

import org.apache.drill.exec.memory.DirectBufferAllocator;
import org.junit.Test;



public class TestEndianess {
  
  @Test
  public void testLittleEndian(){
    DirectBufferAllocator a = new DirectBufferAllocator();
    ByteBuf b = a.buffer(4);
    b.setInt(0, 35);
    assertEquals((int) b.getByte(0), 35);
    assertEquals((int) b.getByte(1), 0);
    assertEquals((int) b.getByte(2), 0);
    assertEquals((int) b.getByte(3), 0);
  }
  
}
