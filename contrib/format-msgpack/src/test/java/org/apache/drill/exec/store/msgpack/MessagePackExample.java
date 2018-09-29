//
package org.apache.drill.exec.store.msgpack;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;

import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;

/**
 * This class describes the usage of MessagePack
 */
public class MessagePackExample {
  public static File destinationFolder = new File("src/test/resources/msgpack/");

  private MessagePackExample() {
  }

  public static void main(String[] args) throws Exception {
    MessagePackExample.testBasicLarge();
  }

  public static MessagePacker makeMessagePacker(String fileName) throws IOException {
    File dst = new File(destinationFolder.getPath() + File.separator + fileName);
    return MessagePack.newDefaultPacker(new FileOutputStream(dst));
  }


  public static void testTypes() throws IOException {
    MessagePacker packer = makeMessagePacker("testTypes.mp");

    packer.packMapHeader(4);

    byte[] bytes = "some data".getBytes();
    packer.packString("raw").packRawStringHeader(bytes.length).addPayload(bytes);

    byte[] binary = {0x1, 0x2, 0x9, 0xF};
    packer.packString("bin").packBinaryHeader(binary.length).addPayload(binary);

    packer.packString("int").packInt(32_000_000);
    packer.packString("big").packBigInteger(BigInteger.valueOf(64_000_000_000L));
    packer.packString("byt").packByte((byte)129);
    packer.packString("lon").packLong(64_000_000_000L);
    packer.packString("nil").packNil();
    packer.packString("sho").packShort((short)222);
    packer.packString("dou").packDouble(1.1d);
    packer.packString("flo").packFloat(1.1f);

    packer.close();
  }

  public static void testBasicLarge() throws IOException {
    MessagePacker packer = makeMessagePacker("testBasicLarge.mp");

    for (int i = 0; i < 25_000_000; i++) {

      packer.packMapHeader(3);
      packer.packString("apple").packInt(1);
      packer.packString("banana").packInt(2);
      packer.packString("orange").packString("infini!!!");

      packer.packMapHeader(3);
      packer.packString("apple").packInt(1);
      packer.packString("banana").packInt(2);
      packer.packString("potato").packDouble(12.12);
    }

    packer.close();
  }




}