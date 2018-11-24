/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.msgpack;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.drill.shaded.guava.com.google.common.base.Stopwatch;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ImmutableValue;
import org.msgpack.value.MapValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueType;

/**
 * This class describes the usage of MessagePack
 */
public class MessagePackExample {
  public static File destinationFolder = new File("/tmp");

  private MessagePackExample() {
  }

  public static void main(String[] args) throws Exception {

    for (int i = 0; i < 1; i++) {

      try (MessagePacker packer = MessagePack.newDefaultPacker(new FileOutputStream(new File("test" + i + ".mp")))) {
        for (int j = 0; j < 1_000_000; j++) {
          writeCompleteModel(packer);
          // // Alice, write map with 3 fields.
          // packer.packMapHeader(3);
          // packer.packString("name");
          // packer.packString("Alice");
          // packer.packString("age");
          // packer.packInt(21);
          // packer.packString("height");
          // packer.packFloat(1.67f);
          // // Bob, write map with 3 fields.
          // packer.packMapHeader(3);
          // packer.packString("name");
          // packer.packString("Bog");
          // packer.packString("age");
          // packer.packInt(32);
          // packer.packString("height");
          // packer.packFloat(1.79f);
        }
      }
    }

    Stopwatch s = Stopwatch.createStarted();

    try (MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(new FileInputStream(new File("test0.mp")))) {
      while (unpacker.hasNext()) {
        ImmutableValue msg = unpacker.unpackValue();
        ValueType type = msg.getValueType();
        if (type == ValueType.MAP) {
          // Sdystem.out.println("Printing message");
          MapValue mapValue = msg.asMapValue();
          Set<Map.Entry<Value, Value>> valueEntries = mapValue.entrySet();
          for (Map.Entry<Value, Value> valueEntry : valueEntries) {
            Value key = valueEntry.getKey();
            Value value = valueEntry.getValue();
            String k = key.asStringValue().asString();
            if (value.getValueType() == ValueType.STRING) {
              String strValue = value.asStringValue().toString();
              // System.out.println(String.format("key: '%s' value: '%s'", k, strValue));
            } else if (value.getValueType() == ValueType.INTEGER) {
              int intValue = value.asNumberValue().toInt();
              // System.out.println(String.format("key: '%s' value: '%d'", k, intValue));
            } else if (value.getValueType() == ValueType.FLOAT) {
              float fValue = value.asFloatValue().toFloat();
              // System.out.println(String.format("key: '%s' value: '%f'", k, fValue));
            }
          }
        }
      }
    }
    System.out.println("reading took " + s.elapsed(TimeUnit.MILLISECONDS));
  }

  public static MessagePacker makeMessagePacker(String fileName) throws IOException {
    File dst = new File(destinationFolder.getPath() + File.separator + fileName);
    return MessagePack.newDefaultPacker(new FileOutputStream(dst));
  }

  public static MessageUnpacker makeMessageUnpacker(String fileName) throws IOException {
    File dst = new File(destinationFolder.getPath() + File.separator + fileName);
    return MessagePack.newDefaultUnpacker(new FileInputStream(dst));
  }

  public static void write2Batchs() throws IOException {
    MessagePacker packer = makeMessagePacker("twoBatches.mp");

    for (int i = 0; i < 4096; i++) {
      packer.packMapHeader(1);
      packer.packString("int").packInt(0);
    }

    writeCompleteModel(packer);
    packer.close();
  }

  public static void writeTest() throws IOException {
    MessagePacker packer = makeMessagePacker("test.mp");

    writeCompleteModel(packer);

    packer.close();
  }

  private static void writeCompleteModel(MessagePacker packer) throws IOException {
    packer.packMapHeader(16);

    // 0
    packer.packString("arrayOfArray");
    packer.packArrayHeader(2);
    packer.packArrayHeader(3);
    packer.packInt(1);
    packer.packInt(1);
    packer.packInt(1);
    packer.packArrayHeader(5);
    packer.packInt(1);
    packer.packInt(1);
    packer.packInt(1);
    packer.packInt(1);
    packer.packInt(1);
    // 1
    packer.packString("arrayOfMap");
    packer.packArrayHeader(2);
    packer.packMapHeader(10);
    // 10 sub fields
    addAllFieldTypes(packer);
    packer.packMapHeader(10);
    // 10 sub fields
    addAllFieldTypes(packer);

    // 2
    packer.packString("mapOfMap");
    packer.packMapHeader(1).packString("aMap").packMapHeader(10);
    // 10 sub fields
    addAllFieldTypes(packer);

    // 3
    packer.packString("mapWithArray");
    packer.packMapHeader(2);
    packer.packString("anArray").packArrayHeader(2).packString("v1").packString("v2");
    packer.packString("aString").packString("x");

    // 4
    packer.packString("arrayWithMap");
    packer.packArrayHeader(2);
    packer.packMapHeader(1).packString("x").packInt(1);
    packer.packMapHeader(1).packString("y").packFloat(2.2f);

    // 10 + 4
    addAllFieldTypes(packer);
    // 15
    packer.packString("arrayOfInt");
    packer.packArrayHeader(2);
    packer.packInt(1);
    packer.packInt(1);
  }

  private static void addAllFieldTypes(MessagePacker packer) throws IOException {
    byte[] bytes = "some data".getBytes();
    packer.packString("raw").packRawStringHeader(bytes.length).addPayload(bytes);

    byte[] binary = { 0x1, 0x2, 0x9, 0xF };
    packer.packString("bin").packBinaryHeader(binary.length).addPayload(binary);

    packer.packString("int").packInt(32_000_000);
    packer.packString("big").packBigInteger(BigInteger.valueOf(64_000_000_000L));
    packer.packString("byt").packByte((byte) 129);
    packer.packString("lon").packLong(64_000_000_000L);
    packer.packString("nil").packNil();
    packer.packString("sho").packShort((short) 222);
    packer.packString("dou").packDouble(1.1d);
    packer.packString("flo").packFloat(1.1f);
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