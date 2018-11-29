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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.compress.utils.CharsetNames;
import org.apache.drill.shaded.guava.com.google.common.base.Stopwatch;
import org.msgpack.core.MessageFormat;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.core.buffer.MessageBuffer;
import org.msgpack.value.ImmutableValue;
import org.msgpack.value.IntegerValue;
import org.msgpack.value.MapValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueType;
import org.msgpack.value.StringValue;

/**
 * This class describes the usage of MessagePack
 */
public class MessagePackExample {
  public static File destinationFolder = new File("/tmp");

  public static class FieldEntry {
    public byte[] keyBytes;
    public String keyString;

    public FieldEntry(byte[] b, String s) {
      this.keyBytes = b;
      this.keyString = s;
    }
  }

  public static class KeyWrapper {
    public byte[] keyBytes;

    public KeyWrapper(byte[] b) {
      this.keyBytes = b;
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(keyBytes);
    }

    @Override
    public boolean equals(Object obj) {
      KeyWrapper w = (KeyWrapper) obj;
      return Arrays.equals(keyBytes, w.keyBytes);
    }
  }

  private MessagePackExample() {
  }

  public static void main(String[] args) throws Exception {

    for (int i = 0; i < 1; i++) {

      try (MessagePacker packer = MessagePack.newDefaultPacker(new FileOutputStream(new File("test" + i + ".mp")))) {
        for (int j = 0; j < 1_000; j++) {
          writeCompleteModel(packer);
        }
      }
    }
    for (int k = 0; k < 11; k++) {
      runTest();
    }
  }

  public static void runTest() throws Exception {
    Stopwatch s = Stopwatch.createStarted();

    s = Stopwatch.createStarted();
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
            String k = null;

            if (k == null) {
              k = key.asStringValue().asString();
            }
          }
        }
      }
    }
    System.out.println("reading no optimization took " + s.elapsed(TimeUnit.MILLISECONDS));

    s = Stopwatch.createStarted();
    List<StringValue> fieldNames = new ArrayList<>();

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
            String k = null;

            for (int i = 0; i < fieldNames.size(); i++) {
              StringValue sv = fieldNames.get(i);
              if (sv.equals(key)) {
                k = sv.asString();
                break;
              }
            }

            if (k == null) {
              k = key.asStringValue().asString();
              fieldNames.add(key.asStringValue());
            }
          }
        }
      }
    }
    System.out.println("reading list optimization took " + s.elapsed(TimeUnit.MILLISECONDS));

    s = Stopwatch.createStarted();

    Map<StringValue, StringValue> fieldNamesMap = new HashMap<>();

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
            String k = null;

            StringValue sv = fieldNamesMap.get(key);
            if (sv != null) {
              k = sv.asString();
            } else {
              k = key.asStringValue().asString();
              fieldNamesMap.put(key.asStringValue(), key.asStringValue());
            }
          }
        }
      }
    }
    System.out.println("reading map optimization " + s.elapsed(TimeUnit.MILLISECONDS));

    s = Stopwatch.createStarted();
    try (MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(new FileInputStream(new File("test0.mp")))) {
      while (unpacker.hasNext()) {
        MessageFormat mf = unpacker.getNextFormat();
        ValueType type = mf.getValueType();
        if (type == ValueType.MAP) {
          int size = unpacker.unpackMapHeader();
          for (int i = 0; i < size * 2;) {
            int keySize = unpacker.unpackRawStringHeader();
            byte[] keyBytes = unpacker.readPayload(keySize);
            // Value key = unpacker.unpackValue();
            i++;
            Value value = unpacker.unpackValue();
            i++;
            String k = null;

            if (k == null) {
              k = new String(keyBytes, CharsetNames.UTF_8);
            }
          }
        }
      }
    }

    System.out.println("reading with format optimization took " + s.elapsed(TimeUnit.MILLISECONDS));

    List<FieldEntry> fieldEntries = new ArrayList<>();
    s = Stopwatch.createStarted();
    try (MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(new FileInputStream(new File("test0.mp")))) {
      while (unpacker.hasNext()) {
        MessageFormat mf = unpacker.getNextFormat();
        ValueType type = mf.getValueType();
        if (type == ValueType.MAP) {
          int size = unpacker.unpackMapHeader();
          for (int i = 0; i < size * 2;) {
            int keySize = unpacker.unpackRawStringHeader();
            byte[] keyBytes = unpacker.readPayload(keySize);
            i++;
            Value value = unpacker.unpackValue();
            i++;

            String k = null;
            for (int j = 0; j < fieldEntries.size(); j++) {
              FieldEntry f = fieldEntries.get(j);
              if (Arrays.equals(f.keyBytes, keyBytes)) {
                k = f.keyString;
                break;
              }
            }
            if (k == null) {
              k = new String(keyBytes, CharsetNames.UTF_8);
              fieldEntries.add(new FieldEntry(keyBytes, k));
            }
          }
        }
      }
    }

    System.out.println("reading with format and list optimization took " + s.elapsed(TimeUnit.MILLISECONDS));

    Map<KeyWrapper, String> mapEntries = new HashMap<>();
    KeyWrapper wrapper = new KeyWrapper(null);
    s = Stopwatch.createStarted();
    try (MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(new FileInputStream(new File("test0.mp")))) {
      while (unpacker.hasNext()) {
        MessageFormat mf = unpacker.getNextFormat();
        ValueType type = mf.getValueType();
        if (type == ValueType.MAP) {
          int size = unpacker.unpackMapHeader();
          for (int i = 0; i < size * 2;) {
            int keySize = unpacker.unpackRawStringHeader();
            byte[] keyBytes = unpacker.readPayload(keySize);
            i++;
            Value value = unpacker.unpackValue();
            i++;

            wrapper.keyBytes = keyBytes;
            String k = mapEntries.get(wrapper);
            if (k == null) {
              k = new String(keyBytes, CharsetNames.UTF_8);
              mapEntries.put(new KeyWrapper(keyBytes), k);
            }
          }
        }
      }
    }

    System.out.println("reading with format and map optimization took " + s.elapsed(TimeUnit.MILLISECONDS));

    Map<ByteBuffer, String> bytebufferEntries = new HashMap<>();
    s = Stopwatch.createStarted();
    try (MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(new FileInputStream(new File("test0.mp")))) {
      while (unpacker.hasNext()) {
        MessageFormat mf = unpacker.getNextFormat();
        ValueType type = mf.getValueType();
        if (type == ValueType.MAP) {
          int size = unpacker.unpackMapHeader();
          for (int i = 0; i < size * 2;) {
            int keySize = unpacker.unpackRawStringHeader();
            MessageBuffer keyBytes = unpacker.readPayloadAsReference(keySize);
            ByteBuffer buff = keyBytes.sliceAsByteBuffer();
            i++;
            Value value = unpacker.unpackValue();
            i++;

            String k = bytebufferEntries.get(buff);
            if (k == null) {
              k = new String(buff.array(), CharsetNames.UTF_8);
              ByteBuffer bb = ByteBuffer.wrap(keyBytes.toByteArray());
              bytebufferEntries.put(bb, k);
            }
          }
        }
      }
    }

    System.out.println("reading with format and map and buffer optimization took " + s.elapsed(TimeUnit.MILLISECONDS));

  }

  private static String getFieldName(Value v) {

    String fieldName = null;

    ValueType valueType = v.getValueType();
    switch (valueType) {
    case STRING:
      // not using StringValue.asString() because it does decoding slower than the
      // java String constructor
      fieldName = v.asStringValue().asString();
      break;
    case BINARY:
      byte[] bytes = v.asBinaryValue().asByteArray();
      fieldName = new String(bytes);
      break;
    case INTEGER:
      IntegerValue iv = v.asIntegerValue();
      fieldName = iv.toString();
      break;
    case ARRAY:
    case BOOLEAN:
    case MAP:
    case FLOAT:
    case EXTENSION:
    case NIL:
      break;
    default:
      throw new RuntimeException("UnSupported msgpack type: " + valueType);
    }
    return fieldName;
  }

  private static String getFieldName2(Value v) {

    String fieldName = null;

    ValueType valueType = v.getValueType();
    if (valueType == ValueType.STRING) {
      // not using StringValue.asString() because it does decoding slower than the
      // java String constructor
      fieldName = v.asStringValue().asString();
    } else if (valueType == ValueType.BINARY) {
      byte[] bytes = v.asBinaryValue().asByteArray();
      fieldName = new String(bytes);
    } else if (valueType == ValueType.INTEGER) {
      IntegerValue iv = v.asIntegerValue();
      fieldName = iv.toString();
    } else {
      fieldName = null;
    }
    return fieldName;
  }

  private static String getFieldName3(Value v) {

    String fieldName = null;

    if (v.isStringValue()) {
      // not using StringValue.asString() because it does decoding slower than the
      // java String constructor
      fieldName = new String(v.asStringValue().asByteArray(), StandardCharsets.UTF_8);
    } else if (v.isBinaryValue()) {
      byte[] bytes = v.asBinaryValue().asByteArray();
      fieldName = new String(bytes);
    } else if (v.isIntegerValue()) {
      IntegerValue iv = v.asIntegerValue();
      fieldName = iv.toString();
    } else {
      fieldName = null;
    }
    return fieldName;
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