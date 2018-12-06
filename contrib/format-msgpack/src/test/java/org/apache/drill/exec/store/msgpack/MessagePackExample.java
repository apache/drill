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

  public static class FieldEntryByteBuffer {
    public ByteBuffer keyBytes;
    public String keyString;

    public FieldEntryByteBuffer(ByteBuffer b, String s) {
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

  static Map<ByteBuffer, String> bytebufferEntries = new HashMap<>();

  public static void main(String[] args) throws Exception {

    for (int i = 0; i < 1; i++) {

      try (MessagePacker packer = MessagePack.newDefaultPacker(new FileOutputStream(new File("test" + i + ".mp")))) {
        for (int j = 0; j < 5_000_000; j++) {
          writeCompleteModel(packer);
        }
      }
    }
    for (int k = 0; k < 10; k++) {
      fieldTracker();
      // value();
      // valueMap();
      // bytes();
      // byteList();
      // byteMap();

      // byteBufferList();
      //bytebufferMap();
    }
  }

  public static void value() throws Exception {
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
            String k = null;

            if (k == null) {
              k = key.asStringValue().asString();
            }
            Value value = valueEntry.getValue();
          }
        }
      }
    }
    System.out.println("reading decode string took " + s.elapsed(TimeUnit.MILLISECONDS));
  }

  public static void valueMap() throws Exception {
    Stopwatch s = Stopwatch.createStarted();

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
            String k = null;

            StringValue sv = fieldNamesMap.get(key);
            if (sv != null) {
              k = sv.asString();
            } else {
              k = key.asStringValue().asString();
              fieldNamesMap.put(key.asStringValue(), key.asStringValue());
            }
            Value value = valueEntry.getValue();
          }
        }
      }
    }
    System.out.println("decode string, map optimization " + s.elapsed(TimeUnit.MILLISECONDS));
  }

  public static void bytes() throws Exception {
    Stopwatch s = Stopwatch.createStarted();
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
            String k = null;

            if (k == null) {
              k = new String(keyBytes, CharsetNames.UTF_8);
            }
            unpacker.skipValue();
            i++;
          }
        }
      }
    }

    System.out.println("byte took       " + s.elapsed(TimeUnit.MILLISECONDS));
  }

  public static void byteList() throws Exception {
    Stopwatch s = Stopwatch.createStarted();
    List<FieldEntry> fieldEntries = new ArrayList<>();
    try (MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(new FileInputStream(new File("test0.mp")))) {
      int lastFieldMatched = 0;
      while (unpacker.hasNext()) {
        MessageFormat mf = unpacker.getNextFormat();
        ValueType type = mf.getValueType();
        if (type == ValueType.MAP) {
          int size = unpacker.unpackMapHeader();
          for (int i = 0; i < size * 2;) {
            int keySize = unpacker.unpackRawStringHeader();
            byte[] keyBytes = unpacker.readPayload(keySize);
            i++;

            String k = null;
            if (fieldEntries.size() > 0) {
              int numEntries = fieldEntries.size();
              int upperBound = numEntries + lastFieldMatched;
              int startBound = (lastFieldMatched + 1) % numEntries;
              for (int j = startBound; j < upperBound; j++) {
                int idx = j % numEntries;
                FieldEntry f = fieldEntries.get(idx);
                if (Arrays.equals(f.keyBytes, keyBytes)) {
                  k = f.keyString;
                  // System.out.println(idx);
                  lastFieldMatched = idx;
                  break;
                }
              }
            }
            if (k == null) {
              k = new String(keyBytes, CharsetNames.UTF_8);
              // System.out.println("adding key: " + k);
              fieldEntries.add(new FieldEntry(keyBytes, k));
            }
            unpacker.skipValue();
            i++;
          }
        }
      }
    }

    System.out.println("byte list       " + s.elapsed(TimeUnit.MILLISECONDS));
  }

  public static void byteMap() throws Exception {
    Stopwatch s = Stopwatch.createStarted();

    Map<KeyWrapper, String> mapEntries = new HashMap<>();
    KeyWrapper wrapper = new KeyWrapper(null);
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

            wrapper.keyBytes = keyBytes;
            String k = mapEntries.get(wrapper);
            if (k == null) {
              k = new String(keyBytes, CharsetNames.UTF_8);
              // System.out.println("adding key: " + k);
              mapEntries.put(new KeyWrapper(keyBytes), k);
            }
            unpacker.skipValue();
            i++;
          }
        }
      }
    }

    System.out.println("byte map        " + s.elapsed(TimeUnit.MILLISECONDS));
  }

  public static void byteBufferList() throws Exception {
    Stopwatch s = Stopwatch.createStarted();

    List<FieldEntryByteBuffer> list = new ArrayList<>();
    try (MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(new FileInputStream(new File("test0.mp")))) {
      int lastFieldMatched = 0;
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

            String k = null;
            int numEntries = list.size();
            if (numEntries > 0) {
              int upperBound = numEntries + lastFieldMatched;
              int startBound = (lastFieldMatched + 1) % numEntries;
              for (int j = startBound; j < upperBound; j++) {
                int idx = j % numEntries;
                FieldEntryByteBuffer f = list.get(idx);
                if (f.keyBytes.equals(buff)) {
                  k = f.keyString;
                  // System.out.println(idx);
                  lastFieldMatched = idx;
                  break;
                }
              }
            }
            if (k == null) {
              byte[] theBytes = keyBytes.toByteArray();
              k = new String(theBytes, CharsetNames.UTF_8);
              // System.out.println("adding key: " + k);
              ByteBuffer bb = ByteBuffer.wrap(theBytes);
              list.add(new FieldEntryByteBuffer(bb, k));
            }

            unpacker.skipValue();
            i++;
          }
        }
      }
    }

    System.out.println("bytebuffer list " + s.elapsed(TimeUnit.MILLISECONDS));
  }

  public static void unpackValue(FieldPathTracker tracker, MessageUnpacker unpacker) throws Exception {

    MessageFormat mf = unpacker.getNextFormat();
    ValueType type = mf.getValueType();
    switch (type) {
    case MAP: {
      try {
        tracker.enterMap();
        // System.out.println(tracker.toString());

        int size = unpacker.unpackMapHeader();
        for (int i = 0; i < size * 2;) {
          int keySize = unpacker.unpackRawStringHeader();
          MessageBuffer keyBytes = unpacker.readPayloadAsReference(keySize);
          ByteBuffer buff = keyBytes.sliceAsByteBuffer();
          String fieldName = tracker.select(buff);
          i++;

          unpackValue(tracker, unpacker);
          i++;
        }
      } finally {
        tracker.leaveMap();
      }
    }
      break;
    case ARRAY: {
      try {
        tracker.enterArray();
        // System.out.println(tracker.toString());
        int arraySize = unpacker.unpackArrayHeader();
        for (int i = 0; i < arraySize; i++) {
          unpackValue(tracker, unpacker);
        }
      } finally {
        tracker.leaveArray();
      }
    }
      break;
    case BINARY:
    case BOOLEAN:
    case EXTENSION:
    case FLOAT:
    case INTEGER:
    case NIL:
    case STRING: {
      // System.out.println(tracker.toString());
      unpacker.skipValue();
    }
      break;
    }
  }

  public static void fieldTracker() throws Exception {
    Stopwatch s = Stopwatch.createStarted();

    FieldPathTracker tracker = new FieldPathTracker();

    try (MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(new FileInputStream(new File("test0.mp")))) {
      while (unpacker.hasNext()) {
        unpackValue(tracker, unpacker);
      }
    }

    System.out.println("fieldTracker     " + s.elapsed(TimeUnit.MILLISECONDS));
  }

  public static void unpackValueSimpleMap(Map<ByteBuffer, String> bytebufferEntries, MessageUnpacker unpacker)
      throws Exception {

    MessageFormat mf = unpacker.getNextFormat();
    ValueType type = mf.getValueType();
    switch (type) {
    case MAP: {
      int size = unpacker.unpackMapHeader();
      for (int i = 0; i < size * 2;) {
        int keySize = unpacker.unpackRawStringHeader();
        MessageBuffer keyBytes = unpacker.readPayloadAsReference(keySize);
        ByteBuffer buff = keyBytes.sliceAsByteBuffer();
        i++;

        String k = bytebufferEntries.get(buff);
        if (k == null) {
          k = new String(keyBytes.toByteArray(), CharsetNames.UTF_8);
          // System.out.println("adding key: " + k);
          ByteBuffer bb = ByteBuffer.wrap(keyBytes.toByteArray());
          bytebufferEntries.put(bb, k);
        }
        unpackValueSimpleMap(bytebufferEntries, unpacker);
        i++;
      }
    }
      break;
    case ARRAY: {
      // System.out.println(tracker.toString());
      int arraySize = unpacker.unpackArrayHeader();
      for (int i = 0; i < arraySize; i++) {
        unpackValueSimpleMap(bytebufferEntries, unpacker);
      }
    }
      break;
    case BINARY:
    case BOOLEAN:
    case EXTENSION:
    case FLOAT:
    case INTEGER:
    case NIL:
    case STRING: {
      unpacker.skipValue();
    }
      break;
    }
  }

  public static void bytebufferMap() throws Exception {
    Stopwatch s = Stopwatch.createStarted();

    Map<ByteBuffer, String> bytebufferEntries = new HashMap<>(64);

    try (MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(new FileInputStream(new File("test0.mp")))) {
      while (unpacker.hasNext()) {
        unpackValueSimpleMap(bytebufferEntries, unpacker);
      }
    }

    System.out.println("bytebuffer map  " + s.elapsed(TimeUnit.MILLISECONDS));
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
    packer.packMapHeader(16 - 2);

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
    // packer.packString("arrayOfMap");
    // packer.packArrayHeader(2);
    // packer.packMapHeader(10);
    // 10 sub fields
    // addAllFieldTypes(packer);
    // packer.packMapHeader(10);
    // 10 sub fields
    // addAllFieldTypes(packer);

    // 2
    // packer.packString("mapOfMapa");
    // packer.packMapHeader(1).packString("aMap").packMapHeader(10);
    // 10 sub fields
    // addAllFieldTypes(packer);

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