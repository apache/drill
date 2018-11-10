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

/**
 * This package contains an Apache Drill format plugin capable of reading
 * msgpack files. <br>
 * The specification for the msgpack file format is found at
 * <a href="https://github.com/msgpack/msgpack/blob/master/spec.md">msgpack
 * spec</a> <br>
 * This format plubin uses a Java library to read the msgpack files.
 * <a href="https://github.com/msgpack/msgpack-java">msgpack-java</a> This
 * library is capable of reading all of the msgpack data types
 * <ul>
 * <li>nil format
 * <li>bool format family
 * <li>int format family
 * <li>float format family
 * <li>str format family
 * <li>bin format family
 * <li>array format family
 * <li>map format family
 * <li>ext format family
 * </ul>
 * <br>
 * The ext format provides a means of providing third party data encodings. For
 * example msgpack defines an extended type for timestamps. <a href=
 * "https://github.com/msgpack/msgpack/blob/master/spec.md#timestamp-extension-type">
 * timestamp extension type</a> <br>
 * This msgpack format plugin implements the timestamp extended type via the
 * class
 * {@link org.apache.drill.exec.store.msgpack.valuewriter.TimestampValueWriter}
 * It's an implementation of
 * {@link org.apache.drill.exec.store.msgpack.valuewriter.ExtensionValueWriter}.
 * These implemenations are discovered via the Java Plugin Service.
 * <p>
 * Here's an example of using the msgpack java library to write a file and to
 * read it back.
 * </p>
 * <code>
 *     try (MessagePacker packer = MessagePack.newDefaultPacker(new FileOutputStream(new File("test.mp")))) {
      // Alice, write map with 3 fields.
      packer.packMapHeader(3);
      packer.packString("name");
      packer.packString("Alice");
      packer.packString("age");
      packer.packInt(21);
      packer.packString("height");
      packer.packFloat(1.67f);
      // Bob, write map with 3 fields.
      packer.packMapHeader(3);
      packer.packString("name");
      packer.packString("Bog");
      packer.packString("age");
      packer.packInt(32);
      packer.packString("height");
      packer.packFloat(1.79f);
    }

    try (MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(new FileInputStream(new File("test.mp")))) {
      while (unpacker.hasNext()) {
        ImmutableValue msg = unpacker.unpackValue();
        ValueType type = msg.getValueType();
        if (type == ValueType.MAP) {
          System.out.println("Printing message");
          MapValue mapValue = msg.asMapValue();
          Set<Map.Entry<Value, Value>> valueEntries = mapValue.entrySet();
          for (Map.Entry<Value, Value> valueEntry : valueEntries) {
            Value key = valueEntry.getKey();
            Value value = valueEntry.getValue();
            String k = key.asStringValue().asString();
            if (value.getValueType() == ValueType.STRING) {
              String strValue = value.asStringValue().toString();
              System.out.println(String.format("key: '%s' value: '%s'", k, strValue));
            } else if (value.getValueType() == ValueType.INTEGER) {
              int intValue = value.asNumberValue().toInt();
              System.out.println(String.format("key: '%s' value: '%d'", k, intValue));
            } else if (value.getValueType() == ValueType.FLOAT) {
              float fValue = value.asFloatValue().toFloat();
              System.out.println(String.format("key: '%s' value: '%f'", k, fValue));
            }
          }
        }
      }
    }
  }

 * </code>
 */
package org.apache.drill.exec.store.msgpack;
