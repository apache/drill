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

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.compress.utils.CharsetNames;

public class Field {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Field.class);

  public enum FieldType {
    SCALAR, MAP, ARRAY
  };

  private FieldType type = FieldType.SCALAR;

  private final Field parent;

  private final String name;

  private Map<ByteBuffer, Field> children;

  private Field selectedField = null;

  public Field() {
    parent = null;
    name = "root";
    setMap();
  }

  public Field(Field parent) {
    this.parent = parent;
    this.name = "";
  }

  public Field(Field parent, String name) throws UnsupportedEncodingException {
    this.parent = parent;
    this.name = name;
  }

  public Field getParent() {
    return parent;
  }

  /**
   * Select a field found in a map. We try to match a previous ByteBuffer to avoid
   * creating a String which requires and the expense of decoding the bytes.
   *
   * @param aByteBuffer
   * @return
   * @throws UnsupportedEncodingException
   */
  public String select(ByteBuffer aByteBuffer) throws UnsupportedEncodingException {
    // find in children
    selectedField = children.get(aByteBuffer);
    if (selectedField == null) {
      byte[] nameBytes = new byte[aByteBuffer.remaining()];
      aByteBuffer.get(nameBytes);
      String name = new String(nameBytes, CharsetNames.UTF_8);
      selectedField = new Field(this, name);
      ByteBuffer byteBufferFieldKey = ByteBuffer.wrap(nameBytes);
      children.put(byteBufferFieldKey, selectedField);
    }
    return selectedField.getName();
  }

  public Field enterMap() {
    if (selectedField == null) {
      // we are entering a MAP from an ARRAY.
      selectedField = new Field(this);
    }
    selectedField.setMap();
    return selectedField;
  }

  private void setMap() {
    this.type = FieldType.MAP;
    // we now know this field will be a map.
    if (children == null) {
      // create the children map if it's not yet created.
      children = new HashMap<ByteBuffer, Field>();
    }

  }

  public Field enterArray() {
    if (selectedField == null) {
      // we are entering a ARRAY from an ARRAY.
      selectedField = new Field(this);
    }
    selectedField.setArray();
    return selectedField;
  }

  private void setArray() {
    this.type = FieldType.ARRAY;
  }

  public Field leaveMap() {
    return parent;
  }

  public Field leaveArray() {
    return parent;
  }

  public String getName() {
    return name;
  }

  public boolean isMap() {
    return type == FieldType.MAP;
  }

  public boolean isArray() {
    return type == FieldType.ARRAY;
  }

  @Override
  public String toString() {
    String s = getName();
    if (isArray()) {
      s += "[]";
    }
    if (selectedField != null) {
      if (!selectedField.getName().isEmpty()) {
        s += ".";
      }
      s += selectedField.toString();
    }
    return s;
  }
}
