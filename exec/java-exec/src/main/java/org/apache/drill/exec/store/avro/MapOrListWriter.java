/**
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
package org.apache.drill.exec.store.avro;

import org.apache.drill.exec.vector.complex.writer.BaseWriter;
import org.apache.drill.exec.vector.complex.writer.BigIntWriter;
import org.apache.drill.exec.vector.complex.writer.BitWriter;
import org.apache.drill.exec.vector.complex.writer.Float4Writer;
import org.apache.drill.exec.vector.complex.writer.Float8Writer;
import org.apache.drill.exec.vector.complex.writer.IntWriter;
import org.apache.drill.exec.vector.complex.writer.VarBinaryWriter;
import org.apache.drill.exec.vector.complex.writer.VarCharWriter;

/**
 * Impersonates a map writer or a list writer depending on construction type.
 * Perhaps this is a tragic misuse of polymorphism?
 */
public class MapOrListWriter {

  final BaseWriter.MapWriter map;
  final BaseWriter.ListWriter list;

  MapOrListWriter(final BaseWriter.MapWriter writer) {
    this.map = writer;
    this.list = null;
  }

  MapOrListWriter(final BaseWriter.ListWriter writer) {
    this.map = null;
    this.list = writer;
  }

  void start() {
    if (map != null) {
      map.start();
    } else {
      list.startList();
    }
  }

  void end() {
    if (map != null) {
      map.end();
    } else {
      list.endList();
    }
  }

  MapOrListWriter map(final String name) {
    assert map != null;
    return new MapOrListWriter(map.map(name));
  }

  MapOrListWriter listoftmap(final String name) {
    assert list != null;
    return new MapOrListWriter(list.map());
  }

  MapOrListWriter list(final String name) {
    assert map != null;
    return new MapOrListWriter(map.list(name));
  }

  boolean isMapWriter() {
    return map != null;
  }

  boolean isListWriter() {
    return list != null;
  }

  VarCharWriter varChar(final String name) {
    return (map != null) ? map.varChar(name) : list.varChar();
  }

  IntWriter integer(final String name) {
    return (map != null) ? map.integer(name) : list.integer();
  }

  BigIntWriter bigInt(final String name) {
    return (map != null) ? map.bigInt(name) : list.bigInt();
  }

  Float4Writer float4(final String name) {
    return (map != null) ? map.float4(name) : list.float4();
  }

  Float8Writer float8(final String name) {
    return (map != null) ? map.float8(name) : list.float8();
  }

  BitWriter bit(final String name) {
    return (map != null) ? map.bit(name) : list.bit();
  }

  VarBinaryWriter binary(final String name) {
    return (map != null) ? map.varBinary(name) : list.varBinary();
  }
}
