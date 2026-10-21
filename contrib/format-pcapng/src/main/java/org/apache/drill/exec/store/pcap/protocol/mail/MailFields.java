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
package org.apache.drill.exec.store.pcap.protocol.mail;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.exec.store.pcap.protocol.DecoderContext;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;

/** Bounded lists and writer helpers shared by the mail protocol decoders. */
public final class MailFields {

  private MailFields() { }

  /** A list that keeps the first MAX_ITEMS entries and warns once when more are added. */
  public static final class Capped<T> {
    private final String name;
    private final List<T> items = new ArrayList<>();
    private boolean warned;

    public Capped(String name) {
      this.name = name;
    }

    public void add(T item, DecoderContext context) {
      if (items.size() < MailLines.MAX_ITEMS) {
        items.add(item);
      } else if (!warned) {
        warned = true;
        context.warn(name + " capped at " + MailLines.MAX_ITEMS);
      }
    }

    public void clear() {
      items.clear();
    }

    public List<T> items() {
      return items;
    }
  }

  public static void setString(TupleWriter t, String name, String value) {
    if (value != null) {
      t.scalar(name).setString(value);
    }
  }

  public static void setInt(TupleWriter t, String name, Integer value) {
    if (value != null) {
      t.scalar(name).setInt(value);
    }
  }

  public static void setLong(TupleWriter t, String name, Long value) {
    if (value != null) {
      t.scalar(name).setLong(value);
    }
  }

  public static void writeStrings(TupleWriter t, String name, List<String> values) {
    ArrayWriter array = t.array(name);
    for (String value : values) {
      array.scalar().setString(value);
    }
  }

  /** Writes a repeated map of string columns, one entry per row of values. */
  public static void writeRows(TupleWriter t, String name, String[] columns, List<String[]> rows) {
    ArrayWriter array = t.array(name);
    for (String[] row : rows) {
      TupleWriter entry = array.tuple();
      for (int i = 0; i < columns.length; i++) {
        setString(entry, columns[i], row[i]);
      }
      array.save();
    }
  }

  /** Parses a small non-negative decimal number, or returns null. */
  public static Long number(String s) {
    if (s == null || s.isEmpty() || s.length() > 18) {
      return null;
    }
    for (int i = 0; i < s.length(); i++) {
      if (s.charAt(i) < '0' || s.charAt(i) > '9') {
        return null;
      }
    }
    return Long.parseLong(s);
  }
}
