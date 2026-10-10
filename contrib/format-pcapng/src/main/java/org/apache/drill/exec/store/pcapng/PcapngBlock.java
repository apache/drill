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
package org.apache.drill.exec.store.pcapng;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * One pcapng block being written as a row, with everything the reader
 * decoded from it. Fields that do not apply to the block are null.
 */
class PcapngBlock {

  /** Options of a descriptive block by stat column name (stat queries only). */
  Map<String, Object> stats;
  /** All opt_comment options of the block, newline separated. */
  String comment;

  // Enhanced Packet Block fields, resolved against the block's interface
  int interfaceId;
  String interfaceName;
  int linkType;
  Instant timestamp;
  int capturedLength;
  int originalLength;
  byte[] data;
  /** epb_flags, or null if absent. */
  Integer flags;
  Long dropCount;
  String hash;
  /** Decoded packet, or null if the link type or protocol is not supported. */
  PacketDecoder packet;
  /** Problems found while reading this block, or null. */
  List<String> errors;
  /** True for a row that only reports an error. */
  boolean errorRow;

  void addError(String error) {
    if (errors == null) {
      errors = new ArrayList<>();
    }
    errors.add(error);
  }

  static PcapngBlock errorRow(String error) {
    PcapngBlock row = new PcapngBlock();
    row.errorRow = true;
    row.addError(error);
    return row;
  }
}
