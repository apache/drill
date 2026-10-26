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
package org.apache.drill.exec.store.pcap.protocol;

/**
 * Common part of packet and session protocol decoders. See docs/dev/PcapProtocolDecoders.md.
 */
public interface ProtocolDecoder {

  /** Value of parsed_protocol and the name of this decoder's sub-map in parsed_data. Lower case, unique. */
  String protocol();

  /** When several decoders accept the same data, higher priority is tried first. */
  default int priority() {
    return 0;
  }
}
