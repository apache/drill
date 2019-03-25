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
package org.apache.drill.exec.store.pcap.schema;

import org.apache.drill.exec.store.pcap.dto.ColumnDto;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Schema {

  private final List<ColumnDto> columns = new ArrayList<>();

  public Schema() {
    setupStructure();
  }

  private void setupStructure() {
    columns.add(new ColumnDto("type", PcapTypes.STRING));
    columns.add(new ColumnDto("network", PcapTypes.INTEGER));
    columns.add(new ColumnDto("timestamp", PcapTypes.TIMESTAMP));
    columns.add(new ColumnDto("timestamp_micro", PcapTypes.LONG));
    columns.add(new ColumnDto("src_ip", PcapTypes.STRING));
    columns.add(new ColumnDto("dst_ip", PcapTypes.STRING));
    columns.add(new ColumnDto("src_port", PcapTypes.INTEGER));
    columns.add(new ColumnDto("dst_port", PcapTypes.INTEGER));
    columns.add(new ColumnDto("src_mac_address", PcapTypes.STRING));
    columns.add(new ColumnDto("dst_mac_address", PcapTypes.STRING));
    columns.add(new ColumnDto("tcp_session", PcapTypes.LONG));
    columns.add(new ColumnDto("tcp_ack", PcapTypes.BOOLEAN));
    columns.add(new ColumnDto("tcp_flags", PcapTypes.INTEGER));
    columns.add(new ColumnDto("tcp_flags_ns", PcapTypes.BOOLEAN));
    columns.add(new ColumnDto("tcp_flags_cwr", PcapTypes.BOOLEAN));
    columns.add(new ColumnDto("tcp_flags_ece ", PcapTypes.BOOLEAN ));
    columns.add(new ColumnDto("tcp_flags_ece_ecn_capable", PcapTypes.BOOLEAN ));
    columns.add(new ColumnDto("tcp_flags_ece_congestion_experienced", PcapTypes.BOOLEAN ));
    columns.add(new ColumnDto("tcp_flags_urg", PcapTypes.BOOLEAN ));
    columns.add(new ColumnDto("tcp_flags_ack", PcapTypes.BOOLEAN ));
    columns.add(new ColumnDto("tcp_flags_psh", PcapTypes.BOOLEAN ));
    columns.add(new ColumnDto("tcp_flags_rst", PcapTypes.BOOLEAN ));
    columns.add(new ColumnDto("tcp_flags_syn", PcapTypes.BOOLEAN ));
    columns.add(new ColumnDto("tcp_flags_fin", PcapTypes.BOOLEAN ));
    columns.add(new ColumnDto("tcp_parsed_flags", PcapTypes.STRING));
    columns.add(new ColumnDto("packet_length", PcapTypes.INTEGER));
    columns.add(new ColumnDto("is_corrupt", PcapTypes.BOOLEAN));
    columns.add(new ColumnDto("data", PcapTypes.STRING));
  }

  /**
   * Return list with all columns names and its types
   *
   * @return List<ColumnDto>
   */
  public List<ColumnDto> getColumns() {
    return Collections.unmodifiableList(columns);
  }

  public ColumnDto getColumnByIndex(int i) {
    return columns.get(i);
  }

  public int getNumberOfColumns() {
    return columns.size();
  }
}
