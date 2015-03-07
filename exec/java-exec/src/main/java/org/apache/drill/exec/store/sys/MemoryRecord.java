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
package org.apache.drill.exec.store.sys;

import com.google.common.collect.Lists;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.TopLevelAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.vector.BigIntVector;
import org.apache.drill.exec.vector.VarCharVector;
import org.eigenbase.sql.type.SqlTypeName;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.List;

/**
 * A {@link org.apache.drill.exec.store.sys.SystemRecord} that holds information about drillbit memory
 */
public class MemoryRecord extends SystemRecord {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MemoryRecord.class);

  private static final MemoryRecord INSTANCE = new MemoryRecord();

  public static SystemRecord getInstance() {
    return INSTANCE;
  }

  private static final String HOST_NAME = "hostname";
  private static final MaterializedField hostNameField = MaterializedField.create(HOST_NAME,
    Types.required(TypeProtos.MinorType.VARCHAR));
  private VarCharVector hostName;

  private static final String USER_PORT = "user_port";
  private static final MaterializedField userPortField = MaterializedField.create(USER_PORT,
    Types.required(TypeProtos.MinorType.BIGINT));
  private BigIntVector userPort;

  private static final String CURRENT_HEAP_SIZE = "heap_current";
  private static final MaterializedField currentHeapSizeField = MaterializedField.create(CURRENT_HEAP_SIZE,
    Types.required(TypeProtos.MinorType.BIGINT));
  private BigIntVector currentHeapSize;

  private static final String MAX_HEAP_SIZE = "heap_max";
  private static final MaterializedField maxHeapSizeField = MaterializedField.create(MAX_HEAP_SIZE,
    Types.required(TypeProtos.MinorType.BIGINT));
  private BigIntVector maxHeapSize;

  private static final String CURRENT_DIRECT_MEMORY = "direct_current";
  private static final MaterializedField currentDirectMemoryField = MaterializedField.create(CURRENT_DIRECT_MEMORY,
    Types.required(TypeProtos.MinorType.BIGINT));
  private BigIntVector currentDirectMemory;

  private static final String MAX_DIRECT_MEMORY = "direct_max";
  private static final MaterializedField maxDirectMemoryField = MaterializedField.create(MAX_DIRECT_MEMORY,
    Types.required(TypeProtos.MinorType.BIGINT));
  private BigIntVector maxDirectMemory;

  private static final List<SqlTypeName> FIELDS = Lists.newArrayList(SqlTypeName.VARCHAR, SqlTypeName.BIGINT,
    SqlTypeName.BIGINT, SqlTypeName.BIGINT, SqlTypeName.BIGINT, SqlTypeName.BIGINT);

  private static final List<String> NAMES = Lists.newArrayList(HOST_NAME, USER_PORT, CURRENT_HEAP_SIZE, MAX_HEAP_SIZE,
    CURRENT_DIRECT_MEMORY, MAX_DIRECT_MEMORY);

  private MemoryRecord() {
  }

  @Override
  public void setup(final OutputMutator output) throws SchemaChangeException {
    hostName = output.addField(hostNameField, VarCharVector.class);
    userPort = output.addField(userPortField, BigIntVector.class);
    currentHeapSize = output.addField(currentHeapSizeField, BigIntVector.class);
    maxHeapSize = output.addField(maxHeapSizeField, BigIntVector.class);
    currentDirectMemory = output.addField(currentDirectMemoryField, BigIntVector.class);
    maxDirectMemory = output.addField(maxDirectMemoryField, BigIntVector.class);
  }

  @Override
  public void setRecordValues(final FragmentContext context) {
    final DrillbitContext drillbitContext = context.getDrillbitContext();
    final MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();

    final CoordinationProtos.DrillbitEndpoint endpoint = drillbitContext.getEndpoint();
    final String address = endpoint.getAddress();
    final VarCharVector.Mutator hostNameMutator = hostName.getMutator();
    hostNameMutator.setSafe(0, address.getBytes());
    hostNameMutator.setValueCount(1);

    final int port = endpoint.getUserPort();
    final BigIntVector.Mutator userPortMutator = userPort.getMutator();
    userPortMutator.setSafe(0, port);
    userPortMutator.setValueCount(1);

    final BigIntVector.Mutator currentHeapSizeMutator = currentHeapSize.getMutator();
    currentHeapSizeMutator.setSafe(0, memoryMXBean.getHeapMemoryUsage().getUsed());
    currentHeapSizeMutator.setValueCount(1);

    final BigIntVector.Mutator maxHeapSizeMutator = maxHeapSize.getMutator();
    maxHeapSizeMutator.setSafe(0, memoryMXBean.getHeapMemoryUsage().getMax());
    maxHeapSizeMutator.setValueCount(1);

    final BigIntVector.Mutator currentDirectMemoryMutator = currentDirectMemory.getMutator();
    currentDirectMemoryMutator.setSafe(0, drillbitContext.getAllocator().getAllocatedMemory());
    currentDirectMemoryMutator.setValueCount(1);

    final BigIntVector.Mutator maxDirectMemoryMutator = maxDirectMemory.getMutator();
    maxDirectMemoryMutator.setSafe(0, TopLevelAllocator.MAXIMUM_DIRECT_MEMORY);
    maxDirectMemoryMutator.setValueCount(1);
  }

  @Override
  public List<SqlTypeName> getFieldSqlTypeNames() {
    return FIELDS;
  }

  @Override
  public List<String> getFieldNames() {
    return NAMES;
  }
}
