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
package org.apache.drill.exec.planner;

import java.io.IOException;
import java.util.Set;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.config.LogicalPlanPersistence;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.scanner.persistence.ScanResult;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.coord.DrillbitEndpointSerDe;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.FragmentLeaf;
import org.apache.drill.exec.physical.base.FragmentRoot;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalOperatorUtil;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.record.MajorTypeSerDe;
import org.apache.drill.exec.server.options.OptionList;
import org.apache.drill.exec.store.StoragePluginRegistry;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.module.SimpleModule;

public class PhysicalPlanReader {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PhysicalPlanReader.class);

  private final ObjectReader physicalPlanReader;
  private final ObjectMapper mapper;
  private final ObjectReader operatorReader;
  private final ObjectReader logicalPlanReader;

  public PhysicalPlanReader(DrillConfig config, ScanResult scanResult, LogicalPlanPersistence lpPersistance, final DrillbitEndpoint endpoint,
                            final StoragePluginRegistry pluginRegistry) {

    // Endpoint serializer/deserializer.
    SimpleModule deserModule = new SimpleModule("PhysicalOperatorModule") //
        .addSerializer(DrillbitEndpoint.class, new DrillbitEndpointSerDe.Se()) //
        .addDeserializer(DrillbitEndpoint.class, new DrillbitEndpointSerDe.De()) //
        .addSerializer(MajorType.class, new MajorTypeSerDe.Se())
        .addDeserializer(MajorType.class, new MajorTypeSerDe.De());

    ObjectMapper lpMapper = lpPersistance.getMapper();
    lpMapper.registerModule(deserModule);
    Set<Class<? extends PhysicalOperator>> subTypes = PhysicalOperatorUtil.getSubTypes(scanResult);
    for (Class<? extends PhysicalOperator> subType : subTypes) {
      lpMapper.registerSubtypes(subType);
    }
    InjectableValues injectables = new InjectableValues.Std() //
            .addValue(StoragePluginRegistry.class, pluginRegistry) //
        .addValue(DrillbitEndpoint.class, endpoint); //

    this.mapper = lpMapper;
    this.physicalPlanReader = mapper.reader(PhysicalPlan.class).with(injectables);
    this.operatorReader = mapper.reader(PhysicalOperator.class).with(injectables);
    this.logicalPlanReader = mapper.reader(LogicalPlan.class).with(injectables);
  }

  public String writeJson(OptionList list) throws JsonProcessingException{
    return mapper.writeValueAsString(list);
  }

  public String writeJson(PhysicalOperator op) throws JsonProcessingException{
    return mapper.writeValueAsString(op);
  }

  public PhysicalPlan readPhysicalPlan(String json) throws JsonProcessingException, IOException {
    logger.debug("Reading physical plan {}", json);
    return physicalPlanReader.readValue(json);
  }

  public FragmentRoot readFragmentOperator(String json) throws JsonProcessingException, IOException {
    logger.debug("Attempting to read {}", json);
    PhysicalOperator op = operatorReader.readValue(json);
    if(op instanceof FragmentLeaf){
      return (FragmentRoot) op;
    }else{
      throw new UnsupportedOperationException(String.format("The provided json fragment doesn't have a FragmentRoot as its root operator.  The operator was %s.", op.getClass().getCanonicalName()));
    }
  }

  public LogicalPlan readLogicalPlan(String json) throws JsonProcessingException, IOException{
    logger.debug("Reading logical plan {}", json);
    return logicalPlanReader.readValue(json);
  }
}
