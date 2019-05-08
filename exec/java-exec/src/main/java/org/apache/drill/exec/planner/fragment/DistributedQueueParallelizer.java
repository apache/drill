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
package org.apache.drill.exec.planner.fragment;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.util.function.CheckedConsumer;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.PhysicalOperatorSetupException;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.common.DrillNode;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.resourcemgr.NodeResources;
import org.apache.drill.exec.resourcemgr.config.QueryQueueConfig;
import org.apache.drill.exec.resourcemgr.config.exception.QueueSelectionException;
import org.apache.drill.exec.work.foreman.rm.QueryResourceManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * Paralellizer specialized for managing resources for a query based on Queues. This parallelizer
 * does not deal with increase/decrease of the parallelization of a query plan based on the current
 * cluster state. However, the memory assignment for each operator, minor fragment and major
 * fragment is based on the cluster state and provided queue configuration.
 */
public class DistributedQueueParallelizer extends SimpleParallelizer {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DistributedQueueParallelizer.class);
  private final boolean planHasMemory;
  private final QueryContext queryContext;
  private final QueryResourceManager rm;
  private final Map<DrillNode, Map<PhysicalOperator, Long>> operators;

  public DistributedQueueParallelizer(boolean memoryPlanning, QueryContext queryContext, QueryResourceManager queryRM) {
    super(queryContext);
    this.planHasMemory = memoryPlanning;
    this.queryContext = queryContext;
    this.rm = queryRM;
    this.operators = new HashMap<>();
  }

  // return the memory computed for a physical operator on a drillbitendpoint.
  // At this stage buffered operator memory could have been reduced depending upon
  // the selected queue limits.
  public BiFunction<DrillbitEndpoint, PhysicalOperator, Long> getMemory() {
    return (endpoint, operator) -> {
      long operatorsMemory = operator.getMaxAllocation();
      if (!planHasMemory) {
        if (operator.isBufferedOperator(queryContext)) {
          final DrillNode drillEndpointNode = DrillNode.create(endpoint);
          operatorsMemory = operators.get(drillEndpointNode).get(operator);
        } else {
          operatorsMemory = (long)operator.getCost().getMemoryCost();
        }
      }
      logger.debug(" Memory requirement for the operator {} in endpoint {} is {}", operator, endpoint, operatorsMemory);
      return operatorsMemory;
    };
  }

  /**
   * Function called by the SimpleParallelizer to adjust the memory post parallelization.
   * The overall logic is to traverse the fragment tree and call the MemoryCalculator on
   * each major fragment. Once the memory is computed, resource requirement are accumulated
   * per drillbit.
   *
   * The total resource requirements are used to select a queue. If the selected queue's
   * resource limit is more/less than the query's requirement than the memory will be re-adjusted.
   *
   * @param planningSet context of the fragments.
   * @param roots root fragments.
   * @param onlineEndpointUUIDs currently active endpoints.
   * @throws PhysicalOperatorSetupException
   */
  public void adjustMemory(PlanningSet planningSet, Set<Wrapper> roots,
                           Map<DrillbitEndpoint, String> onlineEndpointUUIDs) throws ExecutionSetupException {
    if (planHasMemory) {
      logger.debug(" Plan already has memory settings. Adjustment of the memory is skipped");
      return;
    }
    logger.info(" Memory adjustment phase triggered");

    final Map<DrillNode, String> onlineDrillNodeUUIDs = onlineEndpointUUIDs.entrySet().stream()
      .collect(Collectors.toMap(x -> DrillNode.create(x.getKey()), x -> x.getValue()));

    // total node resources for the query plan maintained per drillbit.
    final Map<DrillNode, NodeResources> totalNodeResources =
      onlineDrillNodeUUIDs.keySet().stream().collect(Collectors.toMap(x -> x,
                                                              x -> NodeResources.create()));

    // list of the physical operators and their memory requirements per drillbit.
    final Map<DrillNode, List<Pair<PhysicalOperator, Long>>> operators =
      onlineDrillNodeUUIDs.keySet().stream().collect(Collectors.toMap(x -> x,
                                                              x -> new ArrayList<>()));

    for (Wrapper wrapper : roots) {
      traverse(wrapper, CheckedConsumer.throwingConsumerWrapper((Wrapper fragment) -> {
        MemoryCalculator calculator = new MemoryCalculator(planningSet, queryContext, rm.minimumOperatorMemory());
        fragment.getNode().getRoot().accept(calculator, fragment);
        NodeResources.merge(totalNodeResources, fragment.getResourceMap());
        operators.entrySet()
                  .stream()
                  .forEach((entry) -> entry.getValue()
                                           .addAll(calculator.getBufferedOperators(entry.getKey())));
      }));
    }

    if (logger.isDebugEnabled()) {
      logger.debug(" Total node resource requirements for the plan is {}", getJSONFromResourcesMap(totalNodeResources));
    }

    final QueryQueueConfig queueConfig;
    try {
      queueConfig = this.rm.selectQueue(max(totalNodeResources.values()));
    } catch (QueueSelectionException exception) {
      throw new ExecutionSetupException(exception.getMessage());
    }

    Map<DrillNode,
        List<Pair<PhysicalOperator, Long>>> memoryAdjustedOperators =
                      ensureOperatorMemoryWithinLimits(operators, totalNodeResources,
                                          convertMBToBytes(Math.min(queueConfig.getMaxQueryMemoryInMBPerNode(),
                                                                    queueConfig.getQueueTotalMemoryInMB(onlineEndpointUUIDs.size()))));
    memoryAdjustedOperators.entrySet().stream().forEach((x) -> {
      Map<PhysicalOperator, Long> memoryPerOperator = x.getValue().stream()
                                                                  .collect(Collectors.toMap(operatorLongPair -> operatorLongPair.getLeft(),
                                                                                            operatorLongPair -> operatorLongPair.getRight(),
                                                                                            (mem_1, mem_2) -> (mem_1 + mem_2)));
      this.operators.put(x.getKey(), memoryPerOperator);
    });

    if (logger.isDebugEnabled()) {
      logger.debug(" Total node resource requirements after adjustment {}", getJSONFromResourcesMap(totalNodeResources));
    }

    this.rm.setCost(convertToUUID(totalNodeResources, onlineDrillNodeUUIDs));
  }

  private long convertMBToBytes(long value) {
    return value * 1024 * 1024;
  }

  private Map<String, NodeResources> convertToUUID(Map<DrillNode, NodeResources> nodeResourcesMap,
                                                   Map<DrillNode, String> onlineDrillNodeUUIDs) {
    Map<String, NodeResources> nodeResourcesPerUUID = new HashMap<>();
    for (Map.Entry<DrillNode, NodeResources> nodeResource : nodeResourcesMap.entrySet()) {
      nodeResourcesPerUUID.put(onlineDrillNodeUUIDs.get(nodeResource.getKey()), nodeResource.getValue());
    }
    return nodeResourcesPerUUID;
  }

  private NodeResources max(Collection<NodeResources> resources) {
    NodeResources maxResource = null;
    for (NodeResources resource : resources) {
      if (maxResource == null || maxResource.getMemoryInBytes() < resource.getMemoryInBytes()) {
        maxResource = resource;
      }
    }
    return maxResource;
  }


  /**
   * Helper method to adjust the memory for the buffered operators.
   * @param memoryPerOperator list of physical operators per drillbit
   * @param nodeResourceMap resources per drillbit.
   * @param nodeLimit permissible node limit.
   * @return list of operators which contain adjusted memory limits.
   */
  private Map<DrillNode, List<Pair<PhysicalOperator, Long>>>
          ensureOperatorMemoryWithinLimits(Map<DrillNode, List<Pair<PhysicalOperator, Long>>> memoryPerOperator,
                                           Map<DrillNode, NodeResources> nodeResourceMap, long nodeLimit) throws ExecutionSetupException {
    // Get the physical operators which are above the node memory limit.
    Map<DrillNode,
         List<Pair<PhysicalOperator, Long>>> onlyMemoryAboveLimitOperators = memoryPerOperator.entrySet()
                                                                                              .stream()
                                                                                              .filter(entry -> nodeResourceMap.get(entry.getKey()).getMemoryInBytes() > nodeLimit)
                                                                                              .collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue()));

    // Compute the total memory required by the physical operators on the drillbits which are above node limit.
    // Then use the total memory to adjust the memory requirement based on the permissible node limit.
    Map<DrillNode, List<Pair<PhysicalOperator, Long>>> memoryAdjustedDrillbits = new HashMap<>();
    onlyMemoryAboveLimitOperators.entrySet().stream().forEach(
      CheckedConsumer.throwingConsumerWrapper(entry -> {
        Long totalBufferedOperatorsMemoryReq = entry.getValue().stream().mapToLong(Pair::getValue).sum();
        Long nonBufferedOperatorsMemoryReq = nodeResourceMap.get(entry.getKey()).getMemoryInBytes() - totalBufferedOperatorsMemoryReq;
        Long bufferedOperatorsMemoryLimit = nodeLimit - nonBufferedOperatorsMemoryReq;
        if (bufferedOperatorsMemoryLimit < 0 || nonBufferedOperatorsMemoryReq < 0) {
          logger.error(" Operator memory requirements for buffered operators {} or non buffered operators {} is negative", bufferedOperatorsMemoryLimit,
            nonBufferedOperatorsMemoryReq);
          throw new ExecutionSetupException("Operator memory requirements for buffered operators " + bufferedOperatorsMemoryLimit + " or non buffered operators " +
            nonBufferedOperatorsMemoryReq + " is less than zero");
        }
        List<Pair<PhysicalOperator, Long>> adjustedMemory = entry.getValue().stream().map(operatorAndMemory -> {
          // formula to adjust the memory is (optimalMemory / totalMemory(this is for all the operators)) * permissible_node_limit.
          return Pair.of(operatorAndMemory.getKey(),
                         Math.max(this.rm.minimumOperatorMemory(),
                                  (long) Math.ceil(operatorAndMemory.getValue()/totalBufferedOperatorsMemoryReq * bufferedOperatorsMemoryLimit)));
        }).collect(Collectors.toList());
        memoryAdjustedDrillbits.put(entry.getKey(), adjustedMemory);
        NodeResources nodeResources = nodeResourceMap.get(entry.getKey());
        nodeResources.setMemoryInBytes(nonBufferedOperatorsMemoryReq + adjustedMemory.stream().mapToLong(Pair::getValue).sum());
      })
    );

    checkIfWithinLimit(nodeResourceMap, nodeLimit);

    // Get all the operations on drillbits which were adjusted for memory and merge them with operators which are not
    // adjusted for memory.
    Map<DrillNode,
        List<Pair<PhysicalOperator, Long>>> allDrillbits = memoryPerOperator.entrySet()
                                                                            .stream()
                                                                            .filter((entry) -> !memoryAdjustedDrillbits.containsKey(entry.getKey()))
                                                                            .collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue()));

    memoryAdjustedDrillbits.entrySet().stream().forEach(
      operatorMemory -> allDrillbits.put(operatorMemory.getKey(), operatorMemory.getValue()));

    // At this point allDrillbits contains the operators on all drillbits. The memory also is adjusted based on the nodeLimit and
    // the ratio of their requirements.
    return allDrillbits;
  }

  private void checkIfWithinLimit(Map<DrillNode, NodeResources> nodeResourcesMap, long nodeLimit) throws ExecutionSetupException {
    for (Map.Entry<DrillNode, NodeResources> entry : nodeResourcesMap.entrySet()) {
      if (entry.getValue().getMemoryInBytes() > nodeLimit) {
        logger.error(" Memory requirement for the query cannot be adjusted." +
          " Memory requirement {} (in bytes) for a node {} is greater than limit {}", entry.getValue()
          .getMemoryInBytes(), entry.getKey(), nodeLimit);
        throw new ExecutionSetupException("Minimum memory requirement "
          + entry.getValue().getMemoryInBytes() + " for a node " + entry.getKey() + " is greater than limit: " + nodeLimit);
      }
    }
  }

  private String getJSONFromResourcesMap(Map<DrillNode, NodeResources> resourcesMap) {
    String json = "";
    try {
      json = new ObjectMapper().writeValueAsString(resourcesMap.entrySet()
                                                               .stream()
                                                                .collect(Collectors.toMap(entry -> entry.getKey()
                                                                                                        .toString(), Map.Entry::getValue)));
    } catch (JsonProcessingException exception) {
      logger.error(" Cannot convert the Node resources map to json ");
    }

    return json;
  }
}