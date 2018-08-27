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
package org.apache.drill.exec.work.filter;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.commons.collections.CollectionUtils;
import org.apache.drill.exec.ops.AccountingDataTunnel;
import org.apache.drill.exec.ops.Consumer;
import org.apache.drill.exec.ops.SendingAccountor;
import org.apache.drill.exec.ops.StatusHandler;
import org.apache.drill.exec.physical.base.AbstractPhysicalVisitor;
import org.apache.drill.exec.physical.base.Exchange;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.config.HashJoinPOP;
import org.apache.drill.exec.planner.fragment.Fragment;
import org.apache.drill.exec.planner.fragment.Wrapper;
import org.apache.drill.exec.proto.BitData;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.proto.GeneralRPCProtos;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.RpcOutcomeListener;
import org.apache.drill.exec.rpc.data.DataTunnel;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.work.QueryWorkUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class manages the RuntimeFilter routing information of the pushed down join predicate
 * of the partitioned exchange HashJoin.
 *
 * The working flow of the RuntimeFilter has two kinds: Broadcast case and Partitioned case.
 * The HashJoinRecordBatch is responsible to generate the RuntimeFilter.
 * To Partitioned case:
 * The generated RuntimeFilter will be sent to the Foreman node. The Foreman node receives the RuntimeFilter
 * async, aggregates them, broadcasts them the Scan nodes's MinorFragment. The RuntimeFilterRecordBatch which
 * steps over the Scan node will leverage the received RuntimeFilter to filter out the scanned rows to generate
 * the SV2.
 * To Broadcast case:
 * The generated RuntimeFilter will be sent to Scan node's RuntimeFilterRecordBatch directly. The working of the
 * RuntimeFilterRecordBath is the same as the Partitioned one.
 *
 *
 *
 */
public class RuntimeFilterManager {

  private Wrapper rootWrapper;
  //HashJoin node's major fragment id to its corresponding probe side nodes's endpoints
  private Map<Integer, List<CoordinationProtos.DrillbitEndpoint>> joinMjId2probdeScanEps = new HashMap<>();
  //HashJoin node's major fragment id to its corresponding probe side nodes's number
  private Map<Integer, Integer> joinMjId2scanSize = new ConcurrentHashMap<>();
  //HashJoin node's major fragment id to its corresponding probe side scan node's belonging major fragment id
  private Map<Integer, Integer> joinMjId2ScanMjId = new HashMap<>();
  //HashJoin node's major fragment id to its aggregated RuntimeFilterWritable
  private Map<Integer, RuntimeFilterWritable> joinMjId2AggregatedRF = new ConcurrentHashMap<>();

  private DrillbitContext drillbitContext;

  private SendingAccountor sendingAccountor = new SendingAccountor();

  private static final Logger logger = LoggerFactory.getLogger(RuntimeFilterManager.class);

  /**
   * This class maintains context for the runtime join push down's filter management. It
   * does a traversal of the physical operators by leveraging the root wrapper which indirectly
   * holds the global PhysicalOperator tree and contains the minor fragment endpoints.
   * @param workUnit
   * @param drillbitContext
   */
  public RuntimeFilterManager(QueryWorkUnit workUnit, DrillbitContext drillbitContext) {
    this.rootWrapper = workUnit.getRootWrapper();
    this.drillbitContext = drillbitContext;
  }

  /**
   * This method is to collect the parallel information of the RuntimetimeFilters. Then it generates a RuntimeFilter routing map to
   * record the relationship between the RuntimeFilter producers and consumers.
   */
  public void collectRuntimeFilterParallelAndControlInfo() {
    RuntimeFilterParallelismCollector runtimeFilterParallelismCollector = new RuntimeFilterParallelismCollector();
    rootWrapper.getNode().getRoot().accept(runtimeFilterParallelismCollector, null);
    List<RFHelperHolder> holders = runtimeFilterParallelismCollector.getHolders();

    for (RFHelperHolder holder : holders) {
      List<CoordinationProtos.DrillbitEndpoint> probeSideEndpoints = holder.getProbeSideScanEndpoints();
      int probeSideScanMajorId = holder.getProbeSideScanMajorId();
      int joinNodeMajorId = holder.getJoinMajorId();
      RuntimeFilterDef runtimeFilterDef = holder.getRuntimeFilterDef();
      boolean sendToForeman = runtimeFilterDef.isSendToForeman();
      if (sendToForeman) {
        //send RuntimeFilter to Foreman
        joinMjId2probdeScanEps.put(joinNodeMajorId, probeSideEndpoints);
        joinMjId2scanSize.put(joinNodeMajorId, probeSideEndpoints.size());
        joinMjId2ScanMjId.put(joinNodeMajorId, probeSideScanMajorId);
      }
    }
  }


  public void waitForComplete() {
    sendingAccountor.waitForSendComplete();
  }

  /**
   * This method is passively invoked by receiving a runtime filter from the network
   * @param runtimeFilterWritable
   */
  public void registerRuntimeFilter(RuntimeFilterWritable runtimeFilterWritable) {
    BitData.RuntimeFilterBDef runtimeFilterB = runtimeFilterWritable.getRuntimeFilterBDef();
    int majorId = runtimeFilterB.getMajorFragmentId();
    UserBitShared.QueryId queryId = runtimeFilterB.getQueryId();
    List<String> probeFields = runtimeFilterB.getProbeFieldsList();
    logger.info("RuntimeFilterManager receives a runtime filter , majorId:{}, queryId:{}", majorId, QueryIdHelper.getQueryId(queryId));
    int size;
    synchronized (this) {
      size = joinMjId2scanSize.get(majorId);
      Preconditions.checkState(size > 0);
      RuntimeFilterWritable aggregatedRuntimeFilter = joinMjId2AggregatedRF.get(majorId);
      if (aggregatedRuntimeFilter == null) {
        aggregatedRuntimeFilter = runtimeFilterWritable;
      } else {
        aggregatedRuntimeFilter.aggregate(runtimeFilterWritable);
      }
      joinMjId2AggregatedRF.put(majorId, aggregatedRuntimeFilter);
      size--;
      joinMjId2scanSize.put(majorId, size);
    }
    if (size == 0) {
      broadcastAggregatedRuntimeFilter(majorId, queryId, probeFields);
    }
  }


  private void broadcastAggregatedRuntimeFilter(int joinMajorId, UserBitShared.QueryId queryId, List<String> probeFields) {
    List<CoordinationProtos.DrillbitEndpoint> scanNodeEps = joinMjId2probdeScanEps.get(joinMajorId);
    int scanNodeMjId = joinMjId2ScanMjId.get(joinMajorId);
    for (int minorId = 0; minorId < scanNodeEps.size(); minorId++) {
      BitData.RuntimeFilterBDef.Builder builder = BitData.RuntimeFilterBDef.newBuilder();
      for (String probeField : probeFields) {
        builder.addProbeFields(probeField);
      }
      BitData.RuntimeFilterBDef runtimeFilterBDef = builder
        .setQueryId(queryId)
        .setMajorFragmentId(scanNodeMjId)
        .setMinorFragmentId(minorId)
        .build();
      RuntimeFilterWritable aggregatedRuntimeFilter = joinMjId2AggregatedRF.get(joinMajorId);
      RuntimeFilterWritable runtimeFilterWritable = new RuntimeFilterWritable(runtimeFilterBDef, aggregatedRuntimeFilter.getData());
      CoordinationProtos.DrillbitEndpoint drillbitEndpoint = scanNodeEps.get(minorId);

      DataTunnel dataTunnel = drillbitContext.getDataConnectionsPool().getTunnel(drillbitEndpoint);
      Consumer<RpcException> exceptionConsumer = new Consumer<RpcException>() {
        @Override
        public void accept(final RpcException e) {
          logger.warn("fail to broadcast a runtime filter to the probe side scan node", e);
        }

        @Override
        public void interrupt(final InterruptedException e) {
          logger.warn("fail to broadcast a runtime filter to the probe side scan node", e);
        }
      };
      RpcOutcomeListener<GeneralRPCProtos.Ack> statusHandler = new StatusHandler(exceptionConsumer, sendingAccountor);
      AccountingDataTunnel accountingDataTunnel = new AccountingDataTunnel(dataTunnel, sendingAccountor, statusHandler);
      accountingDataTunnel.sendRuntimeFilter(runtimeFilterWritable);
    }
  }

  /**
   * Collect the runtime filter parallelism related information such as join node major/minor fragment id , probe side scan node's
   * major/minor fragment id, probe side node's endpoints.
   */
  protected class RuntimeFilterParallelismCollector extends AbstractPhysicalVisitor<Void, RFHelperHolder, RuntimeException> {

    private List<RFHelperHolder> holders = new ArrayList<>();

    @Override
    public Void visitOp(PhysicalOperator op, RFHelperHolder holder) throws RuntimeException {
      boolean isHashJoinOp = op instanceof HashJoinPOP;
      if (isHashJoinOp) {
        HashJoinPOP hashJoinPOP = (HashJoinPOP) op;
        RuntimeFilterDef runtimeFilterDef = hashJoinPOP.getRuntimeFilterDef();
        if (runtimeFilterDef != null) {
          if (holder == null) {
            holder = new RFHelperHolder();
            holders.add(holder);
          }
          holder.setRuntimeFilterDef(runtimeFilterDef);
          GroupScan probeSideScanOp = runtimeFilterDef.getProbeSideGroupScan();
          Wrapper container = findPhysicalOpContainer(rootWrapper, hashJoinPOP);
          int majorFragmentId = container.getMajorFragmentId();
          holder.setJoinMajorId(majorFragmentId);
          Wrapper probeSideScanContainer = findPhysicalOpContainer(rootWrapper, probeSideScanOp);
          int probeSideScanMjId = probeSideScanContainer.getMajorFragmentId();
          List<CoordinationProtos.DrillbitEndpoint> probeSideScanEps = probeSideScanContainer.getAssignedEndpoints();
          holder.setProbeSideScanEndpoints(probeSideScanEps);
          holder.setProbeSideScanMajorId(probeSideScanMjId);
        }
      }
      return visitChildren(op, holder);
    }

    public List<RFHelperHolder> getHolders() {
      return holders;
    }
  }

  private class WrapperOperatorsVisitor extends AbstractPhysicalVisitor<Void, Void, RuntimeException> {

    private PhysicalOperator targetOp;

    private Fragment fragment;

    private boolean contain = false;

    private boolean targetIsGroupScan;

    private boolean targetIsHashJoin;

    private String targetGroupScanDigest;

    private String targetHashJoinJson;


    public WrapperOperatorsVisitor(PhysicalOperator targetOp, Fragment fragment) {
      this.targetOp = targetOp;
      this.fragment = fragment;
      this.targetIsGroupScan = targetOp instanceof GroupScan;
      this.targetIsHashJoin = targetOp instanceof HashJoinPOP;
      this.targetGroupScanDigest = targetIsGroupScan ? ((GroupScan) targetOp).getDigest() : null;
      this.targetHashJoinJson = targetIsHashJoin ? jsonOfPhysicalOp(targetOp) : null;
    }

    @Override
    public Void visitExchange(Exchange exchange, Void value) throws RuntimeException {
      List<Fragment.ExchangeFragmentPair> exchangeFragmentPairs = fragment.getReceivingExchangePairs();
      for (Fragment.ExchangeFragmentPair exchangeFragmentPair : exchangeFragmentPairs) {
        boolean same = exchange == exchangeFragmentPair.getExchange();
        if (same) {
          return null;
        }
      }
      return exchange.getChild().accept(this, value);
    }

    @Override
    public Void visitOp(PhysicalOperator op, Void value) throws RuntimeException {
      boolean same = false;
      if (targetIsGroupScan && op instanceof GroupScan) {
        //Since GroupScan may be rewrite during the planing, here we use the digest to identify it.
        String currentDigest = ((GroupScan) op).getDigest();
        same = targetGroupScanDigest.equals(currentDigest);
      }
      if (targetIsHashJoin && op instanceof HashJoinPOP) {
        String currentOpJson = jsonOfPhysicalOp(op);
        same = targetHashJoinJson.equals(currentOpJson);
      }
      if (!same) {
        for (PhysicalOperator child : op) {
          child.accept(this, value);
        }
      } else {
        contain = true;
      }
      return null;
    }

    public boolean isContain() {
      return contain;
    }

    public String jsonOfPhysicalOp(PhysicalOperator operator) {
      try {
        ObjectMapper objectMapper = new ObjectMapper();
        StringWriter stringWriter = new StringWriter();
        objectMapper.writeValue(stringWriter, operator);
        return stringWriter.toString();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private boolean containsPhysicalOperator(Wrapper wrapper, PhysicalOperator op) {
    WrapperOperatorsVisitor wrapperOpsVistitor = new WrapperOperatorsVisitor(op, wrapper.getNode());
    wrapper.getNode().getRoot().accept(wrapperOpsVistitor, null);
    return wrapperOpsVistitor.isContain();
  }

  private Wrapper findPhysicalOpContainer(Wrapper wrapper, PhysicalOperator op) {
    boolean contain = containsPhysicalOperator(wrapper, op);
    if (contain) {
      return wrapper;
    }
    List<Wrapper> dependencies = wrapper.getFragmentDependencies();
    if (CollectionUtils.isEmpty(dependencies)) {
      return null;
    }
    for (Wrapper dependencyWrapper : dependencies) {
      Wrapper opContainer = findPhysicalOpContainer(dependencyWrapper, op);
      if (opContainer != null) {
        return opContainer;
      }
    }
    //should not be here
    throw new IllegalStateException(String.format("No valid Wrapper found for physicalOperator with id=%d", op.getOperatorId()));
  }

  /**
   * RuntimeFilter helper util holder
   */
  private static class RFHelperHolder {

    private int joinMajorId;

    private int probeSideScanMajorId;



    private List<CoordinationProtos.DrillbitEndpoint> probeSideScanEndpoints;

    private RuntimeFilterDef runtimeFilterDef;


    public List<CoordinationProtos.DrillbitEndpoint> getProbeSideScanEndpoints() {
      return probeSideScanEndpoints;
    }

    public void setProbeSideScanEndpoints(List<CoordinationProtos.DrillbitEndpoint> probeSideScanEndpoints) {
      this.probeSideScanEndpoints = probeSideScanEndpoints;
    }

    public int getJoinMajorId() {
      return joinMajorId;
    }

    public void setJoinMajorId(int joinMajorId) {
      this.joinMajorId = joinMajorId;
    }

    public int getProbeSideScanMajorId() {
      return probeSideScanMajorId;
    }

    public void setProbeSideScanMajorId(int probeSideScanMajorId) {
      this.probeSideScanMajorId = probeSideScanMajorId;
    }


    public RuntimeFilterDef getRuntimeFilterDef() {
      return runtimeFilterDef;
    }

    public void setRuntimeFilterDef(RuntimeFilterDef runtimeFilterDef) {
      this.runtimeFilterDef = runtimeFilterDef;
    }
  }
}
