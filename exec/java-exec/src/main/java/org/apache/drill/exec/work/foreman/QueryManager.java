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
package org.apache.drill.exec.work.foreman;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Preconditions;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.proto.BitControl.FragmentStatus;
import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.QueryResult.QueryState;
import org.apache.drill.exec.proto.UserProtos.RunQuery;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
import org.apache.drill.exec.rpc.RemoteRpcException;
import org.apache.drill.exec.store.sys.PStoreProvider;
import org.apache.drill.exec.work.foreman.Foreman.StateListener;
import org.apache.drill.exec.work.fragment.AbstractStatusReporter;

import com.google.common.collect.Sets;


/**
 * Each Foreman holds its own QueryManager.  This manages the events associated with execution of a particular query across all fragments.
 */
public class QueryManager implements FragmentStatusListener, DrillbitStatusListener{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(QueryManager.class);
  private final Set<DrillbitEndpoint> includedBits;

  private final QueryStatus status;
  private final StateListener stateListener;
  private final AtomicInteger remainingFragmentCount;
  private final QueryId queryId;

  public QueryManager(QueryId id, RunQuery query, PStoreProvider pStoreProvider, StateListener stateListener, Foreman foreman) {
    this.stateListener = stateListener;
    this.queryId =  id;
    this.remainingFragmentCount = new AtomicInteger(0);
    this.status = new QueryStatus(query, id, pStoreProvider, foreman);
    this.includedBits = Sets.newHashSet();
  }

  public QueryStatus getStatus(){
    return status;
  }

  @Override
  public void drillbitRegistered(Set<DrillbitEndpoint> registeredDrillbits) {
  }

  @Override
  public void drillbitUnregistered(Set<DrillbitEndpoint> unregisteredDrillbits) {
    for(DrillbitEndpoint ep : unregisteredDrillbits){
      if(this.includedBits.contains(ep)){
        logger.warn("Drillbit {} no longer registered in cluster.  Canceling query {}", ep.getAddress() + ep.getControlPort(), QueryIdHelper.getQueryId(queryId));
        this.stateListener.moveToState(QueryState.FAILED, new ForemanException("One more more nodes lost connectivity during query.  Identified node was " + ep.getAddress()));
      }
    }
  }


  @Override
  public void statusUpdate(FragmentStatus status) {

    logger.debug("New fragment status was provided to Foreman of {}", status);
    switch(status.getProfile().getState()){
    case AWAITING_ALLOCATION:
      updateFragmentStatus(status);
      break;
    case CANCELLED:
      //TODO: define a new query state to distinguish the state of early termination from cancellation
      fragmentDone(status);
      break;
    case FAILED:
      stateListener.moveToState(QueryState.FAILED, new RemoteRpcException(status.getProfile().getError()));
      break;
    case FINISHED:
      fragmentDone(status);
      break;
    case RUNNING:
      updateFragmentStatus(status);
      break;
    default:
      throw new UnsupportedOperationException(String.format("Received status of %s", status));
    }
  }

  private void updateFragmentStatus(FragmentStatus status){
    this.status.updateFragmentStatus(status);
  }

  private void fragmentDone(FragmentStatus status){
    this.status.incrementFinishedFragments();
    int remaining = remainingFragmentCount.decrementAndGet();
    updateFragmentStatus(status);
    logger.debug("waiting for {} fragments", remaining);
    if(remaining == 0){
      stateListener.moveToState(QueryState.COMPLETED, null);
    }
  }

  public void setup(FragmentHandle rootFragmentHandle, DrillbitEndpoint localIdentity, int countOfNonRootFragments){
    remainingFragmentCount.set(countOfNonRootFragments + 1);
    logger.debug("foreman is waiting for {} fragments to finish", countOfNonRootFragments + 1);
    status.add(new FragmentData(rootFragmentHandle, localIdentity, true));
    this.status.setTotalFragments(countOfNonRootFragments + 1);

    List<FragmentData> fragments = status.getFragmentData();
    for (FragmentData fragment : fragments) {
      this.includedBits.add(fragment.getEndpoint());
    }
  }

  public void addFragmentStatusTracker(PlanFragment fragment, boolean isRoot){
    addFragmentStatusTracker(fragment.getHandle(), fragment.getAssignment(), isRoot);
  }

  public void addFragmentStatusTracker(FragmentHandle handle, DrillbitEndpoint node, boolean isRoot){
    status.add(new FragmentData(handle, node, isRoot));
  }

  public RootStatusReporter getRootStatusHandler(FragmentContext context, PlanFragment fragment){
    return new RootStatusReporter(context, fragment);
  }

  class RootStatusReporter extends AbstractStatusReporter{

    private RootStatusReporter(FragmentContext context, PlanFragment fragment){
      super(context);
    }

    @Override
    protected void statusChange(FragmentHandle handle, FragmentStatus status) {
      statusUpdate(status);
    }


  }

}
