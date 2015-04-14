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

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.drill.common.exceptions.UserRemoteException;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.proto.BitControl.FragmentStatus;
import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.proto.SchemaUserBitShared;
import org.apache.drill.exec.proto.UserBitShared.MajorFragmentProfile;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.QueryInfo;
import org.apache.drill.exec.proto.UserBitShared.QueryProfile;
import org.apache.drill.exec.proto.UserBitShared.QueryResult.QueryState;
import org.apache.drill.exec.proto.UserProtos.RunQuery;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.control.Controller;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.sys.PStore;
import org.apache.drill.exec.store.sys.PStoreConfig;
import org.apache.drill.exec.store.sys.PStoreProvider;
import org.apache.drill.exec.work.EndpointListener;
import org.apache.drill.exec.work.foreman.Foreman.StateListener;
import org.apache.drill.exec.work.fragment.AbstractStatusReporter;
import org.apache.drill.exec.work.fragment.FragmentExecutor;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Each Foreman holds its own QueryManager.  This manages the events associated with execution of a particular query across all fragments.
 */
public class QueryManager implements FragmentStatusListener, DrillbitStatusListener {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(QueryManager.class);

  public static final PStoreConfig<QueryProfile> QUERY_PROFILE = PStoreConfig.
          newProtoBuilder(SchemaUserBitShared.QueryProfile.WRITE, SchemaUserBitShared.QueryProfile.MERGE)
      .name("profiles")
      .blob()
      .max(100)
      .build();

  public static final PStoreConfig<QueryInfo> RUNNING_QUERY_INFO = PStoreConfig.
          newProtoBuilder(SchemaUserBitShared.QueryInfo.WRITE, SchemaUserBitShared.QueryInfo.MERGE)
      .name("running")
      .ephemeral()
      .build();

  private final Set<DrillbitEndpoint> includedBits;
  private final StateListener stateListener;
  private final QueryId queryId;
  private final String stringQueryId;
  private final RunQuery runQuery;
  private final Foreman foreman;

  /*
   * Doesn't need to be thread safe as fragmentDataMap is generated in a single thread and then
   * accessed by multiple threads for reads only.
   */
  private final IntObjectOpenHashMap<IntObjectOpenHashMap<FragmentData>> fragmentDataMap =
      new IntObjectOpenHashMap<>();
  private final List<FragmentData> fragmentDataSet = Lists.newArrayList();

  private final PStore<QueryProfile> profilePStore;
  private final PStore<QueryInfo> profileEStore;

  // the following mutable variables are used to capture ongoing query status
  private String planText;
  private long startTime;
  private long endTime;
  private final AtomicInteger finishedFragments = new AtomicInteger(0);

  public QueryManager(final QueryId queryId, final RunQuery runQuery, final PStoreProvider pStoreProvider,
      final StateListener stateListener, final Foreman foreman) {
    this.queryId =  queryId;
    this.runQuery = runQuery;
    this.stateListener = stateListener;
    this.foreman = foreman;

    stringQueryId = QueryIdHelper.getQueryId(queryId);
    try {
      profilePStore = pStoreProvider.getStore(QUERY_PROFILE);
      profileEStore = pStoreProvider.getStore(RUNNING_QUERY_INFO);
    } catch (IOException e) {
      throw new DrillRuntimeException(e);
    }

    includedBits = Sets.newHashSet();
  }

  @Override
  public void drillbitRegistered(final Set<DrillbitEndpoint> registeredDrillbits) {
  }

  @Override
  public void drillbitUnregistered(final Set<DrillbitEndpoint> unregisteredDrillbits) {
    for(DrillbitEndpoint ep : unregisteredDrillbits) {
      if (includedBits.contains(ep)) {
        logger.warn("Drillbit {} no longer registered in cluster.  Canceling query {}",
            ep.getAddress() + ep.getControlPort(), QueryIdHelper.getQueryId(queryId));
        stateListener.moveToState(QueryState.FAILED,
            new ForemanException("One more more nodes lost connectivity during query.  Identified node was "
                + ep.getAddress()));
      }
    }
  }

  @Override
  public void statusUpdate(final FragmentStatus status) {
    logger.debug("New fragment status was provided to QueryManager of {}", status);
    switch(status.getProfile().getState()) {
    case AWAITING_ALLOCATION:
    case RUNNING:
      updateFragmentStatus(status);
      break;

    case FINISHED:
      fragmentDone(status);
      break;

    case CANCELLED:
      /*
       * TODO
       * This doesn't seem right; shouldn't this be similar to FAILED?
       * and this means once all are cancelled we'll get to COMPLETED, even though some weren't?
       *
       * So, we add it to the finishedFragments if we ourselves we receive a statusUpdate (from where),
       * but not if our cancellation listener gets it?
       */
      // TODO(DRILL-2370) we might not get these, so we need to take extra care for cleanup
      fragmentDone(status);
      break;

    case FAILED:
      stateListener.moveToState(QueryState.FAILED, new UserRemoteException(status.getProfile().getError()));
      break;

    default:
      throw new UnsupportedOperationException(String.format("Received status of %s", status));
    }
  }

  private void updateFragmentStatus(final FragmentStatus fragmentStatus) {
    final FragmentHandle fragmentHandle = fragmentStatus.getHandle();
    final int majorFragmentId = fragmentHandle.getMajorFragmentId();
    final int minorFragmentId = fragmentHandle.getMinorFragmentId();
    fragmentDataMap.get(majorFragmentId).get(minorFragmentId).setStatus(fragmentStatus);
  }

  private void fragmentDone(final FragmentStatus status) {
    updateFragmentStatus(status);

    final int finishedFragments = this.finishedFragments.incrementAndGet();
    final int totalFragments = fragmentDataSet.size();
    assert finishedFragments <= totalFragments : "The finished fragment count exceeds the total fragment count";
    final int remaining = totalFragments - finishedFragments;
    logger.debug("waiting for {} fragments", remaining);
    if (remaining == 0) {
      // this target state may be adjusted in moveToState() based on current FAILURE/CANCELLATION_REQUESTED status
      stateListener.moveToState(QueryState.COMPLETED, null);
    }
  }

  private void addFragment(final FragmentData fragmentData) {
    final FragmentHandle fragmentHandle = fragmentData.getHandle();
    final int majorFragmentId = fragmentHandle.getMajorFragmentId();
    final int minorFragmentId = fragmentHandle.getMinorFragmentId();

    IntObjectOpenHashMap<FragmentData> minorMap = fragmentDataMap.get(majorFragmentId);
    if (minorMap == null) {
      minorMap = new IntObjectOpenHashMap<>();
      fragmentDataMap.put(majorFragmentId, minorMap);
    }
    minorMap.put(minorFragmentId, fragmentData);
    fragmentDataSet.add(fragmentData);

    // keep track of all the drill bits that are used by this query
    includedBits.add(fragmentData.getEndpoint());
  }

  public String getFragmentStatesAsString() {
    return fragmentDataMap.toString();
  }

  void addFragmentStatusTracker(final PlanFragment fragment, final boolean isRoot) {
    addFragment(new FragmentData(fragment.getHandle(), fragment.getAssignment(), isRoot));
  }

  /**
   * Stop all fragments with a currently active status.
   */
  void cancelExecutingFragments(final DrillbitContext drillbitContext, final FragmentExecutor rootRunner) {
    final Controller controller = drillbitContext.getController();
    for(FragmentData data : fragmentDataSet) {
      final FragmentStatus fragmentStatus = data.getStatus();
      switch(fragmentStatus.getProfile().getState()) {
      case SENDING:
      case AWAITING_ALLOCATION:
      case RUNNING:
        if (rootRunner != null) {
            rootRunner.cancel();
        } else {
          final DrillbitEndpoint endpoint = data.getEndpoint();
          final FragmentHandle handle = fragmentStatus.getHandle();
          // TODO is the CancelListener redundant? Does the FragmentStatusListener get notified of the same?
          controller.getTunnel(endpoint).cancelFragment(new CancelListener(endpoint, handle), handle);
        }
        break;

      case FINISHED:
      case CANCELLED:
      case FAILED:
        // nothing to do
        break;
      }
    }
  }

  /*
   * This assumes that the FragmentStatusListener implementation takes action when it hears
   * that the target fragment has been cancelled. As a result, this listener doesn't do anything
   * but log messages.
   */
  private class CancelListener extends EndpointListener<Ack, FragmentHandle> {
    public CancelListener(final DrillbitEndpoint endpoint, final FragmentHandle handle) {
      super(endpoint, handle);
    }

    @Override
    public void failed(final RpcException ex) {
      logger.error("Failure while attempting to cancel fragment {} on endpoint {}.", value, endpoint, ex);
    }

    @Override
    public void success(final Ack value, final ByteBuf buf) {
      if (!value.getOk()) {
        logger.warn("Remote node {} responded negative on cancellation request for fragment {}.", endpoint, value);
      }
    }
  }

  public RootStatusReporter getRootStatusHandler(final FragmentContext context) {
    return new RootStatusReporter(context);
  }

  class RootStatusReporter extends AbstractStatusReporter {
    private RootStatusReporter(final FragmentContext context) {
      super(context);
    }

    @Override
    protected void statusChange(final FragmentHandle handle, final FragmentStatus status) {
      statusUpdate(status);
    }
  }

  QueryState updateQueryStateInStore(final QueryState queryState) {
    switch (queryState) {
      case PENDING:
      case RUNNING:
      case CANCELLATION_REQUESTED:
        profileEStore.put(stringQueryId, getQueryInfo());  // store as ephemeral query profile.
        break;

      case COMPLETED:
      case CANCELED:
      case FAILED:
        try {
          profileEStore.delete(stringQueryId);
        } catch(Exception e) {
          logger.warn("Failure while trying to delete the estore profile for this query.", e);
        }

        // TODO(DRILL-2362) when do these ever get deleted?
        profilePStore.put(stringQueryId, getQueryProfile());
        break;

      default:
        throw new IllegalStateException("unrecognized queryState " + queryState);
    }

    return queryState;
  }

  private QueryInfo getQueryInfo() {
    return QueryInfo.newBuilder()
      .setQuery(runQuery.getPlan())
      .setState(foreman.getState())
      .setForeman(foreman.getQueryContext().getCurrentEndpoint())
      .setStart(startTime)
      .build();
  }

  public QueryProfile getQueryProfile() {
    final QueryProfile.Builder profileBuilder = QueryProfile.newBuilder()
        .setQuery(runQuery.getPlan())
        .setType(runQuery.getType())
        .setId(queryId)
        .setState(foreman.getState())
        .setForeman(foreman.getQueryContext().getCurrentEndpoint())
        .setStart(startTime)
        .setEnd(endTime)
        .setTotalFragments(fragmentDataSet.size())
        .setFinishedFragments(finishedFragments.get());

    if (planText != null) {
      profileBuilder.setPlan(planText);
    }

    for (int i = 0; i < fragmentDataMap.allocated.length; i++) {
      if (fragmentDataMap.allocated[i]) {
        final int majorFragmentId = fragmentDataMap.keys[i];
        final IntObjectOpenHashMap<FragmentData> minorMap =
            (IntObjectOpenHashMap<FragmentData>) ((Object[]) fragmentDataMap.values)[i];
        final MajorFragmentProfile.Builder fb = MajorFragmentProfile.newBuilder()
            .setMajorFragmentId(majorFragmentId);
        for (int v = 0; v < minorMap.allocated.length; v++) {
          if (minorMap.allocated[v]) {
            final FragmentData data = (FragmentData) ((Object[]) minorMap.values)[v];
            fb.addMinorFragmentProfile(data.getStatus().getProfile());
          }
        }
        profileBuilder.addFragmentProfile(fb);
      }
    }

    return profileBuilder.build();
  }

  void setPlanText(final String planText) {
    this.planText = planText;
  }

  void markStartTime() {
    startTime = System.currentTimeMillis();
  }

  void markEndTime() {
    endTime = System.currentTimeMillis();
  }
}
