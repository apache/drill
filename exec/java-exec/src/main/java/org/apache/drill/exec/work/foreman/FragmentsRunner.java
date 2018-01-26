/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import org.apache.drill.common.concurrent.ExtendedLatch;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.ops.FragmentContextImpl;
import org.apache.drill.exec.physical.base.FragmentRoot;
import org.apache.drill.exec.proto.BitControl.InitializeFragments;
import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.GeneralRPCProtos;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.QueryResult.QueryState;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.UserClientConnection;
import org.apache.drill.exec.rpc.control.Controller;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.testing.ControlsInjector;
import org.apache.drill.exec.testing.ControlsInjectorFactory;
import org.apache.drill.exec.work.EndpointListener;
import org.apache.drill.exec.work.WorkManager.WorkerBee;
import org.apache.drill.exec.work.fragment.FragmentExecutor;
import org.apache.drill.exec.work.fragment.FragmentStatusReporter;
import org.apache.drill.exec.work.fragment.NonRootFragmentManager;
import org.apache.drill.exec.work.fragment.RootFragmentManager;


import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

/**
 * Is responsible for submitting query fragments for running (locally and remotely).
 */
public class FragmentsRunner {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FragmentsRunner.class);
  private static final ControlsInjector injector = ControlsInjectorFactory.getInjector(FragmentsRunner.class);

  private static final long RPC_WAIT_IN_MSECS_PER_FRAGMENT = 5000;

  private final WorkerBee bee;
  private final UserClientConnection initiatingClient;
  private final DrillbitContext drillbitContext;
  private final Foreman foreman;

  private List<PlanFragment> planFragments;
  private PlanFragment rootPlanFragment;
  private FragmentRoot rootOperator;

  public FragmentsRunner(WorkerBee bee, UserClientConnection initiatingClient, DrillbitContext drillbitContext, Foreman foreman) {
    this.bee = bee;
    this.initiatingClient = initiatingClient;
    this.drillbitContext = drillbitContext;
    this.foreman = foreman;
  }

  public WorkerBee getBee() {
    return bee;
  }

  public void setFragmentsInfo(List<PlanFragment> planFragments,
                                  PlanFragment rootPlanFragment,
                                  FragmentRoot rootOperator) {
    this.planFragments = planFragments;
    this.rootPlanFragment = rootPlanFragment;
    this.rootOperator = rootOperator;
  }

  /**
   * Submits root and non-root fragments fragments for running.
   * In case of success move query to the running state.
   */
  public void submit() throws ExecutionSetupException {
    assert planFragments != null;
    assert rootPlanFragment != null;
    assert rootOperator != null;

    QueryId queryId = foreman.getQueryId();
    assert queryId == rootPlanFragment.getHandle().getQueryId();

    QueryManager queryManager = foreman.getQueryManager();
    drillbitContext.getWorkBus().addFragmentStatusListener(queryId, queryManager.getFragmentStatusListener());
    drillbitContext.getClusterCoordinator().addDrillbitStatusListener(queryManager.getDrillbitStatusListener());

    logger.debug("Submitting fragments to run.");
    // set up the root fragment first so we'll have incoming buffers available.
    setupRootFragment(rootPlanFragment, rootOperator);
    setupNonRootFragments(planFragments);
    logger.debug("Fragments running.");
  }

  /**
   * Set up the root fragment (which will run locally), and submit it for execution.
   *
   * @param rootFragment root fragment
   * @param rootOperator root operator
   * @throws ExecutionSetupException
   */
  private void setupRootFragment(final PlanFragment rootFragment, final FragmentRoot rootOperator) throws ExecutionSetupException {
    QueryManager queryManager = foreman.getQueryManager();
    final FragmentContextImpl rootContext = new FragmentContextImpl(drillbitContext, rootFragment, foreman.getQueryContext(),
        initiatingClient, drillbitContext.getFunctionImplementationRegistry());
    final FragmentStatusReporter statusReporter = new FragmentStatusReporter(rootContext);
    final FragmentExecutor rootRunner = new FragmentExecutor(rootContext, rootFragment, statusReporter, rootOperator);
    final RootFragmentManager fragmentManager = new RootFragmentManager(rootFragment, rootRunner, statusReporter);

    queryManager.addFragmentStatusTracker(rootFragment, true);

    // FragmentManager is setting buffer for FragmentContext
    if (rootContext.isBuffersDone()) {
      // if we don't have to wait for any incoming data, start the fragment runner.
      bee.addFragmentRunner(rootRunner);
    } else {
      // if we do, record the fragment manager in the workBus.
      drillbitContext.getWorkBus().addFragmentManager(fragmentManager);
    }
  }


  /**
   * Set up the non-root fragments for execution. Some may be local, and some may be remote.
   * Messages are sent immediately, so they may start returning data even before we complete this.
   *
   * @param fragments the fragments
   */
  private void setupNonRootFragments(final Collection<PlanFragment> fragments) throws ExecutionSetupException {
    if (fragments.isEmpty()) {
      // nothing to do here
      return;
    }
    /*
     * We will send a single message to each endpoint, regardless of how many fragments will be
     * executed there. We need to start up the intermediate fragments first so that they will be
     * ready once the leaf fragments start producing data. To satisfy both of these, we will
     * make a pass through the fragments and put them into the remote maps according to their
     * leaf/intermediate state, as well as their target drillbit. Also filter the leaf/intermediate
     * fragments which are assigned to run on local Drillbit node (or Foreman node) into separate lists.
     *
     * This will help to schedule local
     */
    final Multimap<DrillbitEndpoint, PlanFragment> remoteLeafFragmentMap = ArrayListMultimap.create();
    final List<PlanFragment> localLeafFragmentList = new ArrayList<>();
    final Multimap<DrillbitEndpoint, PlanFragment> remoteIntFragmentMap = ArrayListMultimap.create();
    final List<PlanFragment> localIntFragmentList = new ArrayList<>();

    final DrillbitEndpoint localDrillbitEndpoint = drillbitContext.getEndpoint();
    // record all fragments for status purposes.
    for (final PlanFragment planFragment : fragments) {

      if (logger.isTraceEnabled()) {
        logger.trace("Tracking intermediate remote node {} with data {}", planFragment.getAssignment(),
            planFragment.getFragmentJson());
      }

      foreman.getQueryManager().addFragmentStatusTracker(planFragment, false);

      if (planFragment.getLeafFragment()) {
        updateFragmentCollection(planFragment, localDrillbitEndpoint, localLeafFragmentList, remoteLeafFragmentMap);
      } else {
        updateFragmentCollection(planFragment, localDrillbitEndpoint, localIntFragmentList, remoteIntFragmentMap);
      }
    }

    /*
     * We need to wait for the intermediates to be sent so that they'll be set up by the time
     * the leaves start producing data. We'll use this latch to wait for the responses.
     *
     * However, in order not to hang the process if any of the RPC requests fails, we always
     * count down (see FragmentSubmitFailures), but we count the number of failures so that we'll
     * know if any submissions did fail.
     */
    scheduleRemoteIntermediateFragments(remoteIntFragmentMap);

    // Setup local intermediate fragments
    for (final PlanFragment fragment : localIntFragmentList) {
      startLocalFragment(fragment);
    }

    injector.injectChecked(foreman.getQueryContext().getExecutionControls(), "send-fragments", ForemanException.class);
    /*
     * Send the remote (leaf) fragments; we don't wait for these. Any problems will come in through
     * the regular sendListener event delivery.
     */
    for (final DrillbitEndpoint ep : remoteLeafFragmentMap.keySet()) {
      sendRemoteFragments(ep, remoteLeafFragmentMap.get(ep), null, null);
    }

    // Setup local leaf fragments
    for (final PlanFragment fragment : localLeafFragmentList) {
      startLocalFragment(fragment);
    }
  }

  /**
   * Send all the remote fragments belonging to a single target drillbit in one request.
   *
   * @param assignment the drillbit assigned to these fragments
   * @param fragments the set of fragments
   * @param latch the countdown latch used to track the requests to all endpoints
   * @param fragmentSubmitFailures the submission failure counter used to track the requests to all endpoints
   */
  private void sendRemoteFragments(final DrillbitEndpoint assignment, final Collection<PlanFragment> fragments,
                                   final CountDownLatch latch, final FragmentSubmitFailures fragmentSubmitFailures) {
    @SuppressWarnings("resource")
    final Controller controller = drillbitContext.getController();
    final InitializeFragments.Builder fb = InitializeFragments.newBuilder();
    for(final PlanFragment planFragment : fragments) {
      fb.addFragment(planFragment);
    }
    final InitializeFragments initFrags = fb.build();

    logger.debug("Sending remote fragments to node: {}\nData: {}", assignment, initFrags);
    final FragmentSubmitListener listener =
        new FragmentSubmitListener(assignment, initFrags, latch, fragmentSubmitFailures);
    controller.getTunnel(assignment).sendFragments(listener, initFrags);
  }

  /**
   * Add planFragment into either of local fragment list or remote fragment map based on assigned Drillbit Endpoint node
   * and the local Drillbit Endpoint.
   *
   * @param planFragment plan fragment
   * @param localEndPoint local endpoint
   * @param localFragmentList local fragment list
   * @param remoteFragmentMap remote fragment map
   */
  private void updateFragmentCollection(final PlanFragment planFragment, final DrillbitEndpoint localEndPoint,
                                        final List<PlanFragment> localFragmentList,
                                        final Multimap<DrillbitEndpoint, PlanFragment> remoteFragmentMap) {
    final DrillbitEndpoint assignedDrillbit = planFragment.getAssignment();

    if (assignedDrillbit.equals(localEndPoint)) {
      localFragmentList.add(planFragment);
    } else {
      remoteFragmentMap.put(assignedDrillbit, planFragment);
    }
  }

  /**
   * Send remote intermediate fragment to the assigned Drillbit node.
   * Throw exception in case of failure to send the fragment.
   *
   * @param remoteFragmentMap - Map of Drillbit Endpoint to list of PlanFragment's
   */
  private void scheduleRemoteIntermediateFragments(final Multimap<DrillbitEndpoint, PlanFragment> remoteFragmentMap) {

    final int numIntFragments = remoteFragmentMap.keySet().size();
    final ExtendedLatch endpointLatch = new ExtendedLatch(numIntFragments);
    final FragmentSubmitFailures fragmentSubmitFailures = new FragmentSubmitFailures();

    // send remote intermediate fragments
    for (final DrillbitEndpoint ep : remoteFragmentMap.keySet()) {
      sendRemoteFragments(ep, remoteFragmentMap.get(ep), endpointLatch, fragmentSubmitFailures);
    }

    final long timeout = RPC_WAIT_IN_MSECS_PER_FRAGMENT * numIntFragments;
    if (numIntFragments > 0 && !endpointLatch.awaitUninterruptibly(timeout)) {
      long numberRemaining = endpointLatch.getCount();
      throw UserException.connectionError()
          .message("Exceeded timeout (%d) while waiting send intermediate work fragments to remote nodes. " +
                  "Sent %d and only heard response back from %d nodes.",
              timeout, numIntFragments, numIntFragments - numberRemaining).build(logger);
    }

    // if any of the intermediate fragment submissions failed, fail the query
    final List<FragmentSubmitFailures.SubmissionException> submissionExceptions =
        fragmentSubmitFailures.submissionExceptions;

    if (submissionExceptions.size() > 0) {
      Set<DrillbitEndpoint> endpoints = Sets.newHashSet();
      StringBuilder sb = new StringBuilder();
      boolean first = true;

      for (FragmentSubmitFailures.SubmissionException e : fragmentSubmitFailures.submissionExceptions) {
        DrillbitEndpoint endpoint = e.drillbitEndpoint;
        if (endpoints.add(endpoint)) {
          if (first) {
            first = false;
          } else {
            sb.append(", ");
          }
          sb.append(endpoint.getAddress());
        }
      }
      throw UserException.connectionError(submissionExceptions.get(0).rpcException)
          .message("Error setting up remote intermediate fragment execution")
          .addContext("Nodes with failures", sb.toString()).build(logger);
    }
  }


  /**
   * Start the locally assigned leaf or intermediate fragment
   *
   * @param fragment fragment
   */
  private void startLocalFragment(final PlanFragment fragment) throws ExecutionSetupException {
    logger.debug("Received local fragment start instruction", fragment);

    final FragmentContextImpl fragmentContext = new FragmentContextImpl(drillbitContext, fragment, drillbitContext.getFunctionImplementationRegistry());
    final FragmentStatusReporter statusReporter = new FragmentStatusReporter(fragmentContext);
    final FragmentExecutor fragmentExecutor = new FragmentExecutor(fragmentContext, fragment, statusReporter);

    // we either need to start the fragment if it is a leaf fragment, or set up a fragment manager if it is non leaf.
    if (fragment.getLeafFragment()) {
      bee.addFragmentRunner(fragmentExecutor);
    } else {
      // isIntermediate, store for incoming data.
      final NonRootFragmentManager manager = new NonRootFragmentManager(fragment, fragmentExecutor, statusReporter);
      drillbitContext.getWorkBus().addFragmentManager(manager);
    }
  }

  /**
   * Used by {@link FragmentSubmitListener} to track the number of submission failures.
   */
  private static class FragmentSubmitFailures {
    static class SubmissionException {
      final DrillbitEndpoint drillbitEndpoint;
      final RpcException rpcException;

      SubmissionException(final DrillbitEndpoint drillbitEndpoint,
                          final RpcException rpcException) {
        this.drillbitEndpoint = drillbitEndpoint;
        this.rpcException = rpcException;
      }
    }

    final List<SubmissionException> submissionExceptions = new LinkedList<>();

    void addFailure(final DrillbitEndpoint drillbitEndpoint, final RpcException rpcException) {
      submissionExceptions.add(new SubmissionException(drillbitEndpoint, rpcException));
    }
  }

  private class FragmentSubmitListener extends EndpointListener<GeneralRPCProtos.Ack, InitializeFragments> {
    private final CountDownLatch latch;
    private final FragmentSubmitFailures fragmentSubmitFailures;

    /**
     * Constructor.
     *
     * @param endpoint the endpoint for the submission
     * @param value the initialize fragments message
     * @param latch the latch to count down when the status is known; may be null
     * @param fragmentSubmitFailures the counter to use for failures; must be non-null iff latch is non-null
     */
    public FragmentSubmitListener(final DrillbitEndpoint endpoint, final InitializeFragments value,
                                  final CountDownLatch latch, final FragmentSubmitFailures fragmentSubmitFailures) {
      super(endpoint, value);
      Preconditions.checkState((latch == null) == (fragmentSubmitFailures == null));
      this.latch = latch;
      this.fragmentSubmitFailures = fragmentSubmitFailures;
    }

    @Override
    public void success(final GeneralRPCProtos.Ack ack, final ByteBuf byteBuf) {
      if (latch != null) {
        latch.countDown();
      }
    }

    @Override
    public void failed(final RpcException ex) {
      if (latch != null) { // this block only applies to intermediate fragments
        fragmentSubmitFailures.addFailure(endpoint, ex);
        latch.countDown();
      } else { // this block only applies to leaf fragments
        // since this won't be waited on, we can wait to deliver this event once the Foreman is ready
        logger.debug("Failure while sending fragment.  Stopping query.", ex);
        foreman.addToEventQueue(QueryState.FAILED, ex);
      }
    }

    @Override
    public void interrupted(final InterruptedException e) {
      // Foreman shouldn't get interrupted while waiting for the RPC outcome of fragment submission.
      // Consider the interrupt as failure.
      final String errMsg = "Interrupted while waiting for the RPC outcome of fragment submission.";
      logger.error(errMsg, e);
      failed(new RpcException(errMsg, e));
    }
  }
}

