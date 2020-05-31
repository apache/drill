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


package org.apache.drill.exec.store.ipfs;


import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.ipfs.api.MerkleNode;
import io.ipfs.cid.Cid;
import io.ipfs.multihash.Multihash;
import org.apache.drill.common.PlanStringBuilder;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.util.DrillVersionInfo;
import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.schedule.AffinityCreator;
import org.apache.drill.exec.store.schedule.AssignmentCreator;
import org.apache.drill.exec.store.schedule.CompleteWork;
import org.apache.drill.exec.store.schedule.EndpointByteMap;
import org.apache.drill.exec.store.schedule.EndpointByteMapImpl;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.base.Stopwatch;
import org.apache.drill.shaded.guava.com.google.common.cache.LoadingCache;
import org.apache.drill.shaded.guava.com.google.common.collect.ArrayListMultimap;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.apache.drill.shaded.guava.com.google.common.collect.ListMultimap;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


@JsonTypeName("ipfs-scan")
public class IPFSGroupScan extends AbstractGroupScan {
  private static final Logger logger = LoggerFactory.getLogger(IPFSGroupScan.class);
  private final IPFSContext ipfsContext;
  private final IPFSScanSpec ipfsScanSpec;
  private final IPFSStoragePluginConfig config;
  private List<SchemaPath> columns;

  private static final long DEFAULT_NODE_SIZE = 1000L;
  public static final int DEFAULT_USER_PORT = 31010;
  public static final int DEFAULT_CONTROL_PORT = 31011;
  public static final int DEFAULT_DATA_PORT = 31012;
  public static final int DEFAULT_HTTP_PORT = 8047;

  private ListMultimap<Integer, IPFSWork> assignments;
  private List<IPFSWork> ipfsWorkList = Lists.newArrayList();
  private ListMultimap<String, IPFSWork> endpointWorksMap;
  private List<EndpointAffinity> affinities;

  @JsonCreator
  public IPFSGroupScan(@JsonProperty("IPFSScanSpec") IPFSScanSpec ipfsScanSpec,
                       @JsonProperty("IPFSStoragePluginConfig") IPFSStoragePluginConfig ipfsStoragePluginConfig,
                       @JsonProperty("columns") List<SchemaPath> columns,
                       @JacksonInject StoragePluginRegistry pluginRegistry) {
    this(
        pluginRegistry.resolve(ipfsStoragePluginConfig, IPFSStoragePlugin.class).getIPFSContext(),
        ipfsScanSpec,
        columns
    );
  }

  public IPFSGroupScan(IPFSContext ipfsContext,
                       IPFSScanSpec ipfsScanSpec,
                       List<SchemaPath> columns) {
    super((String) null);
    this.ipfsContext = ipfsContext;
    this.ipfsScanSpec = ipfsScanSpec;
    this.config = ipfsContext.getStoragePluginConfig();
    logger.debug("GroupScan constructor called with columns {}", columns);
    this.columns = columns == null || columns.size() == 0 ? ALL_COLUMNS : columns;
    init();
  }

  private void init() {
    IPFSHelper ipfsHelper = ipfsContext.getIPFSHelper();
    endpointWorksMap = ArrayListMultimap.create();

    Multihash topHash = ipfsScanSpec.getTargetHash(ipfsHelper);
    try {
      Map<Multihash, String> leafAddrMap = getLeafAddrMappings(topHash);
      logger.debug("Iterating on {} leaves...", leafAddrMap.size());
      ClusterCoordinator coordinator = ipfsContext.getStoragePlugin().getContext().getClusterCoordinator();
      for (Multihash leaf : leafAddrMap.keySet()) {
        String peerHostname = leafAddrMap.get(leaf);

        Optional<DrillbitEndpoint> oep = coordinator.getAvailableEndpoints()
            .stream()
            .filter(a -> a.getAddress().equals(peerHostname))
            .findAny();
        DrillbitEndpoint ep;
        if (oep.isPresent()) {
          ep = oep.get();
          logger.debug("Using existing endpoint {}", ep.getAddress());
        } else {
          logger.debug("created new endpoint on the fly {}", peerHostname);
          //DRILL-7754: read ports & version info from IPFS instead of hard-coded
          ep = DrillbitEndpoint.newBuilder()
              .setAddress(peerHostname)
              .setUserPort(DEFAULT_USER_PORT)
              .setControlPort(DEFAULT_CONTROL_PORT)
              .setDataPort(DEFAULT_DATA_PORT)
              .setHttpPort(DEFAULT_HTTP_PORT)
              .setVersion(DrillVersionInfo.getVersion())
              .setState(DrillbitEndpoint.State.ONLINE)
              .build();
          //DRILL-7777: how to safely remove endpoints that are no longer needed once the query is completed?
          ClusterCoordinator.RegistrationHandle handle = coordinator.register(ep);
        }

        IPFSWork work = new IPFSWork(leaf);
        logger.debug("added endpoint {} to work {}", ep.getAddress(), work);
        work.getByteMap().add(ep, DEFAULT_NODE_SIZE);
        work.setOnEndpoint(ep);
        endpointWorksMap.put(ep.getAddress(), work);
        ipfsWorkList.add(work);
      }
    } catch (Exception e) {
      throw UserException
          .planError(e)
          .message("Exception during initialization of IPFS GroupScan")
          .build(logger);
    }
  }

  Map<Multihash, String> getLeafAddrMappings(Multihash topHash) {
    logger.debug("start to recursively expand nested IPFS hashes, topHash={}", topHash);
    Stopwatch watch = Stopwatch.createStarted();
    ForkJoinPool forkJoinPool = new ForkJoinPool(config.getNumWorkerThreads());
    IPFSTreeFlattener topTask = new IPFSTreeFlattener(topHash, false, ipfsContext);
    Map<Multihash, String> leafAddrMap = forkJoinPool.invoke(topTask);
    logger.debug("Took {} ms to expand hash leaves", watch.elapsed(TimeUnit.MILLISECONDS));

    return leafAddrMap;
  }

  private IPFSGroupScan(IPFSGroupScan that) {
    super(that);
    this.ipfsContext = that.ipfsContext;
    this.ipfsScanSpec = that.ipfsScanSpec;
    this.config = that.config;
    this.assignments = that.assignments;
    this.ipfsWorkList = that.ipfsWorkList;
    this.endpointWorksMap = that.endpointWorksMap;
    this.columns = that.columns;
  }

  @JsonProperty
  public List<SchemaPath> getColumns() {
    return columns;
  }

  @JsonIgnore
  public IPFSStoragePlugin getStoragePlugin() {
    return ipfsContext.getStoragePlugin();
  }

  @JsonProperty
  public IPFSScanSpec getIPFSScanSpec() {
    return ipfsScanSpec;
  }

  @Override
  public List<EndpointAffinity> getOperatorAffinity() {
    if (affinities == null) {
      affinities = AffinityCreator.getAffinityMap(ipfsWorkList);
    }
    return affinities;
  }

  @Override
  public int getMaxParallelizationWidth() {
    DrillbitEndpoint myself = ipfsContext.getStoragePlugin().getContext().getEndpoint();
    int width;
    if (endpointWorksMap.containsKey(myself.getAddress())) {
      // the foreman is also going to be a minor fragment worker under a UnionExchange operator
      width = ipfsWorkList.size();
    } else {
      // the foreman does not hold data, so we have to force parallelization
      // to make sure there is a UnionExchange operator
      width = ipfsWorkList.size() + 1;
    }
    logger.debug("getMaxParallelizationWidth: {}", width);
    return width;
  }

  @Override
  public void applyAssignments(List<DrillbitEndpoint> incomingEndpoints) {
    logger.debug("Applying assignments: endpointWorksMap = {}", endpointWorksMap);
    assignments = AssignmentCreator.getMappings(incomingEndpoints, ipfsWorkList);
  }

  @Override
  public IPFSSubScan getSpecificScan(int minorFragmentId) {
    logger.debug(String.format("getSpecificScan: minorFragmentId = %d", minorFragmentId));
    List<IPFSWork> workList = assignments.get(minorFragmentId);
    List<Multihash> scanSpecList = Lists.newArrayList();
    if (workList != null) {
      logger.debug("workList.size(): {}", workList.size());

      for (IPFSWork work : workList) {
        scanSpecList.add(work.getPartialRootHash());
      }
    }

    return new IPFSSubScan(ipfsContext, scanSpecList, ipfsScanSpec.getFormatExtension(), columns);
  }

  @Override
  public ScanStats getScanStats() {
    long recordCount = 100000 * endpointWorksMap.size();
    return new ScanStats(ScanStats.GroupScanProperty.NO_EXACT_ROW_COUNT, recordCount, 1, recordCount);
  }

  @Override
  public IPFSGroupScan clone(List<SchemaPath> columns) {
    logger.debug("IPFSGroupScan clone {}", columns);
    IPFSGroupScan cloned = new IPFSGroupScan(this);
    cloned.columns = columns;
    return cloned;
  }

  @Override
  @JsonIgnore
  public boolean canPushdownProjects(List<SchemaPath> columns) {
    return true;
  }

  @Override
  @JsonIgnore
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    logger.debug("getNewWithChildren called");
    return new IPFSGroupScan(this);
  }

  @Override
  public String getDigest() {
    return toString();
  }

  @Override
  public String toString() {
    return new PlanStringBuilder(this)
        .field("scan spec", ipfsScanSpec)
        .field("columns", columns)
        .toString();
  }

  private static class IPFSWork implements CompleteWork {
    private final EndpointByteMapImpl byteMap = new EndpointByteMapImpl();
    private final Multihash partialRoot;
    private DrillbitEndpoint onEndpoint = null;


    public IPFSWork(String root) {
      this.partialRoot = Cid.decode(root);
    }

    public IPFSWork(Multihash root) {
      this.partialRoot = root;
    }

    public Multihash getPartialRootHash() {
      return partialRoot;
    }

    public void setOnEndpoint(DrillbitEndpoint endpointAddress) {
      this.onEndpoint = endpointAddress;
    }

    @Override
    public long getTotalBytes() {
      return DEFAULT_NODE_SIZE;
    }

    @Override
    public EndpointByteMap getByteMap() {
      return byteMap;
    }

    @Override
    public int compareTo(CompleteWork o) {
      return 0;
    }

    @Override
    public String toString() {
      return new PlanStringBuilder(this)
          .field("partial root", partialRoot)
          .toString();
    }
  }

  //DRILL-7756: detect and warn about loops/recursions in case of a malformed tree
  static class IPFSTreeFlattener extends RecursiveTask<Map<Multihash, String>> {
    private final Multihash hash;
    private final boolean isProvider;
    private final Map<Multihash, String> ret = new LinkedHashMap<>();
    private final IPFSPeer myself;
    private final IPFSHelper helper;
    private final LoadingCache<Multihash, IPFSPeer> peerCache;
    private final LoadingCache<Multihash, List<Multihash>> providerCache;

    public IPFSTreeFlattener(Multihash hash, boolean isProvider, IPFSContext context) {
      this(
          hash,
          isProvider,
          context.getMyself(),
          context.getIPFSHelper(),
          context.getIPFSPeerCache(),
          context.getProviderCache()
      );
    }

    IPFSTreeFlattener(Multihash hash, boolean isProvider, IPFSPeer myself, IPFSHelper ipfsHelper,
                      LoadingCache<Multihash, IPFSPeer> peerCache, LoadingCache<Multihash, List<Multihash>> providerCache) {
      this.hash = hash;
      this.isProvider = isProvider;
      this.myself = myself;
      this.helper = ipfsHelper;
      this.peerCache = peerCache;
      this.providerCache = providerCache;
    }

    public IPFSTreeFlattener(IPFSTreeFlattener reference, Multihash hash, boolean isProvider) {
      this(hash, isProvider, reference.myself, reference.helper, reference.peerCache, reference.providerCache);
    }

    @Override
    public Map<Multihash, String> compute() {
      try {
        if (isProvider) {
          IPFSPeer peer = peerCache.getUnchecked(hash);
          ret.put(hash, peer.getDrillbitAddress().orElse(null));
          return ret;
        }

        MerkleNode metaOrSimpleNode = helper.getObjectLinksTimeout(hash);
        if (metaOrSimpleNode.links.size() > 0) {
          logger.debug("{} is a meta node", hash);
          //DRILL-7755: do something useful with leaf size, e.g. hint Drill about operation costs
          List<Multihash> intermediates = metaOrSimpleNode.links.stream().map(x -> x.hash).collect(Collectors.toList());

          ImmutableList.Builder<IPFSTreeFlattener> builder = ImmutableList.builder();
          for (Multihash intermediate : intermediates.subList(1, intermediates.size())) {
            builder.add(new IPFSTreeFlattener(this, intermediate, false));
          }
          ImmutableList<IPFSTreeFlattener> subtasks = builder.build();
          subtasks.forEach(IPFSTreeFlattener::fork);

          IPFSTreeFlattener first = new IPFSTreeFlattener(this, intermediates.get(0), false);
          ret.putAll(first.compute());
          subtasks.reverse().forEach(
              subtask -> ret.putAll(subtask.join())
          );
        } else {
          logger.debug("{} is a simple node", hash);
          List<IPFSPeer> providers = providerCache.getUnchecked(hash).stream()
              .map(peerCache::getUnchecked)
              .collect(Collectors.toList());
          providers = providers.stream()
              .filter(IPFSPeer::isDrillReady)
              .collect(Collectors.toList());
          if (providers.size() < 1) {
            logger.warn("No drill-ready provider found for leaf {}, adding foreman as the provider", hash);
            providers.add(myself);
          }

          logger.debug("Got {} providers for {} from IPFS", providers.size(), hash);
          ImmutableList.Builder<IPFSTreeFlattener> builder = ImmutableList.builder();
          for (IPFSPeer provider : providers.subList(1, providers.size())) {
            builder.add(new IPFSTreeFlattener(this, provider.getId(), true));
          }
          ImmutableList<IPFSTreeFlattener> subtasks = builder.build();
          subtasks.forEach(IPFSTreeFlattener::fork);

          List<String> possibleAddrs = new ArrayList<>();
          Multihash firstProvider = providers.get(0).getId();
          IPFSTreeFlattener firstTask = new IPFSTreeFlattener(this, firstProvider, true);
          String firstAddr = firstTask.compute().get(firstProvider);
          if (firstAddr != null) {
            possibleAddrs.add(firstAddr);
          }

          subtasks.reverse().forEach(
              subtask -> {
                String addr = subtask.join().get(subtask.hash);
                if (addr != null) {
                  possibleAddrs.add(addr);
                }
              }
          );

          if (possibleAddrs.size() < 1) {
            logger.error("All attempts to find an appropriate provider address for {} have failed", hash);
            throw UserException
                .planError()
                .message("No address found for any provider for leaf " + hash)
                .build(logger);
          } else {
            //DRILL-7753: better peer selection algorithm
            Random random = new Random();
            String chosenAddr = possibleAddrs.get(random.nextInt(possibleAddrs.size()));
            ret.clear();
            ret.put(hash, chosenAddr);
            logger.debug("Got peer host {} for leaf {}", chosenAddr, hash);
          }
        }
      } catch (IOException e) {
        throw UserException.planError(e).message("Exception during planning").build(logger);
      }
      return ret;
    }
  }
}
