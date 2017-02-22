/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.indexr;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.store.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import io.indexr.segment.Segment;
import io.indexr.segment.SegmentFd;
import io.indexr.segment.SegmentPool;
import io.indexr.segment.SegmentSchema;
import io.indexr.segment.helper.SegmentAssigner;
import io.indexr.segment.helper.SegmentOpener;
import io.indexr.segment.helper.SingleWork;
import io.indexr.segment.pack.DataPack;
import io.indexr.segment.pack.IndexMemCache;
import io.indexr.segment.pack.PackMemCache;
import io.indexr.server.HybridTable;

public class IndexRScanBatchCreator implements BatchCreator<IndexRSubScan> {
  private static final Logger logger = LoggerFactory.getLogger(IndexRScanBatchCreator.class);
  private static final float SINGLE_QUERY_CACHE_MAX_RATIO = 0.35f;

  private static final Cache<String, Assignment> assignmentCache = CacheBuilder.newBuilder()//
      .initialCapacity(1024)//
      .expireAfterAccess(5, TimeUnit.MINUTES)//
      .maximumSize(4096)//
      .build();

  @Override
  public ScanBatch getBatch(FragmentContext context, IndexRSubScan subScan, List<RecordBatch> children) throws ExecutionSetupException {
    Preconditions.checkArgument(children.isEmpty());

    IndexRSubScanSpec spec = subScan.getSpec();
    IndexRStoragePlugin plugin = subScan.getPlugin();
    HybridTable table = plugin.indexRNode().getTablePool().get(spec.tableName);
    SegmentSchema schema = table.schema().schema;

    Assignment assigment;
    try {
      assigment = assignmentCache.get(spec.scanId, new AssigmentCreator(plugin, subScan, table));
    } catch (ExecutionException e) {
      throw new ExecutionSetupException(e);
    }

    List<SingleWork> assignmentList = assigment.assignMap.get(spec.scanIndex);
    assignmentList = assignmentList == null ? Collections.emptyList() : assignmentList;

    SegmentOpener segmentOpener = new MySegmentOpener(//
        table.segmentPool(),//
        plugin.indexMemCache(),//
        assigment.cachePack ? plugin.packMemCache() : null);

    logger.debug("===================== subScan fragment {} assignmentList: {} ", spec.scanIndex, assignmentList);


    List<SingleWork> columned = new ArrayList<>();
    List<SingleWork> notColumned = new ArrayList<>();
    for (SingleWork work : assignmentList) {
      if (work.packId() >= 0) {
        columned.add(work);
      } else {
        notColumned.add(work);
      }
    }
    List<RecordReader> assignReaders = new ArrayList<>();
    if (assignmentList.isEmpty()) {
      logger.warn("==========subScan fragment {} have not record reader to assign", spec.scanIndex);
      assignReaders.add(new EmptyRecordReader(schema));
    } else {
      if (!columned.isEmpty()) {
        assignReaders.add(//
            new IndexRRecordReaderByPack(//
                spec.tableName,//
                schema,//
                subScan.getColumns(),//
                segmentOpener,//
                spec.rsFilter,//
                columned));
      }
      if (!notColumned.isEmpty()) {
        assignReaders.add(//
            new IndexRRecordReaderByRow(//
                spec.tableName,//
                schema,//
                subScan.getColumns(),//
                segmentOpener,//
                notColumned));
      }
    }
    return new ScanBatch(subScan, context, assignReaders.iterator());
  }

  private static class Assignment {
    Map<Integer, List<SingleWork>> assignMap;
    boolean cachePack;

    public Assignment(Map<Integer, List<SingleWork>> assignMap, boolean cachePack) {
      this.assignMap = assignMap;
      this.cachePack = cachePack;
    }
  }

  private static class AssigmentCreator implements Callable<Assignment> {
    IndexRStoragePlugin plugin;
    IndexRSubScan subScan;
    HybridTable tableToolBox;

    public AssigmentCreator(IndexRStoragePlugin plugin, IndexRSubScan subScan, HybridTable tableToolBox) {
      this.plugin = plugin;
      this.subScan = subScan;
      this.tableToolBox = tableToolBox;
    }

    @Override
    public Assignment call() throws Exception {
      SegmentPool segmentPool = tableToolBox.segmentPool();
      IndexMemCache indexMemCache = plugin.indexMemCache();
      PackMemCache packMemCache = plugin.packMemCache();
      IndexRStoragePluginConfig pluginConfig = plugin.getConfig();
      IndexRSubScanSpec spec = subScan.getSpec();

      Map<Integer, List<SingleWork>> assigmentMap = SegmentAssigner.assignBalance(//
          plugin.context().getEndpoint().getAddress(),
          spec.scanCount, //
          spec.endpointWorks, //
          spec.rsFilter, //
          segmentPool,//
          indexMemCache,//
          null,
          packMemCache);

      List<SchemaPath> columns = subScan.getColumns();

      int packCount = 0;
      for (List<SingleWork> works : assigmentMap.values()) {
        packCount += works.size();
      }
      long rowCount = packCount * DataPack.MAX_COUNT;
      double byteCostPerRow = DrillIndexRTable.byteCostPerRow(tableToolBox, columns, true);
      long totcalScanBytes = (long) (byteCostPerRow * rowCount);
      boolean cachePack = packMemCache != null && totcalScanBytes <= (SINGLE_QUERY_CACHE_MAX_RATIO * packMemCache.capacity());
      logger.debug("cache pack: {}", cachePack);
      return new Assignment(assigmentMap, cachePack);
    }
  }

  private static class MySegmentOpener implements SegmentOpener {
    SegmentPool segmentPool;
    IndexMemCache indexMemCache;
    PackMemCache packMemCache;

    public MySegmentOpener(SegmentPool segmentPool, IndexMemCache indexMemCache, PackMemCache packMemCache) {
      this.segmentPool = segmentPool;
      this.indexMemCache = indexMemCache;
      this.packMemCache = packMemCache;
    }

    @Override
    public Segment open(String name) throws IOException {
      SegmentFd fd = segmentPool.get(name);
      return fd.open(indexMemCache, null, packMemCache);
    }
  }
}
