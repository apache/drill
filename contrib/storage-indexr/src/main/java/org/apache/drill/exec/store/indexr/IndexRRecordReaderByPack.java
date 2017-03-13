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

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import io.indexr.segment.CachedSegment;
import io.indexr.segment.ColumnSchema;
import io.indexr.segment.RSValue;
import io.indexr.segment.Segment;
import io.indexr.segment.SegmentSchema;
import io.indexr.segment.helper.SegmentOpener;
import io.indexr.segment.helper.SingleWork;
import io.indexr.segment.rc.Attr;
import io.indexr.segment.rc.RCOperator;
import io.indexr.segment.rc.UnknownOperator;

public class IndexRRecordReaderByPack extends IndexRRecordReader {
  private static final Logger log = LoggerFactory.getLogger(IndexRRecordReaderByPack.class);

  private RCOperator rsFilter;
  private int curStepId = 0;

  private long setupTimePoint = 0;
  private long getPackTime = 0;
  private long setValueTime = 0;
  private long lmCheckTime = 0;

  private boolean isLateMaterialization = false;
  private Segment curSegment;
  private int[] projectColumnIds;

  private PackReader packReader = new PackReader();

  public IndexRRecordReaderByPack(String tableName,//
                                  SegmentSchema schema,//
                                  List<SchemaPath> projectColumns,//
                                  SegmentOpener segmentOpener,//
                                  RCOperator rsFilter,//
                                  List<SingleWork> works) {
    super(tableName, schema, segmentOpener, projectColumns, works);
    this.segmentOpener = segmentOpener;
    this.rsFilter = rsFilter;
    this.works = works;
  }

  @Override
  public void setup(OperatorContext context, OutputMutator output) throws ExecutionSetupException {
    setupTimePoint = System.currentTimeMillis();
    super.setup(context, output);
    projectColumnIds = new int[projectColumnInfos.length];

    if (rsFilter != null) {
      Set<String> predicateColumns = new HashSet<>();
      boolean[] includeString = new boolean[1];
      boolean[] includeUnknown = new boolean[1];
      rsFilter.foreach(
          new Consumer<RCOperator>() {
            @Override
            public void accept(RCOperator op) {
              if (op instanceof UnknownOperator) {
                includeUnknown[0] = true;
              }
              for (Attr attr : op.attr()) {
                predicateColumns.add(attr.name());
                if (!attr.sqlType().isNumber()) {
                  includeString[0] = true;
                }
              }
            }
          });
      // The late materialization is worthy only when there are columns not included in predicates,
      // or predicates include string feilds.
      isLateMaterialization = (predicateColumns.size() < projectColumnInfos.length || includeString[0]) && !includeUnknown[0];

      log.debug("isLateMaterialization: {}, predicateColumns.size(): {}, projectColumnInfos.length: {}, includeString[0]: {}, includeUnknown[0]: {}, rsFilter: {}",
          isLateMaterialization, predicateColumns.size(), projectColumnInfos.length, includeString[0], includeUnknown[0], rsFilter);
    }
  }

  @Override
  public int next() {
    int read = -1;
    while (read <= 0) {
      try {
        if (packReader.hasMore()) {
          read = packReader.read();
        } else {
          if (curStepId >= works.size()) {
            return 0;
          }
          SingleWork stepWork = works.get(curStepId);
          curStepId++;

          Segment segment = segmentMap.get(stepWork.segment());
          int packId = stepWork.packId();
          Segment cachedSegment = new CachedSegment(segment);
          if (!beforeRead(cachedSegment, packId)) {
            continue;
          }

          packReader.setPack(cachedSegment, packId, projectColumnInfos, projectColumnIds);
        }
      } catch (Throwable t) {
        // No matter or what, don't thrown exception from here.
        // It will break the Drill algorithm and make system unpredictable.
        // I do think Drill should handle this...

        log.error("Read rows error, query may return incorrect result.", t);
        read = 0;
      }
    }
    return read;
  }

  /**
   * This method check whether those rows in packId possibly contains any rows we interested.
   *
   * @return false when this pack can be ignored.
   */
  private boolean beforeRead(Segment segment, int packId) throws IOException {
    List<ColumnSchema> schemas = segment.schema().getColumns();
    if (curSegment != segment) {
      curSegment = segment;

      // Set the project columns to the real columnIds.
      for (int i = 0; i < projectColumnInfos.length; i++) {
        ColumnSchema column = projectColumnInfos[i].columnSchema;
        Integer columnId = DrillIndexRTable.mapColumn(column, segment.schema());
        if (columnId == null) {
          throw new IllegalStateException(String.format("segment[%s]: column %s not found in %s",
              segment.name(), column, segment.schema()));
        }
        projectColumnIds[i] = columnId;
      }
    }
    // Set the attrs to the real columnIds.
    if (rsFilter != null) {
      rsFilter.materialize(schemas);
    }

    if (!isLateMaterialization || rsFilter == null) {
      return true;
    }

    long time2 = System.currentTimeMillis();
    byte res = rsFilter.roughCheckOnRow(segment, packId);
    lmCheckTime += System.currentTimeMillis() - time2;

    if (res == RSValue.None) {
      log.debug("ignore (LM) segment: {}, pack: {}", segment.name(), packId);
    } else {
      log.debug("hit (LM) segment: {}, packId: {}", segment.name(), packId);
    }

    return res != RSValue.None;
  }

  @Override
  public void close() throws Exception {
    super.close();
    long now = System.currentTimeMillis();
    log.debug("cost: total: {}ms, getPack: {}ms, setValue: {}ms, lmCheck: {}ms", now - setupTimePoint, getPackTime, setValueTime, lmCheckTime);
  }
}
