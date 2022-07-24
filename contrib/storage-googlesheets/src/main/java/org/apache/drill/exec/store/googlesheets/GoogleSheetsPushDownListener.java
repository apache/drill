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

package org.apache.drill.exec.store.googlesheets;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.map.CaseInsensitiveMap;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.ops.OptimizerRulesContext;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.apache.drill.exec.store.base.filter.ExprNode;
import org.apache.drill.exec.store.base.filter.ExprNode.AndNode;
import org.apache.drill.exec.store.base.filter.ExprNode.ColRelOpConstNode;
import org.apache.drill.exec.store.base.filter.ExprNode.OrNode;
import org.apache.drill.exec.store.base.filter.FilterPushDownListener;
import org.apache.drill.exec.store.base.filter.FilterPushDownStrategy;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The GoogleSheets storage plugin accepts filters which are:
 * <ul>
 * <li>A single column = value expression </li>
 * <li>An AND'ed set of such expressions,</li>
 * <li>If the value is one with an unambiguous conversion to
 * a string. (That is, not dates, binary, maps, etc.)</li>
 * </ul>
 *
 * Note, at the moment, no filters are pushed down. Once we figure out the Google SDK for this,
 * we can easily uncomment out the lines below and the filters will be pushed down.
 */
public class GoogleSheetsPushDownListener implements FilterPushDownListener {

  public static Set<StoragePluginOptimizerRule> rulesFor(OptimizerRulesContext optimizerRulesContext) {
    return FilterPushDownStrategy.rulesFor(new GoogleSheetsPushDownListener());
  }

  @Override
  public String prefix() {
    return "GoogleSheets";
  }

  @Override
  public boolean isTargetScan(GroupScan groupScan) {
    return groupScan instanceof GoogleSheetsGroupScan;
  }

  @Override
  public ScanPushDownListener builderFor(GroupScan groupScan) {
    GoogleSheetsGroupScan gsScan = (GoogleSheetsGroupScan) groupScan;
    if (gsScan.hasFilters() || !gsScan.allowsFilters()) {
      return null;
    } else {
      return new GoogleSheetsScanPushDownListener(gsScan);
    }
  }

  private static class GoogleSheetsScanPushDownListener implements ScanPushDownListener {

    private final GoogleSheetsGroupScan groupScan;
    private final Map<String, String> filterParams = CaseInsensitiveMap.newHashMap();

    GoogleSheetsScanPushDownListener(GoogleSheetsGroupScan groupScan) {
      this.groupScan = groupScan;
      for (SchemaPath field : groupScan.columns()) {
        filterParams.put(field.getAsUnescapedPath(), field.getAsUnescapedPath());
      }
    }

    @Override
    public ExprNode accept(ExprNode node) {
      if (node instanceof OrNode) {
        return null;
      } else if (node instanceof ColRelOpConstNode) {
        // TODO Implement Filter Pushdowns
        // This entire method always returns null.  Google Sheets SDK may allow
        // filter pushdowns, however the SDK was so complicated and poorly documented, I was not able
        // to figure out how to implement filter pushdowns. If and when we figure that out, uncomment
        // the line below, and then the filter rules will be pushed down.
        return null;
        //return acceptRelOp((ColRelOpConstNode) node);
      } else {
        return null;
      }
    }

    private ColRelOpConstNode acceptRelOp(ColRelOpConstNode relOp) {
      return acceptColumn(relOp.colName) && acceptType(relOp.value.type) ? relOp : null;
    }

    /**
     * Only accept columns in the filter params list.
     */
    private boolean acceptColumn(String colName) {
      return filterParams.containsKey(colName);
    }

    /**
     * Only accept types which have an unambiguous mapping to
     * String.
     */
    private boolean acceptType(MinorType type) {
      switch (type) {
        case BIGINT:
        case BIT:
        case FLOAT4:
        case FLOAT8:
        case INT:
        case SMALLINT:
        case VARCHAR:
        case VARDECIMAL:
          return true;
        default:
          return false;
      }
    }

    /**
     * Convert the nodes to a map of param/string pairs using
     * the case specified in the storage plugin config.
     */
    @Override
    public Pair<GroupScan, List<RexNode>> transform(AndNode andNode) {
      Map<String, ExprNode.ColRelOpConstNode> filters = new HashMap<>();
      double selectivity = 1;
      for (ExprNode expr : andNode.children) {
        ColRelOpConstNode relOp = (ColRelOpConstNode) expr;
        filters.put(filterParams.get(relOp.colName), relOp);
        selectivity *= relOp.op.selectivity();
      }
      GoogleSheetsGroupScan newScan = new GoogleSheetsGroupScan(groupScan, filters, selectivity);
      return Pair.of(newScan, Collections.emptyList());
    }
  }
}
