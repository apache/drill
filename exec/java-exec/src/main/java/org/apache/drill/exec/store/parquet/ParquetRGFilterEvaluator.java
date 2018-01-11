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
package org.apache.drill.exec.store.parquet;

import com.google.common.collect.Sets;
import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;
import org.apache.drill.exec.compile.sig.ConstantExpressionIdentifier;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.expr.fn.FunctionLookupContext;
import org.apache.drill.exec.expr.stat.ParquetFilterPredicate;
import org.apache.drill.exec.expr.stat.RangeExprEvaluator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.UdfUtilities;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.parquet.stat.ColumnStatCollector;
import org.apache.drill.exec.store.parquet.stat.ColumnStatistics;
import org.apache.drill.exec.store.parquet.stat.ParquetFooterStatCollector;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ParquetRGFilterEvaluator {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ParquetRGFilterEvaluator.class);

  public static boolean evalFilter(LogicalExpression expr, ParquetMetadata footer, int rowGroupIndex,
      OptionManager options, FragmentContext fragmentContext) {
    final HashMap<String, String> emptyMap = new HashMap<String, String>();
    return evalFilter(expr, footer, rowGroupIndex, options, fragmentContext, emptyMap);
  }

  public static boolean evalFilter(LogicalExpression expr, ParquetMetadata footer, int rowGroupIndex,
      OptionManager options, FragmentContext fragmentContext, Map<String, String> implicitColValues) {
    // figure out the set of columns referenced in expression.
    final Set<SchemaPath> schemaPathsInExpr = expr.accept(new FieldReferenceFinder(), null);
    final ColumnStatCollector columnStatCollector = new ParquetFooterStatCollector(footer, rowGroupIndex, implicitColValues,true, options);

    Map<SchemaPath, ColumnStatistics> columnStatisticsMap = columnStatCollector.collectColStat(schemaPathsInExpr);

    boolean canDrop = canDrop(expr, columnStatisticsMap, footer.getBlocks().get(rowGroupIndex).getRowCount(), fragmentContext, fragmentContext.getFunctionRegistry());
    return canDrop;
  }


  public static boolean canDrop(ParquetFilterPredicate parquetPredicate, Map<SchemaPath,
      ColumnStatistics> columnStatisticsMap, long rowCount) {
    boolean canDrop = false;
    if (parquetPredicate != null) {
      RangeExprEvaluator rangeExprEvaluator = new RangeExprEvaluator(columnStatisticsMap, rowCount);
      canDrop = parquetPredicate.canDrop(rangeExprEvaluator);
    }
    return canDrop;
  }


  public static boolean canDrop(LogicalExpression expr, Map<SchemaPath, ColumnStatistics> columnStatisticsMap,
      long rowCount, UdfUtilities udfUtilities, FunctionLookupContext functionImplementationRegistry) {
    ErrorCollector errorCollector = new ErrorCollectorImpl();
    LogicalExpression materializedFilter = ExpressionTreeMaterializer.materializeFilterExpr(
        expr, columnStatisticsMap, errorCollector, functionImplementationRegistry);

    if (errorCollector.hasErrors()) {
      logger.error("{} error(s) encountered when materialize filter expression : {}",
          errorCollector.getErrorCount(), errorCollector.toErrorString());
      return false;
    }

    Set<LogicalExpression> constantBoundaries = ConstantExpressionIdentifier.getConstantExpressionSet(materializedFilter);
    ParquetFilterPredicate parquetPredicate = (ParquetFilterPredicate) ParquetFilterBuilder.buildParquetFilterPredicate(
        materializedFilter, constantBoundaries, udfUtilities);

    return canDrop(parquetPredicate, columnStatisticsMap, rowCount);
  }

  /**
   * Search through a LogicalExpression, finding all internal schema path references and returning them in a set.
   */
  public static class FieldReferenceFinder extends AbstractExprVisitor<Set<SchemaPath>, Void, RuntimeException> {
    @Override
    public Set<SchemaPath> visitSchemaPath(SchemaPath path, Void value) {
      Set<SchemaPath> set = Sets.newHashSet();
      set.add(path);
      return set;
    }

    @Override
    public Set<SchemaPath> visitUnknown(LogicalExpression e, Void value) {
      Set<SchemaPath> paths = Sets.newHashSet();
      for (LogicalExpression ex : e) {
        paths.addAll(ex.accept(this, null));
      }
      return paths;
    }
  }
}
