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
package org.apache.drill.exec.store.base.filter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.store.base.PlanStringBuilder;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

@JsonInclude(Include.NON_NULL)
public class DisjunctionFilterSpec {
  public final String column;
  public final MinorType type;
  public final Object[] values;

  public DisjunctionFilterSpec(String column, MinorType type,
      Object[] values) {
    this.column = column;
    this.type = type;
    this.values = values;
  }

  public DisjunctionFilterSpec(List<RelOp> orTerms) {
    Preconditions.checkArgument(orTerms != null & orTerms.size() > 1);
    RelOp first = orTerms.get(0);
    column = first.colName;
    type = first.value.type;
    values = new Object[orTerms.size()];
    for (int i = 0; i < orTerms.size(); i++) {
      values[i] = orTerms.get(i).value.value;
    }
  }

  public List<List<RelOp>> distributeOverCnf(List<RelOp> cnfTerms) {

    // Distribute the (single) OR clause over the AND clauses
    // (a AND b AND (x OR y)) --> ((a AND b AND x), (a AND b AND y))

    List<List<RelOp>> dnfTerms = new ArrayList<>();
    for (int i = 0; i < values.length; i++) {
      dnfTerms.add(distributeTerm(cnfTerms, i));
    }
    return dnfTerms;
  }

  public RelOp toRelOp(int orIndex) {
    return new RelOp(RelOp.Op.EQ, column,
        new ConstantHolder(type, values[orIndex]));
  }

  public List<RelOp> distributeTerm(List<RelOp> andFilters, int orIndex) {
    List<RelOp> filters = new ArrayList<>();
    if (andFilters != null) {
      filters.addAll(andFilters);
    }
    filters.add(toRelOp(orIndex));
    return filters;
  }

  public static List<RelOp> distribute(List<RelOp> andFilters,
      DisjunctionFilterSpec orFilters, int orIndex) {
    Preconditions.checkArgument(orIndex == 0 || (orFilters != null && orIndex < orFilters.values.length));
    return orFilters == null ? andFilters :
      orFilters.distributeTerm(andFilters, orIndex);
  }

  /**
   * Compute the selectivity of this DNF (OR) clause. Drill assumes
   * the selectivity of = is 0.15. An OR is a series of equal statements,
   * so selectivity is n * 0.15. However, limit total selectivity to
   * 0.9 (that is, if there are more than 6 clauses in the DNF, assume
   * at least some reduction.)
   *
   * @return the estimated selectivity of this DNF clause
   */
  public double selectivity() {
    if (values.length == 0) {
      return 1.0;
    } else {
      return Math.min(0.9,  0.15 * values.length);
    }
  }

  @Override
  public String toString() {
    PlanStringBuilder builder = new PlanStringBuilder(this);
    builder.field("column", column);
    builder.field("type", type);
    List<String> strValues = Arrays.stream(values).map(v -> v.toString()).collect(Collectors.toList());
    builder.unquotedField("values",
        "[" + String.join(", ", strValues) + "]");
    return builder.toString();
  }
}
