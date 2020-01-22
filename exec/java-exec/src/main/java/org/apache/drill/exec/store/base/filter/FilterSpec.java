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

import java.util.List;

import org.apache.drill.exec.store.base.PlanStringBuilder;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

/**
 * A description of the constant filter terms for a scan divided
 * into CNF and DNF portions. The DNF portion contains expressions
 * for a single column.
 * <p>
 * This form is Jackson-serializable so that it may be included in
 * an execution plan (as is done in the "dummY" test mule.)
 */
@JsonInclude(value = Include.NON_EMPTY, content = Include.NON_NULL)
@JsonPropertyOrder({"andTerms", "orTerms"})
public class FilterSpec {

  private final List<RelOp> andTerms;
  private final DisjunctionFilterSpec orTerms;

  @JsonCreator
  public FilterSpec(
      @JsonProperty("andTerms") List<RelOp> andFilters,
      @JsonProperty("orTerms") DisjunctionFilterSpec orFilters) {
    this.andTerms = andFilters == null || andFilters.isEmpty() ? null : andFilters;
    this.orTerms = orFilters == null || orFilters.values.length == 0 ? null : orFilters;
  }

  public static FilterSpec build(List<RelOp> andFilters, DisjunctionFilterSpec orFilters) {
    FilterSpec spec = new FilterSpec(andFilters, orFilters);
    return spec.isEmpty() ? null : spec;
  }

  @JsonProperty("andTerms")
  public List<RelOp> andTerms() { return andTerms; }

  @JsonProperty("orTerms")
  public DisjunctionFilterSpec orTerms() { return orTerms; }

  /**
   * The number of partitions in the sense of scan "segments" given
   * by an OR clause. (May not correspond to HDFS file partitions.)
   * Assumes that each OR term represents a partition: <pre><code>
   * a < 10 OR a > 20 -- Two partitions
   * a = 10 or a = 20 -- Two partitions
   * a IN (10, 20) -- Two partitions</code></pre>
   * <p>
   * This is a simple assumption, specific cases may need a more
   * complex strategy.
   *
   * @return the number of logical partitions, which is the number
   * of OR clause terms (or 1 if no OR terms exist)
   */
  public int partitionCount() {
    return orTerms == null ? 1 : orTerms.values.length;
  }

  public static int parititonCount(FilterSpec filters) {
    return filters == null ? 1 : filters.partitionCount();
  }

  public List<RelOp> distribute(int orTerm) {
    return DisjunctionFilterSpec.distribute(andTerms, orTerms, orTerm);
  }

  /**
   * Compute selectivity of a CNF form of equality conditions. Without stats,
   * Drill assumes a selectivity of 0.15 for each equality, then multiplies
   * the selectivity of multiple columns. Place a lower limit of 0.001 on
   * the result, assuming the user wants to return some rows.
   *
   * @return selectivity of the filters, which may be used in computing
   * the cost of a scan after pushing filters into the scan
   */
  public double cnfSelectivity() {
    if (andTerms == null || andTerms.isEmpty()) {
      return 1.0;
    }
    double selectivity = 1.0;
    for (RelOp relOp : andTerms) {
      selectivity *= relOp.op.selectivity();
    }
    return Math.max(0.001, selectivity);
  }

  /**
   * Compute the combined selectivity of a set of AND (CNF) and OR
   * (DNF) filters. We assume the DNF returns a subset of rows, which
   * the CNF terms further reduce. Selectivity will be at least
   * 0.001, which assumes the user wants to return some rows.
   *
   * @return combined selectivity
   */
  public double selectivity() {
    double selectivity = cnfSelectivity();
    if (orTerms != null) {
      selectivity = Math.max(0.001, selectivity * orTerms.selectivity());
    }
    return selectivity;
  }

  /**
   * Drill scan stats want a row count, the selectivity is a means to
   * that end. Apply the selectivity of a set of filters to the given
   * row count to produce a reduced row count.
   *
   * @param filterSpec combined CNF and DNF terms, or null if no filter
   * @param rowCount original estimated row count before filtering
   * @return adjusted estimated row count after filtering
   */

  public static int applySelectivity(FilterSpec filterSpec, int rowCount) {
    return filterSpec == null ? rowCount : filterSpec.applySelectivity(rowCount);
  }

  public int applySelectivity(int rowCount) {
    return (int) Math.round(rowCount * selectivity());
  }

  @JsonIgnore
  public boolean isEmpty() {
    return (andTerms == null || andTerms.isEmpty()) &&
           (orTerms == null || orTerms.values.length == 0);
  }

  @Override
  public String toString() {
    return new PlanStringBuilder(this)
      .field("andFilters", andTerms)
      .field("orFilters", orTerms)
      .toString();
  }
}
