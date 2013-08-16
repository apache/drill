/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.optiq;

import net.hydromatic.optiq.prepare.OptiqPrepareImpl;
import net.hydromatic.optiq.rules.java.JavaRules;

import org.apache.drill.jdbc.DrillHandler;
import org.apache.drill.jdbc.Driver;
import org.eigenbase.relopt.RelOptPlanner;

/**
 * Implementation of {@link net.hydromatic.optiq.jdbc.OptiqPrepare} for Drill.
 */
public class DrillPrepareImpl extends OptiqPrepareImpl {
  final Driver driver;
  public DrillPrepareImpl(Driver driver) {
    super();
    this.driver = driver;
  }

  @Override
  protected RelOptPlanner createPlanner() {
    final RelOptPlanner planner = super.createPlanner();
    planner.addRule(EnumerableDrillRule.getInstance(driver.getClient()));

    // Enable when https://issues.apache.org/jira/browse/DRILL-57 fixed
    if (false) {
      planner.addRule(DrillValuesRule.INSTANCE);
      planner.removeRule(JavaRules.ENUMERABLE_VALUES_RULE);
    }
    return planner;
  }
}
