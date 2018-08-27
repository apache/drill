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
package org.apache.drill.exec.physical.impl.xsort;

import java.util.List;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.ExecutorFragmentContext;
import org.apache.drill.exec.physical.config.ExternalSort;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.record.AbstractRecordBatch;
import org.apache.drill.exec.record.RecordBatch;

import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.exec.server.options.OptionManager;

public class ExternalSortBatchCreator implements BatchCreator<ExternalSort>{

  @Override
  public AbstractRecordBatch<ExternalSort> getBatch(ExecutorFragmentContext context, ExternalSort config, List<RecordBatch> children)
      throws ExecutionSetupException {
    Preconditions.checkArgument(children.size() == 1);

    // Prefer the managed version, but provide runtime and boot-time options
    // to disable it and revert to the "legacy" version. The legacy version
    // is retained primarily to allow cross-check testing against the managed
    // version, and as a fall back in the first release of the managed version.

    OptionManager optionManager = context.getOptions();
    boolean disableManaged = optionManager.getOption(ExecConstants.EXTERNAL_SORT_DISABLE_MANAGED_OPTION);
    if ( ! disableManaged ) {
      DrillConfig drillConfig = context.getConfig();
      disableManaged = drillConfig.hasPath(ExecConstants.EXTERNAL_SORT_DISABLE_MANAGED) &&
                       drillConfig.getBoolean(ExecConstants.EXTERNAL_SORT_DISABLE_MANAGED);
    }
    if (disableManaged) {
      return new ExternalSortBatch(config, context, children.iterator().next());
    } else {
      return new org.apache.drill.exec.physical.impl.xsort.managed.ExternalSortBatch(config, context, children.iterator().next());
    }
  }
}
