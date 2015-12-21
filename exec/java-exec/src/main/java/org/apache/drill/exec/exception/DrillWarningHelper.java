/**
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
package org.apache.drill.exec.exception;

import com.google.common.collect.Lists;
import org.apache.drill.exec.proto.UserBitShared.WarningMsg;

import java.util.IdentityHashMap;
import java.util.List;

/**
 * Helper class to manage warnings for FragmentContext and Foreman
 */
public class DrillWarningHelper {

  private List<IdentityHashMap<String, WarningMsg>> warnings;
  private String fragmentId;
  int numWarnings = 0;

  public DrillWarningHelper (String fragmentId) {
    this.warnings = Lists.newArrayList();
    this.fragmentId = fragmentId;
  }

  /**
   * Interface for operators to add warnings
   * @param operatorId operator warning is coming from
   * @param type pre-defined warning type
   * @param msg custom warning message
   */
  public void addWarning (int operatorId, String type, String msg) {
    assert (operatorId >= 0 && type != null);

    IdentityHashMap<String, WarningMsg> warningMap = null;
    if (operatorId < warnings.size()) {
      // we have received warnings from this operator before, go fetch it
      warningMap = warnings.get(operatorId);
    }

    if (warningMap == null) {
      // first warning for this operator
      warningMap = new IdentityHashMap<>();
    }

    WarningMsg wm = warningMap.get(type);
    if (wm == null) {
      // first warning of this type from the operator
      wm = WarningMsg.newBuilder()
          .setMessage(msg)
          .setCount(1)
          .build();
      numWarnings++;
    } else {
      // we have seen this type before from the operator, update it
      wm = wm.toBuilder()
          .setMessage(msg)
          .setCount(wm.getCount() + 1)
          .build();
    }
    warningMap.put(type, wm);
    warnings.add(operatorId, warningMap);
  }

  /**
   * Get Unique warning code
   * @param operatorId
   * @param type
   * @return
   */
  public String getWarningCode (int operatorId, String type) {
    return operatorId + "-" + this.fragmentId + "-" + type;
  }

  /**
   * Get flattened list of WarningMsg
   * @return list of WarningMsg
   */
  public List<WarningMsg> getWarnings () {
    List<WarningMsg> retWarnings = Lists.newArrayList();
    for (int opId = 0; opId < warnings.size(); opId++) {
      IdentityHashMap<String, WarningMsg> warning = warnings.get(opId);
      if (warning == null) {
        continue;
      }
      for (String type : warning.keySet()) {
        WarningMsg wm = warning.get(type);
        wm.toBuilder()
            .setWarningCode(getWarningCode(opId, type))
            .build();
        retWarnings.add(wm);
      }
    }
    return retWarnings;
  }

  public void clearWarnings () {
    for (IdentityHashMap<String, WarningMsg> warning : warnings) {
      warning.clear();
    }
    warnings.clear();
    numWarnings = 0;
  }

  public boolean hasWarnings () {
    return numWarnings > 0;
  }

}
