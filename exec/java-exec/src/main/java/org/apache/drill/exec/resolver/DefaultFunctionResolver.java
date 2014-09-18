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

package org.apache.drill.exec.resolver;

import java.util.List;

import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.exec.expr.fn.DrillFuncHolder;

public class DefaultFunctionResolver implements FunctionResolver {

  @Override
  public DrillFuncHolder getBestMatch(List<DrillFuncHolder> methods,FunctionCall call) {

    int bestcost = Integer.MAX_VALUE;
    int currcost = Integer.MAX_VALUE;
    DrillFuncHolder bestmatch = null;

    for (DrillFuncHolder h : methods) {

      currcost = TypeCastRules.getCost(call, h);

      // if cost is lower than 0, func implementation is not matched, either w/ or w/o implicit casts
      if (currcost  < 0 ) {
        continue;
      }

      if (currcost < bestcost) {
        bestcost = currcost;
        bestmatch = h;
      }
    }

    if (bestcost < 0) {
      //did not find a matched func implementation, either w/ or w/o implicit casts
      //TODO: raise exception here?
      return null;
    } else {
      return bestmatch;
    }
  }

}
