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
package org.apache.drill.exec.planner.physical;

import net.hydromatic.optiq.tools.FrameworkContext;

import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.server.options.OptionValidator;
import org.apache.drill.exec.server.options.TypeValidators.BooleanValidator;

public class PlannerSettings implements FrameworkContext{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PlannerSettings.class);

  private int numEndPoints = 0;
  private boolean useDefaultCosting = false; // True: use default Optiq costing, False: use Drill costing

  public static final OptionValidator EXCHANGE = new BooleanValidator("planner.disable_exchanges", false);

  public OptionManager options;

  public PlannerSettings(OptionManager options){
    this.options = options;
  }

  public boolean isSingleMode() {
    return options.getOption(EXCHANGE.getOptionName()).bool_val;
  }

  public int numEndPoints() {
    return numEndPoints;  
  }
  
  public boolean useDefaultCosting() {
    return useDefaultCosting;
  }
    
  public void setNumEndPoints(int numEndPoints) {
    this.numEndPoints = numEndPoints;
  }

  public void setUseDefaultCosting(boolean defcost) {
    this.useDefaultCosting = defcost;
  }
  
  @Override
  public <T> T unwrap(Class<T> clazz) {
    if(clazz == PlannerSettings.class){
      return (T) this;
    }else{
      return null;
    }
  }


}
