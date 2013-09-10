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
package org.apache.drill.exec.ref.rse;

import java.io.OutputStream;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.StorageEngineConfigBase;
import org.apache.drill.common.logical.data.Store;
import org.apache.drill.exec.ref.rops.DataWriter.ConverterType;

import com.fasterxml.jackson.annotation.JsonTypeName;

public class ConsoleRSE extends RSEBase {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ConsoleRSE.class);
  
  private final DrillConfig dConfig;
  
  public static enum Pipe {
    STD_OUT, STD_ERR
  };

  public ConsoleRSE(ConsoleRSEConfig engineConfig, DrillConfig dConfig){
    this.dConfig = dConfig;
  }
  
  public static class ConsoleOutputConfig {
    public Pipe pipe = Pipe.STD_OUT;
    public ConverterType type = ConverterType.JSON;
  }
  
  @JsonTypeName("console") public static class ConsoleRSEConfig extends StorageEngineConfigBase {
    @Override
    public boolean equals(Object o) {
      // if fields are added to this class this method needs to be updated
      return true;
    }
  }
  
  public boolean supportsWrite() {
    return true;
  }

  @Override
  public RecordRecorder getWriter(Store store) {
    ConsoleOutputConfig config = store.getTarget().getWith(dConfig, ConsoleOutputConfig.class);
    OutputStream out = config.pipe == Pipe.STD_OUT ? System.out : System.err;
    return new OutputStreamWriter(out, config.type, false);
  }

}
