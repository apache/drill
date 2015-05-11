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
package org.apache.drill.exec.store.parquet;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.work.ExecErrorConstants;

/*
 * Utility class where we can capture common logic between the two parquet readers
 */
public class ParquetReaderUtility {
  public static void checkDecimalTypeEnabled(OptionManager options) {
    if (options.getOption(PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY).bool_val == false) {
      throw UserException
          .unsupportedError()
          .message(ExecErrorConstants.DECIMAL_DISABLE_ERR_MSG)
          .build();
    }
  }

  public static int getIntFromLEBytes(byte[] input, int start) {
    int out = 0;
    int shiftOrder = 0;
    for (int i = start; i < start + 4; i++) {
      out |= (((input[i]) & 0xFF) << shiftOrder);
      shiftOrder += 8;
    }
    return out;
  }
}
