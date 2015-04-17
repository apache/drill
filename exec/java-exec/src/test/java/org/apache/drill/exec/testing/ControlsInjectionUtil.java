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
package org.apache.drill.exec.testing;

import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.exec.rpc.user.UserSession.QueryCountIncrementer;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.server.options.OptionValue;
import org.apache.drill.exec.testing.ExecutionControls.Controls;

import java.util.List;

import static org.junit.Assert.fail;

/**
 * Static methods for constructing exception and pause injections for testing purposes.
 */
public class ControlsInjectionUtil {
  /**
   * Constructor. Prevent instantiation of static utility class.
   */
  private ControlsInjectionUtil() {
  }

  private static final QueryCountIncrementer incrementer = new QueryCountIncrementer() {
    @Override
    public void increment(final UserSession session) {
      session.incrementQueryCount(this);
    }
  };

  public static void setControls(final DrillClient drillClient, final String controls) {
    validateControlsString(controls);
    try {
      final List<QueryDataBatch> results = drillClient.runQuery(
        UserBitShared.QueryType.SQL, String.format("alter session set `%s` = '%s'",
          ExecConstants.DRILLBIT_CONTROL_INJECTIONS, controls));
      for (final QueryDataBatch data : results) {
        data.release();
      }
    } catch (RpcException e) {
      fail("Could not set controls options: " + e.toString());
    }
  }

  public static void setControls(final UserSession session, final String controls) {
    validateControlsString(controls);
    final OptionValue opValue = OptionValue.createString(OptionValue.OptionType.SESSION,
      ExecConstants.DRILLBIT_CONTROL_INJECTIONS, controls);

    final OptionManager options = session.getOptions();
    try {
      options.getAdmin().validate(opValue);
      options.setOption(opValue);
    } catch (Exception e) {
      fail("Could not set controls options: " + e.getMessage());
    }
    incrementer.increment(session); // to simulate that a query completed
  }

  private static void validateControlsString(final String controls) {
    try {
      ExecutionControls.controlsOptionMapper.readValue(controls, Controls.class);
    } catch (Exception e) {
      fail("Could not validate controls JSON: " + e.getMessage());
    }
  }

  /**
   * Clears all the controls.
   */
  public static void clearControls(final DrillClient client) {
    setControls(client, ExecutionControls.DEFAULT_CONTROLS);
  }
}
