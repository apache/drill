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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.drill.common.exceptions.DrillRuntimeException;

/**
 * Injection for a single pause. Specifies how long to pause. This class is used internally for tracking
 * injected pauses; these pauses are specified via
 * {@link org.apache.drill.exec.ExecConstants#DRILLBIT_CONTROL_INJECTIONS} session option.
 *
 * TODO(DRILL-2697): Pause indefinitely until signalled, rather than for a specified time.
 */
@JsonAutoDetect(fieldVisibility = Visibility.ANY)
public class PauseInjection extends Injection {

  private final long millis;

  @JsonCreator // ensures instances are created only through JSON
  private PauseInjection(@JsonProperty("address") final String address,
                         @JsonProperty("port") final int port,
                         @JsonProperty("siteClass") final String siteClass,
                         @JsonProperty("desc") final String desc,
                         @JsonProperty("nSkip") final int nSkip,
                         @JsonProperty("nFire") final int nFire,
                         @JsonProperty("millis") final long millis) throws InjectionConfigurationException {
    super(address, port, siteClass, desc, nSkip, nFire);
    if (millis <= 0) {
      throw new InjectionConfigurationException("Pause millis is non-positive.");
    }
    this.millis = millis;
  }

  public void pause() {
    if (! injectNow()) {
      return;
    }
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      throw new DrillRuntimeException("Well, I should be sleeping.");
    }
  }
}
