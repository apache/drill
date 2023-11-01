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
package org.apache.drill.exec.metrics;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.util.Map;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;
import com.google.common.collect.ImmutableMap;

/**
 * Creates a Cpu GaugeSet
 */
@SuppressWarnings("restriction")
public class CpuGaugeSet implements MetricSet {

  private final OperatingSystemMXBean osMxBean = ManagementFactory.getOperatingSystemMXBean();
  private final RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();

  @Override
  public Map<String, Metric> getMetrics() {

    // We create each CPU gauge using an anonymous inner class that fetches
    // its metric from the relevant Java MX bean.

    Gauge<Double> osLoadAvgGauge = new Gauge<Double>() {
      @Override
      public Double getValue() {
        return osMxBean.getSystemLoadAverage();
      }
    };

    // Sun/Oracle runtimes only, c.f. DRILL-6702 and DRILL-8446.
    Gauge<Double> procLoadAvgGauge = new Gauge<Double>() {
      private boolean canUseSunInternalBean = true;

      @Override
      @SuppressWarnings("restriction")
      public Double getValue() {
        if (!canUseSunInternalBean) {
          return null;
        }

        // Make a single attempt to cast to the com.sun internal class. The mere
        // mention of com.sun must happen inside a try block.
        try {
          return ((com.sun.management.OperatingSystemMXBean)osMxBean).getProcessCpuLoad();

        } catch (NoClassDefFoundError | ClassCastException e) {
          org.slf4j.LoggerFactory.getLogger(CpuGaugeSet.class).warn(
            "The process load gauge is not supported on this Java runtime: {}",
            System.getProperty("java.vm.name")
          );
          canUseSunInternalBean = false;
          return null;
        }
      }
    };

    Gauge<Long> uptimeGauge = new Gauge<Long>() {
      @Override
      public Long getValue() {
        return runtimeMxBean.getUptime();
      }
    };

    return ImmutableMap.of(
      "os.load.avg", osLoadAvgGauge,
      "drillbit.load.avg", procLoadAvgGauge,
      "drillbit.uptime", uptimeGauge
    );
  }
}
