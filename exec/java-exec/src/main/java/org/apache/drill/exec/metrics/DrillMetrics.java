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
package org.apache.drill.exec.metrics;

import java.lang.management.ManagementFactory;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.Slf4jReporter;
import com.codahale.metrics.jvm.BufferPoolMetricSet;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;

public class DrillMetrics {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillMetrics.class);
  static final DrillConfig config = DrillConfig.create();

  private DrillMetrics() {
  }

  private static class RegistryHolder {
    public static final MetricRegistry REGISTRY;
//    private static JmxReporter jmxReporter = getJmxReporter();
//    private static Slf4jReporter logReporter = getLogReporter();

    static {
      REGISTRY = new MetricRegistry();
      registerSysStats();
    }

    private static void registerSysStats(){
      REGISTRY.registerAll(new GarbageCollectorMetricSet());
      REGISTRY.registerAll(new BufferPoolMetricSet(ManagementFactory.getPlatformMBeanServer()));
      REGISTRY.registerAll(new MemoryUsageGaugeSet());
      REGISTRY.registerAll(new ThreadStatesGaugeSet());
    }

    private static JmxReporter getJmxReporter() {
      if (config.getBoolean(ExecConstants.METRICS_JMX_OUTPUT_ENABLED)) {
        JmxReporter reporter = JmxReporter.forRegistry(getInstance()).build();
        reporter.start();

        return reporter;
      } else {
        return null;
      }
    }

    private static Slf4jReporter getLogReporter() {
      if (config.getBoolean(ExecConstants.METRICS_LOG_OUTPUT_ENABLED)) {
        Slf4jReporter reporter = Slf4jReporter.forRegistry(getInstance()).outputTo(logger)
            .convertRatesTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.MILLISECONDS).build();
        reporter.start(config.getInt(ExecConstants.METRICS_LOG_OUTPUT_INTERVAL), TimeUnit.SECONDS);

        return reporter;
      } else {
        return null;
      }
    }
  }

  private static void registerAll(String prefix, MetricSet metricSet, MetricRegistry registry) {
    for (Entry<String, Metric> entry : metricSet.getMetrics().entrySet()) {
      if (entry.getValue() instanceof MetricSet) {
        registerAll(prefix + "." + entry.getKey(), (MetricSet) entry.getValue(), registry);
      } else {
        registry.register(prefix + "." + entry.getKey(), entry.getValue());
      }
    }
  }

  public static MetricRegistry getInstance() {
    return RegistryHolder.REGISTRY;
  }

  public static void resetMetrics(){
    RegistryHolder.REGISTRY.removeMatching(new MetricFilter(){
      @Override
      public boolean matches(String name, Metric metric) {
        return true;
      }});
    RegistryHolder.registerSysStats();
  }

}
