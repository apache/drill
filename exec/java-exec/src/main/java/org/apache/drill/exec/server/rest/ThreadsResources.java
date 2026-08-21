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
package org.apache.drill.exec.server.rest;

import jakarta.annotation.security.RolesAllowed;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import org.apache.drill.exec.server.rest.auth.DrillUserPrincipal;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Exposes a JVM thread dump for the Drillbit. Admin-only — stack traces can
 * leak sensitive context (in-flight query SQL, credentials in handlers).
 */
@Path("/api/v1/threads")
@RolesAllowed(DrillUserPrincipal.ADMIN_ROLE)
public class ThreadsResources {

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Map<String, Object> dumpThreads() {
    ThreadMXBean bean = ManagementFactory.getThreadMXBean();
    ThreadInfo[] infos = bean.dumpAllThreads(bean.isObjectMonitorUsageSupported(),
        bean.isSynchronizerUsageSupported());

    List<Map<String, Object>> threads = new ArrayList<>(infos.length);
    for (ThreadInfo info : infos) {
      Map<String, Object> entry = new HashMap<>();
      entry.put("id", info.getThreadId());
      entry.put("name", info.getThreadName());
      entry.put("state", info.getThreadState().name());
      entry.put("daemon", info.isDaemon());
      entry.put("priority", info.getPriority());
      entry.put("blockedCount", info.getBlockedCount());
      entry.put("blockedTime", info.getBlockedTime());
      entry.put("waitedCount", info.getWaitedCount());
      entry.put("waitedTime", info.getWaitedTime());
      entry.put("lockName", info.getLockName());
      entry.put("lockOwnerId", info.getLockOwnerId());
      entry.put("lockOwnerName", info.getLockOwnerName());
      entry.put("stackTrace", renderStackTrace(info.getStackTrace()));
      threads.add(entry);
    }

    Map<String, Object> result = new HashMap<>();
    result.put("count", threads.size());
    result.put("peakCount", bean.getPeakThreadCount());
    result.put("daemonCount", bean.getDaemonThreadCount());
    result.put("totalStartedCount", bean.getTotalStartedThreadCount());
    long[] deadlockedIds = bean.findDeadlockedThreads();
    result.put("deadlockedThreadIds", deadlockedIds == null
        ? List.of()
        : Arrays.stream(deadlockedIds).boxed().toArray());
    result.put("threads", threads);
    return result;
  }

  private static List<String> renderStackTrace(StackTraceElement[] frames) {
    List<String> rendered = new ArrayList<>(frames.length);
    for (StackTraceElement frame : frames) {
      rendered.add(frame.toString());
    }
    return rendered;
  }
}
