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
package org.apache.drill.exec.util;

import org.apache.drill.exec.expr.fn.impl.DateUtility;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.proto.BitControl.QueryContextInformation;
import org.apache.drill.exec.proto.ExecProtos;
import org.apache.drill.exec.proto.helper.QueryIdHelper;

public class Utilities {

  public static String getFileNameForQueryFragment(FragmentContext context, String location, String tag) {
     /*
     * From the context, get the query id, major fragment id, minor fragment id. This will be used as the file name to
     * which we will dump the incoming buffer data
     */
    ExecProtos.FragmentHandle handle = context.getHandle();

    String qid = QueryIdHelper.getQueryId(handle.getQueryId());

    int majorFragmentId = handle.getMajorFragmentId();
    int minorFragmentId = handle.getMinorFragmentId();

    String fileName = String.format("%s//%s_%s_%s_%s", location, qid, majorFragmentId, minorFragmentId, tag);

    return fileName;
  }

  /**
   * Create QueryContextInformation with given <i>defaultSchemaName</i>. Rest of the members of the
   * QueryContextInformation is derived from the current state of the process.
   *
   * @param defaultSchemaName
   * @return
   */
  public static QueryContextInformation createQueryContextInfo(final String defaultSchemaName) {
    final long queryStartTime = System.currentTimeMillis();
    final int timeZone = DateUtility.getIndex(System.getProperty("user.timezone"));
    return QueryContextInformation.newBuilder()
        .setDefaultSchemaName(defaultSchemaName)
        .setQueryStartTime(queryStartTime)
        .setTimeZone(timeZone)
        .build();
  }

  /**
   * Read the manifest file and get the Drill version number
   * @return
   */
  public static String getDrillVersion() {
      String v = Utilities.class.getPackage().getImplementationVersion();
      return v;
  }
}
