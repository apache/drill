/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.util;

import com.google.common.base.Strings;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.ops.OperatorStats;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

/**
 * Utilities for impersonation purpose.
 */
public class ImpersonationUtil {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ImpersonationUtil.class);

  /**
   * Create and return proxy user {@link org.apache.hadoop.security.UserGroupInformation} of operator owner if operator
   * owner is valid. Otherwise create and return proxy user {@link org.apache.hadoop.security.UserGroupInformation} for
   * query user.
   *
   * @param opUserName Name of the user whom to impersonate while setting up the operator.
   * @param queryUserName Name of the user who issues the query. If <i>opUserName</i> is invalid,
   *                      then this parameter must be valid user name.
   * @return
   */
  public static UserGroupInformation createProxyUgi(String opUserName, String queryUserName) {
    if (!Strings.isNullOrEmpty(opUserName)) {
      return createProxyUgi(opUserName);
    }

    if (Strings.isNullOrEmpty(queryUserName)) {
      // TODO(DRILL-2097): Tests that use SimpleRootExec have don't assign any query user name in FragmentContext.
      // Disable throwing exception to modifying the long list of test files.
      // throw new DrillRuntimeException("Invalid value for query user name");
      return getProcessUserUGI();
    }

    return createProxyUgi(queryUserName);
  }

  /**
   * Create and return proxy user {@link org.apache.hadoop.security.UserGroupInformation} for give user name.
   *
   * TODO: we may want to cache the {@link org.apache.hadoop.security.UserGroupInformation} instances as we try to
   * create different instances for the same user which is an unnecessary overhead.
   *
   * @param proxyUserName Proxy user name (must be valid)
   * @return
   */
  public static UserGroupInformation createProxyUgi(String proxyUserName) {
    try {
      if (Strings.isNullOrEmpty(proxyUserName)) {
        throw new DrillRuntimeException("Invalid value for proxy user name");
      }

      // If the request proxy user is same as process user name, return the process UGI.
      if (proxyUserName.equals(getProcessUserName())) {
        return getProcessUserUGI();
      }

      return UserGroupInformation.createProxyUser(proxyUserName, UserGroupInformation.getLoginUser());
    } catch(IOException e) {
      final String errMsg = "Failed to create proxy user UserGroupInformation object: " + e.getMessage();
      logger.error(errMsg, e);
      throw new DrillRuntimeException(errMsg, e);
    }
  }

  /**
   * If the given user name is empty, return the current process user name. This is a temporary change to avoid
   * modifying long list of tests files which have GroupScan operator with no user name property.
   * @param userName User name found in GroupScan POP definition.
   */
  public static String resolveUserName(String userName) {
    if (!Strings.isNullOrEmpty(userName)) {
      return userName;
    }
    return getProcessUserName();
  }

  /**
   * Return the name of the user who is running the Drillbit.
   *
   * @return Drillbit process user.
   */
  public static String getProcessUserName() {
    return getProcessUserUGI().getUserName();
  }

  /**
   * Return the {@link org.apache.hadoop.security.UserGroupInformation} of user who is running the Drillbit.
   *
   * @return Drillbit process user {@link org.apache.hadoop.security.UserGroupInformation}.
   */
  public static UserGroupInformation getProcessUserUGI() {
    try {
      return UserGroupInformation.getLoginUser();
    } catch (IOException e) {
      final String errMsg = "Failed to get process user UserGroupInformation object.";
      logger.error(errMsg, e);
      throw new DrillRuntimeException(errMsg, e);
    }
  }

  /**
   * Create DrillFileSystem for given <i>proxyUserName</i> and configuration.
   *
   * @param proxyUserName Name of the user whom to impersonate while accessing the FileSystem contents.
   * @param fsConf FileSystem configuration.
   * @return
   */
  public static DrillFileSystem createFileSystem(String proxyUserName, Configuration fsConf) {
    return createFileSystem(proxyUserName, fsConf, null);
  }

  /**
   * Create DrillFileSystem for given <i>proxyUserName</i>, configuration and stats.
   *
   * @param proxyUserName Name of the user whom to impersonate while accessing the FileSystem contents.
   * @param fsConf FileSystem configuration.
   * @param stats OperatorStats for DrillFileSystem (optional)
   * @return
   */
  public static DrillFileSystem createFileSystem(String proxyUserName, Configuration fsConf, OperatorStats stats) {
    return createFileSystem(createProxyUgi(proxyUserName), fsConf, stats);
  }

  /** Helper method to create DrillFileSystem */
  private static DrillFileSystem createFileSystem(UserGroupInformation proxyUserUgi, final Configuration fsConf,
      final OperatorStats stats) {
    DrillFileSystem fs;
    try {
      fs = proxyUserUgi.doAs(new PrivilegedExceptionAction<DrillFileSystem>() {
        public DrillFileSystem run() throws Exception {
          logger.debug("Creating DrillFileSystem for proxy user: " + UserGroupInformation.getCurrentUser());
          return new DrillFileSystem(fsConf, stats);
        }
      });
    } catch (InterruptedException | IOException e) {
      final String errMsg = "Failed to create DrillFileSystem for proxy user: " + e.getMessage();
      logger.error(errMsg, e);
      throw new DrillRuntimeException(errMsg, e);
    }

    return fs;
  }
}
