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

import java.util.Collection;
import javax.annotation.security.PermitAll;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.SecurityContext;
import javax.xml.bind.annotation.XmlRootElement;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.rest.DrillRestServer.UserAuthEnabled;
import org.apache.drill.exec.server.rest.auth.AuthDynamicFeature;
import org.apache.drill.exec.server.rest.auth.DrillUserPrincipal;
import org.apache.drill.exec.work.WorkManager;
import org.apache.drill.exec.work.foreman.rm.DistributedQueryQueue;
import org.apache.drill.exec.work.foreman.rm.DistributedQueryQueue.ZKQueueInfo;
import org.apache.drill.exec.work.foreman.rm.DynamicResourceManager;
import org.apache.drill.exec.work.foreman.rm.QueryQueue;
import org.apache.drill.exec.work.foreman.rm.ResourceManager;
import org.apache.drill.exec.work.foreman.rm.ThrottledResourceManager;
import org.glassfish.jersey.server.mvc.Viewable;

import com.fasterxml.jackson.annotation.JsonCreator;

@Path("/")
@PermitAll
public class DrillRoot {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillRoot.class);

  @Inject UserAuthEnabled authEnabled;
  @Inject WorkManager work;
  @Inject SecurityContext sc;

  @GET
  @Produces(MediaType.TEXT_HTML)
  public Viewable getClusterInfo() {
    return ViewableWithPermissions.create(authEnabled.get(), "/rest/index.ftl", sc, getClusterInfoJSON());
  }

  @SuppressWarnings("resource")
  @GET
  @Path("/cluster.json")
  @Produces(MediaType.APPLICATION_JSON)
  public ClusterInfo getClusterInfoJSON() {
    final Collection<DrillbitInfo> drillbits = Sets.newTreeSet();
    final Collection<String> mismatchedVersions = Sets.newTreeSet();

    final DrillbitContext dbContext = work.getContext();
    final DrillbitEndpoint currentDrillbit = dbContext.getEndpoint();
    final String currentVersion = currentDrillbit.getVersion();

    final DrillConfig config = dbContext.getConfig();
    final boolean userEncryptionEnabled =
        config.getBoolean(ExecConstants.USER_ENCRYPTION_SASL_ENABLED) ||
            config .getBoolean(ExecConstants.USER_SSL_ENABLED);
    final boolean bitEncryptionEnabled = config.getBoolean(ExecConstants.BIT_ENCRYPTION_SASL_ENABLED);
    // If the user is logged in and is admin user then show the admin user info
    // For all other cases the user info need-not or should-not be displayed
    OptionManager optionManager = work.getContext().getOptionManager();
    final boolean isUserLoggedIn = AuthDynamicFeature.isUserLoggedIn(sc);
    String adminUsers = isUserLoggedIn ?
            ExecConstants.ADMIN_USERS_VALIDATOR.getAdminUsers(optionManager) : null;
    String adminUserGroups = isUserLoggedIn ?
            ExecConstants.ADMIN_USER_GROUPS_VALIDATOR.getAdminUserGroups(optionManager) : null;

    // separate groups by comma + space
    if (adminUsers != null) {
      String[] groups = adminUsers.split(",");
      adminUsers = Joiner.on(", ").join(groups);
    }

    // separate groups by comma + space
    if (adminUserGroups != null) {
      String[] groups = adminUserGroups.split(",");
      adminUserGroups = Joiner.on(", ").join(groups);
    }

    final boolean shouldShowUserInfo = isUserLoggedIn &&
            ((DrillUserPrincipal)sc.getUserPrincipal()).isAdminUser();

    for (DrillbitEndpoint endpoint : work.getContext().getBits()) {
      final DrillbitInfo drillbit = new DrillbitInfo(endpoint,
              currentDrillbit.equals(endpoint),
              currentVersion.equals(endpoint.getVersion()));
      if (!drillbit.isVersionMatch()) {
        mismatchedVersions.add(drillbit.getVersion());
      }
      drillbits.add(drillbit);
    }
    logger.debug("Admin info: user: "  + adminUsers +  " user group: " + adminUserGroups +
            " userLoggedIn "  + isUserLoggedIn + " shouldShowUserInfo: " + shouldShowUserInfo );

    return new ClusterInfo(drillbits, currentVersion, mismatchedVersions,
      userEncryptionEnabled, bitEncryptionEnabled, adminUsers, adminUserGroups, shouldShowUserInfo,
      QueueInfo.build(dbContext.getResourceManager()));
  }

  /**
   * Pretty-printing wrapper class around the ZK-based queue summary.
   */

  @XmlRootElement
  public static class QueueInfo {
    private final ZKQueueInfo zkQueueInfo;

    public static QueueInfo build(ResourceManager rm) {

      // Consider queues enabled only if the ZK-based queues are in use.

      ThrottledResourceManager throttledRM = null;
      if (rm != null && rm instanceof DynamicResourceManager) {
        DynamicResourceManager dynamicRM = (DynamicResourceManager) rm;
        rm = dynamicRM.activeRM();
      }
      if (rm != null && rm instanceof ThrottledResourceManager) {
        throttledRM = (ThrottledResourceManager) rm;
      }
      if (throttledRM == null) {
        return new QueueInfo(null);
      }
      QueryQueue queue = throttledRM.queue();
      if (queue == null || !(queue instanceof DistributedQueryQueue)) {
        return new QueueInfo(null);
      }

      return new QueueInfo(((DistributedQueryQueue) queue).getInfo());
    }

    @JsonCreator
    public QueueInfo(ZKQueueInfo queueInfo) {
      zkQueueInfo = queueInfo;
    }

    public boolean isEnabled() { return zkQueueInfo != null; }

    public int smallQueueSize() {
      return isEnabled() ? zkQueueInfo.smallQueueSize : 0;
    }

    public int largeQueueSize() {
      return isEnabled() ? zkQueueInfo.largeQueueSize : 0;
    }

    public String threshold() {
      return isEnabled()
          ? Double.toString(zkQueueInfo.queueThreshold)
          : "N/A";
    }

    public String smallQueueMemory() {
      return isEnabled()
          ? toBytes(zkQueueInfo.memoryPerSmallQuery)
          : "N/A";
    }

    public String largeQueueMemory() {
      return isEnabled()
          ? toBytes(zkQueueInfo.memoryPerLargeQuery)
          : "N/A";
    }

    public String totalMemory() {
      return isEnabled()
          ? toBytes(zkQueueInfo.memoryPerNode)
          : "N/A";
    }

    private final long ONE_MB = 1024 * 1024;

    private String toBytes(long memory) {
      if (memory < 10 * ONE_MB) {
        return String.format("%,d bytes", memory);
      } else {
        return String.format("%,.0f MB", memory * 1.0D / ONE_MB);
      }
    }
  }

  @XmlRootElement
  public static class ClusterInfo {
    private final Collection<DrillbitInfo> drillbits;
    private final String currentVersion;
    private final Collection<String> mismatchedVersions;
    private final boolean userEncryptionEnabled;
    private final boolean bitEncryptionEnabled;
    private final String adminUsers;
    private final String adminUserGroups;
    private final boolean shouldShowUserInfo;
    private final QueueInfo queueInfo;

    @JsonCreator
    public ClusterInfo(Collection<DrillbitInfo> drillbits,
                       String currentVersion,
                       Collection<String> mismatchedVersions,
                       boolean userEncryption,
                       boolean bitEncryption,
                       String adminUsers,
                       String adminUserGroups,
                       boolean shouldShowUserInfo,
                       QueueInfo queueInfo) {
      this.drillbits = Sets.newTreeSet(drillbits);
      this.currentVersion = currentVersion;
      this.mismatchedVersions = Sets.newTreeSet(mismatchedVersions);
      this.userEncryptionEnabled = userEncryption;
      this.bitEncryptionEnabled = bitEncryption;
      this.adminUsers = adminUsers;
      this.adminUserGroups = adminUserGroups;
      this.shouldShowUserInfo = shouldShowUserInfo;
      this.queueInfo = queueInfo;
    }

    public Collection<DrillbitInfo> getDrillbits() {
      return Sets.newTreeSet(drillbits);
    }

    public String getCurrentVersion() {
      return currentVersion;
    }

    public Collection<String> getMismatchedVersions() {
      return Sets.newTreeSet(mismatchedVersions);
    }

    public boolean isUserEncryptionEnabled() { return userEncryptionEnabled; }

    public boolean isBitEncryptionEnabled() { return bitEncryptionEnabled; }

    public String getAdminUsers() { return adminUsers; }

    public String getAdminUserGroups() { return adminUserGroups; }

    public boolean shouldShowUserInfo() { return shouldShowUserInfo; }

    public QueueInfo queueInfo() { return queueInfo; }
  }

  public static class DrillbitInfo implements Comparable<DrillbitInfo> {
    private final String address;
    private final String userPort;
    private final String controlPort;
    private final String dataPort;
    private final String version;
    private final boolean current;
    private final boolean versionMatch;

    @JsonCreator
    public DrillbitInfo(DrillbitEndpoint drillbit, boolean current, boolean versionMatch) {
      this.address = drillbit.getAddress();
      this.userPort = String.valueOf(drillbit.getUserPort());
      this.controlPort = String.valueOf(drillbit.getControlPort());
      this.dataPort = String.valueOf(drillbit.getDataPort());
      this.version = Strings.isNullOrEmpty(drillbit.getVersion()) ? "Undefined" : drillbit.getVersion();
      this.current = current;
      this.versionMatch = versionMatch;
    }

    public String getAddress() { return address; }

    public String getUserPort() { return userPort; }

    public String getControlPort() { return controlPort; }

    public String getDataPort() { return dataPort; }

    public String getVersion() { return version; }

    public boolean isCurrent() { return current; }

    public boolean isVersionMatch() { return versionMatch; }

    /**
     * Method used to sort Drillbits. Current Drillbit goes first.
     * Then Drillbits with matching versions, after them Drillbits with mismatching versions.
     * Matching Drillbits are sorted according address natural order,
     * mismatching Drillbits are sorted according version, address natural order.
     *
     * @param drillbitToCompare Drillbit to compare against
     * @return -1 if Drillbit should be before, 1 if after in list
     */
    @Override
    public int compareTo(DrillbitInfo drillbitToCompare) {
      if (this.isCurrent()) {
        return -1;
      }

      if (drillbitToCompare.isCurrent()) {
        return 1;
      }

      if (this.isVersionMatch() == drillbitToCompare.isVersionMatch()) {
        if (this.version.equals(drillbitToCompare.getVersion())) {
          return this.address.compareTo(drillbitToCompare.getAddress());
        }
        return this.version.compareTo(drillbitToCompare.getVersion());
      }
      return this.versionMatch ? -1 : 1;
    }
  }
}
