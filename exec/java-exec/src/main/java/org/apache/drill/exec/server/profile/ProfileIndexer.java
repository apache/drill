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
package org.apache.drill.exec.server.profile;

import static org.apache.drill.exec.ExecConstants.DRILL_SYS_FILE_SUFFIX;

import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.coord.DistributedSemaphore;
import org.apache.drill.exec.coord.DistributedSemaphore.DistributedLease;
import org.apache.drill.exec.coord.zk.ZKClusterCoordinator;
import org.apache.drill.exec.coord.zk.ZkDistributedSemaphore;
import org.apache.drill.exec.exception.StoreException;
import org.apache.drill.exec.proto.UserBitShared.QueryProfile;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.QueryProfileStoreContext;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.sys.PersistentStoreConfig;
import org.apache.drill.exec.store.sys.store.DrillSysFilePathFilter;
import org.apache.drill.exec.store.sys.store.LocalPersistentStore;
import org.apache.drill.exec.store.sys.store.ProfileSet;
import org.apache.drill.exec.store.sys.store.provider.ZookeeperPersistentStoreProvider;
import org.apache.drill.exec.util.DrillFileSystemUtil;
import org.apache.drill.shaded.guava.com.google.common.base.Stopwatch;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manage profiles by archiving
 */
public class ProfileIndexer {
  private static final Logger logger = LoggerFactory.getLogger(ProfileIndexer.class);
  private static final String lockPathString = "/profileIndexer";
  private static final int DRILL_SYS_FILE_EXT_SIZE = DRILL_SYS_FILE_SUFFIX.length();

  private final ZKClusterCoordinator zkCoord;
  private final DrillFileSystem fs;
  private final Path basePath;
  private final ProfileSet profiles;
  private final int indexingRate;
  private final PathFilter sysFileSuffixFilter;
  private SimpleDateFormat indexedPathFormat;
  private final boolean useZkCoordinatedManagement;
  private DrillConfig drillConfig;

  private PersistentStoreConfig<QueryProfile> pStoreConfig;
  private LocalPersistentStore<QueryProfile> completedProfileStore;
  private Stopwatch indexWatch;
  private int indexedCount;
  private int currentProfileCount;


  /**
   * ProfileIndexer
   */
  public ProfileIndexer(ClusterCoordinator coord, DrillbitContext context) throws StoreException, IOException {
    drillConfig = context.getConfig();

    // FileSystem
    try {
      this.fs = inferFileSystem(drillConfig);
    } catch (IOException ex) {
      throw new StoreException("Unable to get filesystem", ex);
    }

    //Use Zookeeper for coordinated management
    final List<String> supportedFS = drillConfig.getStringList(ExecConstants.PROFILES_STORE_INDEX_SUPPORTED_FS);
    if (this.useZkCoordinatedManagement = supportedFS.contains(fs.getScheme())) {
      this.zkCoord = (ZKClusterCoordinator) coord;
    } else {
      this.zkCoord = null;
    }

    // Query Profile Store
    QueryProfileStoreContext pStoreContext = context.getProfileStoreContext();
    this.completedProfileStore = (LocalPersistentStore<QueryProfile>) pStoreContext.getCompletedProfileStore();
    this.pStoreConfig = pStoreContext.getProfileStoreConfig();
    this.basePath = completedProfileStore.getBasePath();

    this.indexingRate = drillConfig.getInt(ExecConstants.PROFILES_STORE_INDEX_MAX);
    this.profiles = new ProfileSet(indexingRate);
    this.indexWatch = Stopwatch.createUnstarted();
    this.sysFileSuffixFilter = new DrillSysFilePathFilter();
    String indexPathPattern = drillConfig.getString(ExecConstants.PROFILES_STORE_INDEX_FORMAT);
    this.indexedPathFormat = new SimpleDateFormat(indexPathPattern);
  }


  /**
   * Index profiles
   */
  public void indexProfiles() {
    this.indexWatch.start();

    // Acquire lock IFF required
    if (useZkCoordinatedManagement) {
      DistributedSemaphore indexerMutex = new ZkDistributedSemaphore(zkCoord.getCurator(), lockPathString, 1);
      try (DistributedLease lease = indexerMutex.acquire(0, TimeUnit.SECONDS)) {
        if (lease != null) {
          listAndIndex();
        } else {
          logger.debug("Couldn't get a lease acquisition");
        }
      } catch (Exception e) {
        //DoNothing since lease acquisition failed
        logger.error("Exception during lease-acquisition:: {}", e);
      }
    } else {
      try {
        listAndIndex();
      } catch (IOException e) {
        logger.error("Failed to index: {}", e);
      }
    }
    logger.info("Successfully indexed {} of {} profiles during startup in {} seconds", indexedCount, currentProfileCount, this.indexWatch.stop().elapsed(TimeUnit.SECONDS));
  }


  //Lists and Indexes the latest profiles
  private void listAndIndex() throws IOException {
    currentProfileCount = listForArchiving();
    indexedCount = 0;
    logger.info("Found {} profiles that need to be indexed. Will attempt to index {} profiles", currentProfileCount,
        (currentProfileCount > this.indexingRate) ? this.indexingRate : currentProfileCount);

    // Track MRU index paths
    Map<String, Path> mruIndexPath = new HashMap<>();
    if (currentProfileCount > 0) {
      while (!this.profiles.isEmpty()) {
        String profileToIndex = profiles.removeYoungest() + DRILL_SYS_FILE_SUFFIX;
        Path srcPath = new Path(basePath, profileToIndex);
        long profileStartTime = getProfileStart(srcPath);
        if (profileStartTime < 0) {
          logger.debug("Will skip indexing {}", srcPath);
          continue;
        }
        String indexPath = indexedPathFormat.format(new Date(profileStartTime));
        //Check if dest dir exists
        Path indexDestPath = null;
        if (!mruIndexPath.containsKey(indexPath)) {
          indexDestPath = new Path(basePath, indexPath);
          if (!fs.isDirectory(indexDestPath)) {
            // Build dir
            if (fs.mkdirs(indexDestPath)) {
              mruIndexPath.put(indexPath, indexDestPath);
            } else {
              //Creation failed. Did someone else create?
              if (fs.isDirectory(indexDestPath)) {
                mruIndexPath.put(indexPath, indexDestPath);
              }
            }
          } else {
            mruIndexPath.put(indexPath, indexDestPath);
          }
        } else {
          indexDestPath = mruIndexPath.get(indexPath);
        }

        //Attempt Move
        boolean renameStatus = false;
        if (indexDestPath != null) {
          Path destPath = new Path(indexDestPath, profileToIndex);
          renameStatus = DrillFileSystemUtil.rename(fs, srcPath, destPath);
          if (renameStatus) {
            indexedCount++;
          }
        }
        if (indexDestPath == null || !renameStatus) {
          // Stop attempting any more archiving since other StoreProviders might be archiving
          logger.error("Move failed for {} [{} | {}]", srcPath, indexDestPath == null, renameStatus);
          continue;
        }
      }
    }
  }

  // Deserialized and extract the profile's start time
  private long getProfileStart(Path srcPath) {
    try (InputStream is = fs.open(srcPath)) {
      QueryProfile profile = pStoreConfig.getSerializer().deserialize(IOUtils.toByteArray(is));
      return profile.getStart();
    } catch (IOException e) {
      logger.error("Unable to deserialize {}\n{}", srcPath, e.getMessage());
    }
    return Long.MIN_VALUE;
  }

  // List all profiles in store's root and identify potential candidates for archiving
  private int listForArchiving() throws IOException {
    // Not performing recursive search of profiles
    List<FileStatus> fileStatuses = DrillFileSystemUtil.listFiles(fs, basePath, false, sysFileSuffixFilter );

    int numProfilesInStore = 0;
    for (FileStatus stat : fileStatuses) {
      String profileName = stat.getPath().getName();
      //Strip extension and store only query ID
      profiles.add(profileName.substring(0, profileName.length() - DRILL_SYS_FILE_EXT_SIZE), false);
      numProfilesInStore++;
    }

    return numProfilesInStore;
  }

  // Infers File System of Local Store
  private DrillFileSystem inferFileSystem(DrillConfig drillConfig) throws IOException {
    boolean hasZkBlobRoot = drillConfig.hasPath(ZookeeperPersistentStoreProvider.DRILL_EXEC_SYS_STORE_PROVIDER_ZK_BLOBROOT);
    final Path blobRoot = hasZkBlobRoot ?
        new org.apache.hadoop.fs.Path(drillConfig.getString(ZookeeperPersistentStoreProvider.DRILL_EXEC_SYS_STORE_PROVIDER_ZK_BLOBROOT)) :
          LocalPersistentStore.getLogDir();

    return LocalPersistentStore.getFileSystem(drillConfig, blobRoot);
  }

}