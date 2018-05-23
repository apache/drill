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
package org.apache.drill.exec.store.sys.store;

import static org.apache.drill.exec.ExecConstants.DRILL_SYS_FILE_SUFFIX;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.util.DrillFileSystemUtil;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;

/**
 * Archive profiles
 */
public class LocalPersistentStoreArchiver {
  private static final Logger logger = LoggerFactory.getLogger(LocalPersistentStoreArchiver.class);

  private static final String ARCHIVE_LOCATION = "archived";

  private final DrillFileSystem fs;
  private Path basePath;
  private Path archivePath;
  private ProfileSet pendingArchivalSet;
  private int archivalThreshold;
  private int archivalRate;
  private Stopwatch archiveWatch;

  public LocalPersistentStoreArchiver(DrillFileSystem fs, Path base, DrillConfig drillConfig) throws IOException {
    this.fs = fs;
    this.basePath = base;
    this.archivalThreshold = drillConfig.getInt(ExecConstants.PROFILES_STORE_CAPACITY);
    this.archivalRate = drillConfig.getInt(ExecConstants.PROFILES_STORE_ARCHIVE_RATE);
    this.pendingArchivalSet = new ProfileSet(archivalRate);
    this.archivePath = new Path(basePath, ARCHIVE_LOCATION);
    this.archiveWatch = Stopwatch.createUnstarted();

    try {
      if (!fs.exists(archivePath)) {
        fs.mkdirs(archivePath);
      }
    } catch (IOException e) {
      logger.error("Disabling profile archiving due to failure in creating profile archive {} : {}", archivePath, e);
      throw e;
    }
  }

  void archiveProfiles(int profilesInStoreCount) {
    if (profilesInStoreCount > archivalThreshold) {
      //We'll attempt to reduce to 90% of threshold, but in batches of archivalRate
      int excessCount = profilesInStoreCount - (int) Math.round(0.9*archivalThreshold);
      int numToArchive = Math.min(excessCount, archivalRate);
      logger.info("Found {} excess profiles. For now, will attempt archiving {} profiles to {}", excessCount
          , numToArchive, archivePath);
      try {
        if (fs.isDirectory(archivePath)) {
          int archivedCount = 0;
          archiveWatch.reset().start(); //Clocking
          while (!pendingArchivalSet.isEmpty()) {
            String toArchive = pendingArchivalSet.removeOldest() + DRILL_SYS_FILE_SUFFIX;
            boolean renameStatus = DrillFileSystemUtil.rename(fs, new Path(basePath, toArchive), new Path(archivePath, toArchive));
            if (!renameStatus) {
              //Stop attempting any more archiving since other StoreProviders might be archiving
              logger.error("Move failed for {} from {} to {}", toArchive, basePath.toString(), archivePath.toString());
              logger.warn("Skip archiving under the assumption that another Drillbit is archiving");
              break;
            }
            archivedCount++;
          }
          logger.info("Archived {} profiles to {} in {} ms", archivedCount, archivePath, archiveWatch.stop().elapsed(TimeUnit.MILLISECONDS));
        } else {
          logger.error("Unable to archive {} profiles to {}", pendingArchivalSet.size(), archivePath.toString());
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    //Clean up
    clearPending();
  }

  /**
   * Clears the remaining pending profiles
   */
  public void clearPending() {
    this.pendingArchivalSet.clear();
  }

  /**
   * Add a profile for archiving
   * @param profileName
   * @return youngest profile that will not be archived
   */
  public String addProfile(String profileName) {
    return this.pendingArchivalSet.add(profileName, true);
  }


}
