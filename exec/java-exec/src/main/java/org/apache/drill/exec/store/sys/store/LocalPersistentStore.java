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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.Nullable;

import org.apache.commons.io.IOUtils;
import org.apache.drill.common.collections.ImmutableEntry;
import org.apache.drill.common.concurrent.AutoCloseableLock;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.sys.BasePersistentStore;
import org.apache.drill.exec.store.sys.PersistentStoreConfig;
import org.apache.drill.exec.store.sys.PersistentStoreMode;
import org.apache.drill.exec.util.DrillFileSystemUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;
import org.apache.drill.shaded.guava.com.google.common.base.Function;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.collect.Iterables;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.hadoop.fs.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalPersistentStore<V> extends BasePersistentStore<V> {
  private static final Logger logger = LoggerFactory.getLogger(LocalPersistentStore.class);

  //Provides a threshold above which we report an event's time
  private static final long RESPONSE_TIME_THRESHOLD_MSEC = 2000L;
  private static final String ARCHIVE_LOCATION = "archived";

  private static final int DRILL_SYS_FILE_EXT_SIZE = DRILL_SYS_FILE_SUFFIX.length();
  private final Path basePath;
  private final PersistentStoreConfig<V> config;
  private final DrillFileSystem fs;
  private int version = -1;
  private Function<String, Entry<String, V>> transformer;

  private ProfileSet profilesSet;
  private PathFilter sysFileSuffixFilter;
//  private String mostRecentProfile;
  private long basePathLastModified;
  private long lastKnownFileCount;
  private int maxSetCapacity;
  private Stopwatch listAndBuildWatch;
  private Stopwatch transformWatch;

  private boolean enableArchiving;
  private Path archivePath;
  private ProfileSet pendingArchivalSet;
  private int archivalThreshold;
  private int archivalRate;
  private Iterable<Entry<String, V>> iterableProfileSet;
  private Stopwatch archiveWatch;

  public LocalPersistentStore(DrillFileSystem fs, Path base, PersistentStoreConfig<V> config, DrillConfig drillConfig) {
    super();
    this.basePath = new Path(base, config.getName());
    this.config = config;
    this.fs = fs;
    //MRU Profile Cache
    this.profilesSet = new ProfileSet(drillConfig.getInt(ExecConstants.HTTP_MAX_PROFILES));
    this.basePathLastModified = 0L;
    this.lastKnownFileCount = 0L;
    this.sysFileSuffixFilter = new DrillSysFilePathFilter();
    //this.mostRecentProfile = null;

    //Initializing for archiving
    this.enableArchiving = drillConfig.getBoolean(ExecConstants.PROFILES_STORE_ARCHIVE_ENABLED); //(maxStoreCapacity > 0); //Implicitly infer
    if (enableArchiving == true ) {
      this.archivalThreshold = drillConfig.getInt(ExecConstants.PROFILES_STORE_CAPACITY);
      this.archivalRate = drillConfig.getInt(ExecConstants.PROFILES_STORE_ARCHIVE_RATE);
      this.pendingArchivalSet = new ProfileSet(archivalRate);
      this.archivePath = new Path(basePath, ARCHIVE_LOCATION);
    }

    // Timing
    this.listAndBuildWatch = Stopwatch.createUnstarted();
    this.transformWatch = Stopwatch.createUnstarted();
    this.archiveWatch = Stopwatch.createUnstarted();

    // One time transformer function instantiation
    this.transformer = new Function<String, Entry<String, V>>() {
      @Nullable
      @Override
      public Entry<String, V> apply(String key) {
        return new ImmutableEntry<>(key, get(key));
      }
    };

    //Base Dir
    try {
      if (!mkdirs(basePath)) {
        version++;
      }
      //Creating Archive if required
      if (enableArchiving) {
        try {
          if (!fs.exists(archivePath)) {
            mkdirs(archivePath);
          }
        } catch (IOException e) {
          logger.warn("Disabling profile archiving due to failure in creating profile archive {} : {}", archivePath, e);
          this.enableArchiving = false;
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("Failure setting pstore configuration path.");
    }
  }

  protected Path getBasePath() {
    return basePath;
  }

  @Override
  public PersistentStoreMode getMode() {
    return PersistentStoreMode.PERSISTENT;
  }

  private boolean mkdirs(Path path) throws IOException {
    return fs.mkdirs(path);
  }

  public static Path getLogDir() {
    String drillLogDir = System.getenv("DRILL_LOG_DIR");
    if (drillLogDir == null) {
      drillLogDir = System.getProperty("drill.log.dir");
    }
    if (drillLogDir == null) {
      drillLogDir = "/var/log/drill";
    }
    return new Path(new File(drillLogDir).getAbsoluteFile().toURI());
  }

  public static DrillFileSystem getFileSystem(DrillConfig config, Path root) throws IOException {
    Path blobRoot = root == null ? getLogDir() : root;
    Configuration fsConf = new Configuration();
    if (blobRoot.toUri().getScheme() != null) {
      fsConf.set(FileSystem.FS_DEFAULT_NAME_KEY, blobRoot.toUri().toString());
    }


    DrillFileSystem fs = new DrillFileSystem(fsConf);
    fs.mkdirs(blobRoot);
    return fs;
  }

  @Override
  public Iterator<Map.Entry<String, V>> getRange(int skip, int take) {
    //Marking currently seen modification time
    long currBasePathModified = 0L;
    try {
      currBasePathModified = fs.getFileStatus(basePath).getModificationTime();
    } catch (IOException e) {
      logger.error("Failed to get FileStatus for {}", basePath, e);
      throw new RuntimeException(e);
    }

    //No need to acquire lock since incoming requests are synchronized
    try {
      long expectedFileCount = fs.getFileStatus(basePath).getLen();
      logger.debug("Current ModTime: {} (Last known ModTime: {})", currBasePathModified, basePathLastModified);
      logger.debug("Expected {} files (Last known {} files)", expectedFileCount, lastKnownFileCount);

      //Force-read list of profiles based on change of any of the 3 states
      if (this.basePathLastModified < currBasePathModified  //Has ModificationTime changed?
          || this.lastKnownFileCount != expectedFileCount   //Has Profile Count changed?
          || (skip + take) > maxSetCapacity ) {             //Does requestSize exceed current cached size

        if (maxSetCapacity < (skip + take)) {
          logger.debug("Updating last Max Capacity from {} to {}", maxSetCapacity , (skip + take) );
          maxSetCapacity = skip + take;
        }
        //Mark Start Time
        listAndBuildWatch.reset().start();

        //Listing ALL DrillSysFiles
        //Can apply MostRecentProfile name as filter. Unfortunately, Hadoop (2.7.1) currently doesn't leverage this to speed up
        List<FileStatus> fileStatuses = DrillFileSystemUtil.listFiles(fs, basePath, false,
            sysFileSuffixFilter /*TODO: Use MostRecentProfile */
            );
        //Checking if empty
        if (fileStatuses.isEmpty()) {
          return Collections.emptyIterator();
        }

        //Force a reload of the profile.
        //Note: We shouldn't need to do this if the load is incremental (i.e. using mostRecentProfile)
        profilesSet.clear(maxSetCapacity);
        int numProfilesInStore = 0;

        if (enableArchiving) {
          pendingArchivalSet.clear();
        }

        //Constructing TreeMap from List
        for (FileStatus stat : fileStatuses) {
          String profileName = stat.getPath().getName();
          //Strip extension and store only query ID
          String oldestProfile = profilesSet.add(profileName.substring(0, profileName.length() - DRILL_SYS_FILE_EXT_SIZE));
          if (enableArchiving && oldestProfile != null) {
            pendingArchivalSet.add(oldestProfile, true);
          }
          numProfilesInStore++;
        }

        //Archive older profiles
        if (enableArchiving) {
          archiveProfiles(numProfilesInStore);
        }

        //Report Lag
        if (listAndBuildWatch.stop().elapsed(TimeUnit.MILLISECONDS) >= RESPONSE_TIME_THRESHOLD_MSEC) {
          logger.warn("Took {} ms to list & map {} profiles (out of {} profiles in store)", listAndBuildWatch.elapsed(TimeUnit.MILLISECONDS)
              , profilesSet.size(), numProfilesInStore);
        }
        //Recording last checked modified time and the most recent profile
        basePathLastModified = currBasePathModified;
        /*TODO: mostRecentProfile = profilesSet.getYoungest();*/
        lastKnownFileCount = expectedFileCount;

        //Transform profileSet for consumption
        transformWatch.start();
        iterableProfileSet = Iterables.transform(profilesSet, transformer);
        if (transformWatch.stop().elapsed(TimeUnit.MILLISECONDS) >= RESPONSE_TIME_THRESHOLD_MSEC) {
          logger.warn("Took {} ms to transform {} profiles", transformWatch.elapsed(TimeUnit.MILLISECONDS), profilesSet.size());
        }
      }
      return iterableProfileSet.iterator();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void archiveProfiles(int profilesInStoreCount) {
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
          //while (archivedCount < archivalRate) {
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
    pendingArchivalSet.clear();
  }

  private Path makePath(String name) {
    Preconditions.checkArgument(
        !name.contains("/") &&
        !name.contains(":") &&
        !name.contains(".."));
    return new Path(basePath, name + DRILL_SYS_FILE_SUFFIX);
  }

  @Override
  public boolean contains(String key) {
    try {
      return fs.exists(makePath(key));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public V get(String key) {
    try {
      Path path = makePath(key);
      if (!fs.exists(path)) {
        return null;
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    final Path path = makePath(key);
    try (InputStream is = fs.open(path)) {
      return config.getSerializer().deserialize(IOUtils.toByteArray(is));
    } catch (IOException e) {
      throw new RuntimeException("Unable to deserialize \"" + path + "\"", e);
    }
  }

  @Override
  public void put(String key, V value) {
    try (OutputStream os = fs.create(makePath(key))) {
      IOUtils.write(config.getSerializer().serialize(value), os);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean putIfAbsent(String key, V value) {
    try {
      Path p = makePath(key);
      if (fs.exists(p)) {
        return false;
      } else {
        try (OutputStream os = fs.create(makePath(key))) {
          IOUtils.write(config.getSerializer().serialize(value), os);
        }
        return true;
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void delete(String key) {
    try {
      fs.delete(makePath(key), false);
    } catch (IOException e) {
      logger.error("Unable to delete data from storage.", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
  }
}
