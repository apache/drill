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
import org.apache.drill.exec.exception.VersionMismatchException;
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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterables;

public class LocalPersistentStore<V> extends BasePersistentStore<V> {
  private static final Logger logger = LoggerFactory.getLogger(LocalPersistentStore.class);

  private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
  private final AutoCloseableLock readLock = new AutoCloseableLock(readWriteLock.readLock());
  private final AutoCloseableLock writeLock = new AutoCloseableLock(readWriteLock.writeLock());
  private final AutoCloseableLock readCachedProfilesLock = new AutoCloseableLock(new ReentrantLock());

  //Provides a threshold above which we report the time to load
  private static final long LIST_TIME_THRESHOLD_MSEC = 2000L;

  private static final int drillSysFileExtSize = DRILL_SYS_FILE_SUFFIX.length();
  private final Path basePath;
  private final PersistentStoreConfig<V> config;
  private final DrillFileSystem fs;
  private int version = -1;
  private TreeSet<String> profilesSet;
//  private String mostRecentProfile;
  private long basePathLastModified;
  private long lastKnownFileCount;
  private int maxSetCapacity;
  private Stopwatch listAndBuildWatch;
  private PathFilter sysFileSuffixFilter;

  public LocalPersistentStore(DrillFileSystem fs, Path base, PersistentStoreConfig<V> config) {
    super();
    this.basePath = new Path(base, config.getName());
    this.config = config;
    this.fs = fs;
    this.profilesSet = new TreeSet<String>();
    this.basePathLastModified = 0L;
    this.lastKnownFileCount = 0L;
    this.sysFileSuffixFilter = new DrillSysFilePathFilter();
    //this.mostRecentProfile = null;
    this.listAndBuildWatch = Stopwatch.createUnstarted();

    try {
      if (!fs.mkdirs(basePath)) {
        version++;
      }
    } catch (IOException e) {
      throw new RuntimeException("Failure setting pstore configuration path.");
    }
  }

  @Override
  public PersistentStoreMode getMode() {
    return PersistentStoreMode.PERSISTENT;
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

    logger.info("Requesting thread: {}-{}" , Thread.currentThread().getName(), Thread.currentThread().getId());
    //Acquiring lock to avoid reloading for request coming in before completion of profile read
    try (AutoCloseableLock lock = readCachedProfilesLock.open()) {
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
              sysFileSuffixFilter
              );
          //Checking if empty
          if (fileStatuses.isEmpty()) {
            //WithoutFilter::
            return Collections.emptyIterator();
          }
          //Force a reload of the profile.
          //Note: We shouldn't need to do this if the load is incremental (i.e. using mostRecentProfile)
          profilesSet.clear();
          int profileCounter = 0;
          //Constructing TreeMap from List
          for (FileStatus stat : fileStatuses) {
            String profileName = stat.getPath().getName();
            profileCounter = addToProfileSet(profileName, profileCounter, maxSetCapacity);
          }

          //Report Lag
          if (listAndBuildWatch.stop().elapsed(TimeUnit.MILLISECONDS) >= LIST_TIME_THRESHOLD_MSEC) {
            logger.warn("Took {} ms to list+map from {} profiles", listAndBuildWatch.elapsed(TimeUnit.MILLISECONDS), profileCounter);
          }
          //Recording last checked modified time and the most recent profile
          basePathLastModified = currBasePathModified;
          /*TODO: mostRecentProfile = profilesSet.first();*/
          lastKnownFileCount = expectedFileCount;
        }

        return Iterables.transform(Iterables.limit(Iterables.skip(profilesSet, skip), take), new Function<String, Entry<String, V>>() {
          @Nullable
          @Override
          public Entry<String, V> apply(String key) {
            return new ImmutableEntry<>(key, get(key));
          }
        }).iterator();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Add profile name to a TreeSet
   * @param profileName Name of the profile to add
   * @param currentSize Provided so as to avoid the need to recount
   * @param maximumCapacity Maximum number of profiles to maintain
   * @return Current size of tree after addition
   */
  private int addToProfileSet(String profileName, int currentSize, int maximumCapacity) {
    //Add if not reached max capacity
    if (currentSize < maximumCapacity) {
      profilesSet.add(profileName.substring(0, profileName.length() - drillSysFileExtSize));
      currentSize++;
    } else {
      boolean addedProfile = profilesSet.add(profileName.substring(0, profileName.length() - drillSysFileExtSize));
      //Remove existing 'oldest' file
      if (addedProfile) {
        profilesSet.pollLast();
      }
    }
    return currentSize;
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
    return contains(key, null);
  }

  @Override
  public boolean contains(String key, DataChangeVersion dataChangeVersion) {
    try (AutoCloseableLock lock = readLock.open()) {
      try {
        Path path = makePath(key);
        boolean exists = fs.exists(path);
        if (exists && dataChangeVersion != null) {
          dataChangeVersion.setVersion(version);
        }
        return exists;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public V get(String key) {
    return get(key, null);
  }

  @Override
  public V get(String key, DataChangeVersion dataChangeVersion) {
    try (AutoCloseableLock lock = readLock.open()) {
      try {
        if (dataChangeVersion != null) {
          dataChangeVersion.setVersion(version);
        }
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
  }

  @Override
  public void put(String key, V value) {
    put(key, value, null);
  }

  @Override
  public void put(String key, V value, DataChangeVersion dataChangeVersion) {
    try (AutoCloseableLock lock = writeLock.open()) {
      if (dataChangeVersion != null && dataChangeVersion.getVersion() != version) {
        throw new VersionMismatchException("Version mismatch detected", dataChangeVersion.getVersion());
      }
      try (OutputStream os = fs.create(makePath(key))) {
        IOUtils.write(config.getSerializer().serialize(value), os);
        version++;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public boolean putIfAbsent(String key, V value) {
    try (AutoCloseableLock lock = writeLock.open()) {
      try {
        Path p = makePath(key);
        if (fs.exists(p)) {
          return false;
        } else {
          try (OutputStream os = fs.create(makePath(key))) {
            IOUtils.write(config.getSerializer().serialize(value), os);
            version++;
          }
          return true;
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void delete(String key) {
    try (AutoCloseableLock lock = writeLock.open()) {
      try {
        fs.delete(makePath(key), false);
        version++;
      } catch (IOException e) {
        logger.error("Unable to delete data from storage.", e);
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void close() {
  }

}
