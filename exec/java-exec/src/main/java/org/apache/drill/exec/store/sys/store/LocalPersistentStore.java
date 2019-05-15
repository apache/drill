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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.Map.Entry;

import javax.annotation.Nullable;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.drill.common.collections.ImmutableEntry;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.QueryProfile;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.util.DrillFileSystemUtil;
import org.apache.drill.exec.store.sys.BasePersistentStore;
import org.apache.drill.exec.store.sys.PersistentStoreConfig;
import org.apache.drill.exec.store.sys.PersistentStoreMode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.drill.shaded.guava.com.google.common.base.Function;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.cache.CacheBuilder;
import org.apache.drill.shaded.guava.com.google.common.cache.CacheLoader;
import org.apache.drill.shaded.guava.com.google.common.cache.LoadingCache;
import org.apache.drill.shaded.guava.com.google.common.collect.Iterables;
import org.apache.hadoop.fs.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalPersistentStore<V> extends BasePersistentStore<V> {
  private static final Logger logger = LoggerFactory.getLogger(LocalPersistentStore.class);

  //Provides a threshold above which we report an event's time
  //TODO Configurable threshold?
  private static final long RESPONSE_TIME_THRESHOLD_MSEC = /*200*/0L;
  private static final String DIORAMA = "diorama";

  private static final int DRILL_SYS_FILE_EXT_SIZE = DRILL_SYS_FILE_SUFFIX.length();

  private final Path basePath;
  private final PersistentStoreConfig<V> config;
  private final DrillFileSystem fs;

  private final SimpleDateFormat indexedPathFormat;
  private final String indexPathPattern;
  private final IncrementType incrementType;

  private final PathFilter sysFileSuffixFilter;
  private final Comparator<String> profilePathComparator;
  private final Function<String, Entry<String, V>> stringTransformer;
  //TODO Do we need this?
  private  Function<FileStatus, Entry<String, V>> fileStatusTransformer;

  private final int deserializedCacheCapacity;
  private final CacheLoader<String, V> cacheLoader;
  private final LoadingCache<String, V> deserializedVCache;

  public LocalPersistentStore(DrillFileSystem fs, Path base, PersistentStoreConfig<V> config, DrillConfig drillConfig) {
    this.basePath = new Path(base, config.getName());
    this.config = config;
    this.fs = fs;
    try {
      mkdirs(getBasePath());
    } catch (IOException e) {
      throw new RuntimeException("Failure setting pstore configuration path.");
    }

    //TODO: int cacheCapacity = drillConfig.getInt(ExecConstants.HTTP_MAX_PROFILES [or] PROFILES_STORE_CACHE_SIZE
    deserializedCacheCapacity = drillConfig.getInt(ExecConstants.PROFILES_STORE_CACHE_SIZE);

    indexPathPattern = drillConfig.getString(ExecConstants.PROFILES_STORE_INDEX_FORMAT);
    incrementType = indexPathPattern.contains("m") ? IncrementType.Minute :
      indexPathPattern.contains("H") ? IncrementType.Hour :
        indexPathPattern.contains("d") ? IncrementType.Day :
          indexPathPattern.contains("M") ? IncrementType.Month :
            indexPathPattern.contains("y") ? IncrementType.Year : null;
    indexedPathFormat = new SimpleDateFormat(indexPathPattern);

    this.sysFileSuffixFilter = new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return path.getName().endsWith(DRILL_SYS_FILE_SUFFIX);
      }
    };

    this.profilePathComparator = new Comparator<String>() {
      @Override
      public int compare(String path1, String path2) {
        return path1.substring(path1.lastIndexOf('/')+1).compareTo(path2.substring(path2.lastIndexOf('/')+1));
      }
    };

    // Transformer function to extract profile based on query ID String
    this.stringTransformer = new Function<String, Entry<String, V>>() {
      @Nullable
      @Override
      public Entry<String, V> apply(String key) {
        return new ImmutableEntry<>(key, getViaAbsolutePath(key));
      }
    };

    /*// Transformer function to extract profile based on FileStatus
    this.fileStatusTransformer = new Function<FileStatus, Entry<String, V>>() {
      @Nullable
      @Override
      public Entry<String, V> apply(FileStatus fStatus) {
        Path fPath = fStatus.getPath();
        String sanSuffixName = fPath.getName().substring(0, fPath.getName().length() - DRILL_SYS_FILE_EXT_SIZE);
        return new ImmutableEntry<>(sanSuffixName, get(fStatus));
      }
    };*/

    //Defining Cache loader for handling missing entries
    this.cacheLoader = new CacheLoader<String, V>() {
      @Override
      public V load(String srcPathAsStr) {
        //Cache miss to force loading from FS
        //logger.info("cacheMiss::fetchFromFS:: {}", srcPathAsStr);
        return deserializeFromFileSystem(srcPathAsStr);
      }
    };

    //Creating the cache
    this.deserializedVCache = CacheBuilder.newBuilder()
        .initialCapacity(Math.max(deserializedCacheCapacity/5, 20)) //startingCapacity: 20% or 20
        .maximumSize(deserializedCacheCapacity)
        .recordStats() //TODO Should we get rid of this since we arent using it?
        .build(cacheLoader);


  }

  public Path getBasePath() {
    return basePath;
  }

  @Override
  public PersistentStoreMode getMode() {
    return PersistentStoreMode.PERSISTENT;
  }

  private void mkdirs(Path path) throws IOException {
    fs.mkdirs(path);
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
    public Iterator<Map.Entry<String, V>> getRange/*Neo*/(int skip, int take) {
      try {
        List<String> files = new LinkedList<>();
        // Sort and explore Directory stack using DepthFirstSearch
        LinkedList<FileStatus> profileDirStack = new LinkedList<FileStatus>(DrillFileSystemUtil.listDirectoriesSafe(fs, basePath, false));
        profileDirStack.sort(Comparator.naturalOrder());
        logger.info("dirSize:: {} ", profileDirStack.size());

        int collectedProfileCount = 0;
        while (!profileDirStack.isEmpty()) {
          // Explore dir from top of stack
          FileStatus latestDir = profileDirStack.removeLast();

          // Read all profiles in last dir
          List<FileStatus> profileStatus = DrillFileSystemUtil.listFiles(fs, latestDir.getPath(), false, sysFileSuffixFilter);
          if (!profileStatus.isEmpty()) {
            List<String> additions = new LinkedList<>();
            for (FileStatus stat : profileStatus) {
              String filePathStr = stat.getPath().toUri().getPath();
              additions.add(filePathStr.substring(0, filePathStr.length() - DRILL_SYS_FILE_EXT_SIZE));
            }
            //Sort additions & append (saves time in resorting entire list)
            additions.sort(profilePathComparator);
            files.addAll(additions);

            //[sodBug]
            if (!files.isEmpty()) {
              logger.info("First is {}", files.get(0));
            }

            int _pCount = profileStatus.size();
            if (_pCount > 0) {
              collectedProfileCount += _pCount;
              logger.info("# profiles added = {} [Total: {} (act) / {} (est)] ", _pCount, files.size(), collectedProfileCount);
            }
            //[eodBug]
          }

          // Explore subdirectories
          List<FileStatus> childSubdirStack = DrillFileSystemUtil.listDirectoriesSafe(fs, latestDir.getPath(), false);
          // Sorting list before addition to stack
          childSubdirStack.sort(Comparator.naturalOrder());
          if (!childSubdirStack.isEmpty()) {
            profileDirStack.addAll(childSubdirStack);
          } else {
            logger.info("foundLeaf:: {}", latestDir.getPath().toUri());
          }

          // Terminate exploration if required count has been met
          if ( collectedProfileCount >= (skip + take) ) {
            profileDirStack.clear();
          }
        }

        //Sorting not required since preSorted //dBug
        logger.info("Post Scan First is {}", files.get(0));

        Iterator<Entry<String, V>> rangeIterator = Iterables.transform(Iterables.limit(Iterables.skip(files, skip), take), this.stringTransformer).iterator();
        logger.info("CacheSTATS::{}:: {}", (take+skip), this.deserializedVCache.stats().toString());
        return rangeIterator;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

  private Path makePath(String name) {
    Preconditions.checkArgument(
        //!name.contains("/") &&
        !name.contains(":") &&
        !name.contains(".."));
//    return new Path(basePath, name + DRILL_SYS_FILE_SUFFIX);
    return new Path(name + DRILL_SYS_FILE_SUFFIX);
  }

  // Using timestamp to infer correct pigeon-hole for writing destination
  private Path makeIndexedPath(String name, long timestamp) {
    Preconditions.checkArgument(
        !name.contains("/") &&
        !name.contains(":") &&
        !name.contains(".."));
    Path indexedPath = new Path(basePath, indexedPathFormat.format(timestamp));
    return new Path(indexedPath, name + DRILL_SYS_FILE_SUFFIX);
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
    Path actualPath = makePath(key);
    try {
      //logger.info("key2make::{}", key);
      if (!fs.exists(actualPath)) {
        //Generate paths within upper and lower bounds to test
        List<String> possibleDirs = getPossiblePaths(key.substring(key.lastIndexOf('/') + 1));
        possibleDirs.add(DIORAMA);
        actualPath = getPathFromPossibleDirList(key, possibleDirs);
        if (actualPath == null) {
          return null;
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
//    logger.info("DeSerializing {}", actualPath.toUri().getPath());
    return deserializedVCache.getUnchecked(actualPath.toString());
  }

  @Override
  public void put(String key, V value) {
    Path writePath = null;
    if (value instanceof QueryProfile) {
      QueryProfile profile = (QueryProfile) value;
      writePath = makeIndexedPath(key, profile.getStart());
    } else {
      writePath = makePath(key);
    }
    try (OutputStream os = fs.create(writePath)) {
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

  // Gets deserialized by exact path (Used for listing)
  private V getViaAbsolutePath(String key) {
    try {
//      logger.info("key2make::{}", key);
      Path path = makePath(key);
      if (!fs.exists(path)) {
//        logger.info("gotNullPath for {} ", key);
        return null;
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    final Path path = makePath(key);
    return deserializedVCache.getUnchecked(path.toString());
  }

  // Returns path if profile is found within list of possible direct (Used for blind lookup of key)
  private Path getPathFromPossibleDirList(String key, List<String> possibleDirList) {
    for (String possibleDir : possibleDirList) {
      Path testPath = new Path(basePath + "/" + possibleDir, key + DRILL_SYS_FILE_SUFFIX);
      try {
        if (fs.exists(testPath)) {
          return testPath;
        }
      } catch (IOException e) {
        /*DoNothing*/
      }
    }
    return null;
  }

  // Infers the list of possible directories where the profile is located (Used for blind lookup of key)
    private List<String> getPossiblePaths(String queryIdString) {
      //Reqd::
      QueryId queryId = QueryIdHelper.getQueryIdFromString(queryIdString);
      long lowerBoundTime = (Integer.MAX_VALUE - ((queryId.getPart1() + Integer.MAX_VALUE) >> 32)) * 1000; // +/- 1000 for border cases
      long upperBoundTime = (Integer.MAX_VALUE - ((queryId.getPart1() + Integer.MIN_VALUE) >> 32)) * 1000; // +/- 1000 for border cases
      //[sodBug]
      Date lowerBoundDate = new Date(lowerBoundTime);
      String lowerBoundPath = indexedPathFormat.format(lowerBoundDate);
      logger.info("Inferred LowerBound Time is {} . Look from {}", lowerBoundDate, lowerBoundPath);
      Date upperBoundDate = new Date(upperBoundTime);
      logger.info("Inferred UpperBound Time is {} . Look until {}", upperBoundDate, indexedPathFormat.format(upperBoundDate));
      //[eodBug]

      if (incrementType == null) {
        return new ArrayList<>(0); //Empty
      }

      Date currDate = lowerBoundDate;
      logger.info("currDate.after(upperBoundDate) : {}", currDate.after(upperBoundDate));
      int increment = 0;

      Set<String> possibleSrcDirSet = new TreeSet<>();
      do {
        //Add tokenized parents as well
        String[] possibleDirTokens = indexedPathFormat.format(currDate).split("/");
        String possibleDir = "";
        for (String token : possibleDirTokens) {
          if (possibleDir.isEmpty()) {
            possibleDir = token;
          } else {
            possibleDir = possibleDir.concat("/").concat(token);
          }
          // Adding
          possibleSrcDirSet.add(possibleDir);
          //logger.info("Added {}", possibleDir);
        }

        // Incrementing
        switch (incrementType) {
        case Minute:
          currDate = DateUtils.addMinutes(lowerBoundDate, ++increment);
          break;

        case Hour:
          currDate = DateUtils.addHours(lowerBoundDate, ++increment);
          break;

        case Day:
          currDate = DateUtils.addDays(lowerBoundDate, ++increment);
          break;

        case Month:
          currDate = DateUtils.addMonths(lowerBoundDate, ++increment);
          break;

        case Year:
          currDate = DateUtils.addYears(lowerBoundDate, ++increment);
          break;

        default:
          break;
        }
      } while (!currDate.after(upperBoundDate));

      List<String> sortedPossibleDirs = new ArrayList<String>();
      sortedPossibleDirs.addAll(possibleSrcDirSet);
      sortedPossibleDirs.sort(Comparator.reverseOrder());

      //TODO [sodBug]
      for (String possibility : sortedPossibleDirs) {
        logger.info("Possibility :: {}", possibility);
      }
      //[eodBug]

      return sortedPossibleDirs;
      /*
      // create a new queryid where the first four bytes are a growing time (each new value comes earlier in sequence).  Last 12 bytes are random.
      final long time = (int) (System.currentTimeMillis()/1000);
      final long p1 = ((Integer.MAX_VALUE - time) << 32) + r.nextInt();
       */
    }

  //TODO | FIXME: Guava to Handle RuntimeException by
  //Deserialize path's contents (leveraged by Guava Cache)
  private V deserializeFromFileSystem(String srcPath) {
    final Path path = new Path(srcPath);
    try (InputStream is = fs.open(path)) {
      return config.getSerializer().deserialize(IOUtils.toByteArray(is));
    } catch (IOException e) {
      throw new RuntimeException("Unable to deserialize \"" + path + "\"\n" + e.getMessage(), e);
    }
  }

  //Enumerator
  private enum IncrementType {
    Minute, Hour, Day, Month, Year
  }
}
