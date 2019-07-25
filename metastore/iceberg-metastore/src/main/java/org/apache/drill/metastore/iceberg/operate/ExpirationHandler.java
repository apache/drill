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
package org.apache.drill.metastore.iceberg.operate;

import com.typesafe.config.ConfigValue;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.metastore.iceberg.config.IcebergConfigConstants;
import org.apache.drill.metastore.iceberg.exceptions.IcebergMetastoreException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Iceberg table generates metadata for each modification operation:
 * snapshot, manifest file, table metadata file. Also when performing delete operation,
 * previously stored data files are not deleted. These files with the time
 * can occupy lots of space.
 * <p/>
 * Expiration handler expires outdated metadata and data files after configured expiration period.
 * Expiration period is set in the Iceberg Metastore config {@link IcebergConfigConstants#EXPIRATION_PERIOD}.
 * Units should correspond to {@link ChronoUnit} values that do not have estimated duration
 * (millis, seconds, minutes, hours, days).
 * If expiration period is not set, zero or negative, expiration process will not be executed.
 * <p/>
 * Expiration process is launched using executor service which allows to execute only one thread at a time,
 * idle thread is not kept in the core pool since it is assumed that expiration process won't be launched to often.
 * <p/>
 * During Drillbit shutdown, if there are expiration tasks in the queue, they will be discarded in order to
 * unblock Drillbit shutdown process.
 */
public class ExpirationHandler implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(ExpirationHandler.class);

  private static final Pattern METADATA_VERSION_PATTERN = Pattern.compile("^v([0-9]+)\\..*");

  // contains Iceberg table location and its last expiration time
  private final Map<String, Long> expirationStatus = new ConcurrentHashMap<>();
  private final Configuration configuration;
  private final long expirationPeriod;
  private volatile ExecutorService executorService;

  public ExpirationHandler(DrillConfig config, Configuration configuration) {
    this.configuration = configuration;
    this.expirationPeriod = expirationPeriod(config);
    logger.debug("Drill Iceberg Metastore expiration period: {}", expirationPeriod);
  }

  /**
   * Checks if expiration process needs to be performed for the given Iceberg table
   * by comparing stored last expiration time.
   * If difference between last expiration time and current time is more or equal to
   * expiration period, launches expiration process.
   * If expiration period is zero or negative, no expiration process will be launched.
   *
   * @param table Iceberg table instance
   * @return true if expiration process was launched, false otherwise
   */
  public boolean expire(Table table) {
    if (expirationPeriod <= 0) {
      return false;
    }

    long current = System.currentTimeMillis();
    Long last = expirationStatus.putIfAbsent(table.location(), current);

    if (last != null && current - last >= expirationPeriod) {
      expirationStatus.put(table.location(), current);

      ExecutorService executorService = executorService();
      executorService.submit(() -> {
        logger.debug("Expiring Iceberg table [{}] metadata", table.location());
        table.expireSnapshots()
          .expireOlderThan(current)
          .commit();
        // TODO: Replace with table metadata expiration through Iceberg API
        //       when https://github.com/apache/incubator-iceberg/issues/181 is resolved
        //       table.expireTableMetadata().expireOlderThan(current).commit();
        expireTableMetadata(table);
      });
      return true;
    }
    return false;
  }

  public long expirationPeriod() {
    return expirationPeriod;
  }

  @Override
  public void close() {
    if (executorService != null) {
      // unlike shutdown(), shutdownNow() discards all queued waiting tasks
      // this is done in order to unblock Drillbit shutdown
      executorService.shutdownNow();
    }
  }

  private long expirationPeriod(DrillConfig config) {
    if (config.hasPath(IcebergConfigConstants.EXPIRATION_PERIOD)) {
      Duration duration = config.getConfig(IcebergConfigConstants.EXPIRATION_PERIOD).entrySet().stream()
        .map(this::duration)
        .reduce(Duration.ZERO, Duration::plus);
      return duration.toMillis();
    }
    return 0;
  }

  private Duration duration(Map.Entry<String, ConfigValue> entry) {
    String amountText = String.valueOf(entry.getValue().unwrapped());
    String unitText = entry.getKey().toUpperCase();
    try {
      long amount = Long.parseLong(amountText);
      ChronoUnit unit = ChronoUnit.valueOf(unitText);
      return Duration.of(amount, unit);
    } catch (NumberFormatException e) {
      throw new IcebergMetastoreException(String.format("Error when parsing expiration period config. " +
        "Unable to convert [%s] into long", amountText), e);
    } catch (IllegalArgumentException e) {
      throw new IcebergMetastoreException(String.format("Error when parsing expiration period config. " +
        "Unable to convert [%s] into [%s]", unitText, ChronoUnit.class.getCanonicalName()), e);
    }
  }

  /**
   * Initializes executor service instance using DCL.
   * Created thread executor instance allows to execute only one thread at a time
   * but unlike single thread executor does not keep this thread in the pool.
   * Custom thread factory is used to define Iceberg Metastore specific thread names.
   *
   * @return executor service instance
   */
  private ExecutorService executorService() {
    if (executorService == null) {
      synchronized (this) {
        if (executorService == null) {
          this.executorService = new ThreadPoolExecutor(0, 1, 0L,
            TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), new IcebergThreadFactory());
        }
      }
    }
    return executorService;
  }

  /**
   * Expires outdated Iceberg table metadata files.
   * Reads current Iceberg table metadata version from version-hint.text file
   * and deletes all metadata files that end with ".metadata.json" and have
   * version less than current.
   * <p/>
   * Should be replaced with
   * <code>table.expireTableMetadata().expireOlderThan(current).commit();</code>
   * when <a href="https://github.com/apache/incubator-iceberg/issues/181">Issue#181</a>
   * is resolved.
   *
   * @param table Iceberg table instance
   */
  private void expireTableMetadata(Table table) {
    try {
      String location = table.location();
      Path metadata = new Path(location, "metadata");
      FileSystem fs = metadata.getFileSystem(configuration);
      for (FileStatus fileStatus : listExpiredMetadataFiles(fs, metadata)) {
        if (fs.delete(fileStatus.getPath(), false)) {
          logger.debug("Deleted Iceberg table [{}] metadata file [{}]", table.location(), fileStatus.getPath());
        }
      }
    } catch (NumberFormatException | IOException e) {
      logger.warn("Unable to expire Iceberg table [{}] metadata files", table.location(), e);
    }
  }

  /**
   * Reads current Iceberg table metadata version from version-hint.text file
   * and returns all metadata files that end with ".metadata.json" and have
   * version less than current.
   *
   * @param fs file system
   * @param metadata pth to Iceberg metadata
   * @return metadata files with version less than current
   * @throws IOException in case of error listing file statuses
   */
  private FileStatus[] listExpiredMetadataFiles(FileSystem fs, Path metadata) throws IOException {
    int currentVersion = currentVersion(fs, metadata);
    return fs.listStatus(metadata, path -> {
      if (path.getName().endsWith(".metadata.json")) {
        int version = parseVersion(path);
        return version != -1 && currentVersion > version;
      }
      return false;
    });
  }

  /**
   * Reads current table metadata version from version-hint.text file.
   *
   * @param fs file system
   * @param metadata table metadata path
   * @return current table metadata version
   * @throws IOException if unable to read current table metadata version
   */
  private int currentVersion(FileSystem fs, Path metadata) throws IOException {
    Path versionHintFile = new Path(metadata, "version-hint.text");
    try (BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(versionHintFile)))) {
      return Integer.parseInt(in.readLine().replace("\n", ""));
    }
  }

  /**
   * Extracts metadata version from table metadata file name.
   * Example: v1.metadata.json -> 1, v15.metadata.json -> 15
   *
   * @param path table metadata file path
   * @return metadata version
   */
  private int parseVersion(Path path) {
    Matcher matcher = METADATA_VERSION_PATTERN.matcher(path.getName());
    if (matcher.find() && matcher.groupCount() == 1) {
      return Integer.parseInt(matcher.group(1));
    }
    throw new NumberFormatException("Unable to parse version for path " + path);
  }

  /**
   * Wraps default thread factory and adds Iceberg Metastore prefix to the original thread name.
   * Is used to uniquely identify Iceberg metastore threads.
   * Example: drill-iceberg-metastore-pool-1-thread-1
   */
  private static class IcebergThreadFactory implements ThreadFactory {

    private static final String THREAD_PREFIX = "drill-iceberg-metastore-";
    private final ThreadFactory delegate = Executors.defaultThreadFactory();

    @Override
    public Thread newThread(Runnable runnable) {
      Thread thread = delegate.newThread(runnable);
      thread.setName(THREAD_PREFIX + thread.getName());
      return thread;
    }
  }
}
