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
package org.apache.drill.exec.store.hive;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.util.ImpersonationUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Class which provides methods to get metadata of given Hive table selection. It tries to use the stats stored in
 * MetaStore whenever available and delays the costly operation of loading of InputSplits until needed. When
 * loaded, InputSplits are cached to speedup subsequent access.
 */
public class HiveMetadataProvider {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HiveMetadataProvider.class);

  public static final int RECORD_SIZE = 1024;

  private final HiveReadEntry hiveReadEntry;
  private final UserGroupInformation ugi;
  private final boolean isPartitionedTable;
  private final Map<Partition, List<InputSplitWrapper>> partitionInputSplitMap;
  private final HiveConf hiveConf;
  private List<InputSplitWrapper> tableInputSplits;

  private final Stopwatch watch = Stopwatch.createUnstarted();

  public HiveMetadataProvider(final String userName, final HiveReadEntry hiveReadEntry, final HiveConf hiveConf) {
    this.hiveReadEntry = hiveReadEntry;
    this.ugi = ImpersonationUtil.createProxyUgi(userName);
    isPartitionedTable = hiveReadEntry.getTable().getPartitionKeysSize() > 0;
    partitionInputSplitMap = Maps.newHashMap();
    this.hiveConf = hiveConf;
  }

  /**
   * Return stats for table/partitions in given {@link HiveReadEntry}. If valid stats are available in MetaStore,
   * return it. Otherwise estimate using the size of the input data.
   *
   * @param hiveReadEntry Subset of the {@link HiveReadEntry} used when creating this cache object.
   * @return
   * @throws IOException
   */
  public HiveStats getStats(final HiveReadEntry hiveReadEntry) throws IOException {
    final Stopwatch timeGetStats = Stopwatch.createStarted();

    final Table table = hiveReadEntry.getTable();
    try {
      if (!isPartitionedTable) {
        final Properties properties = MetaStoreUtils.getTableMetadata(table);
        final HiveStats stats = getStatsFromProps(properties);
        if (stats.valid()) {
          return stats;
        }

        // estimate the stats from the InputSplits.
        return getStatsEstimateFromInputSplits(getTableInputSplits());
      } else {
        final HiveStats aggStats = new HiveStats(0, 0);
        for(Partition partition : hiveReadEntry.getPartitions()) {
          final Properties properties = HiveUtilities.getPartitionMetadata(partition, table);
          HiveStats stats = getStatsFromProps(properties);

          if (!stats.valid()) {
            // estimate the stats from InputSplits
            stats = getStatsEstimateFromInputSplits(getPartitionInputSplits(partition));
          }
          aggStats.add(stats);
        }

        return aggStats;
      }
    } catch (final Exception e) {
      throw new IOException("Failed to get numRows from HiveTable", e);
    } finally {
      logger.debug("Took {} µs to get stats from {}.{}", timeGetStats.elapsed(TimeUnit.NANOSECONDS) / 1000,
          table.getDbName(), table.getTableName());
    }
  }

  /** Helper method which return InputSplits for non-partitioned table */
  private List<InputSplitWrapper> getTableInputSplits() throws Exception {
    Preconditions.checkState(!isPartitionedTable, "Works only for non-partitioned tables");
    if (tableInputSplits != null) {
      return tableInputSplits;
    }

    final Properties properties = MetaStoreUtils.getTableMetadata(hiveReadEntry.getTable());
    tableInputSplits = splitInputWithUGI(properties, hiveReadEntry.getTable().getSd(), null);

    return tableInputSplits;
  }

  /** Helper method which returns the InputSplits for given partition. InputSplits are cached to speed up subsequent
   * metadata cache requests for the same partition(s).
   */
  private List<InputSplitWrapper> getPartitionInputSplits(final Partition partition) throws Exception {
    if (partitionInputSplitMap.containsKey(partition)) {
      return partitionInputSplitMap.get(partition);
    }

    final Properties properties = HiveUtilities.getPartitionMetadata(partition, hiveReadEntry.getTable());
    final List<InputSplitWrapper> splits = splitInputWithUGI(properties, partition.getSd(), partition);
    partitionInputSplitMap.put(partition, splits);

    return splits;
  }

  /**
   * Return {@link InputSplitWrapper}s for given {@link HiveReadEntry}. First splits are looked up in cache, if not
   * found go through {@link InputFormat#getSplits(JobConf, int)} to find the splits.
   *
   * @param hiveReadEntry Subset of the {@link HiveReadEntry} used when creating this object.
   *
   * @return
   */
  public List<InputSplitWrapper> getInputSplits(final HiveReadEntry hiveReadEntry) {
    final Stopwatch timeGetSplits = Stopwatch.createStarted();
    try {
      if (!isPartitionedTable) {
        return getTableInputSplits();
      }

      final List<InputSplitWrapper> splits = Lists.newArrayList();
      for (Partition p : hiveReadEntry.getPartitions()) {
        splits.addAll(getPartitionInputSplits(p));
      }
      return splits;
    } catch (final Exception e) {
      logger.error("Failed to get InputSplits", e);
      throw new DrillRuntimeException("Failed to get InputSplits", e);
    } finally {
      logger.debug("Took {} µs to get InputSplits from {}.{}", timeGetSplits.elapsed(TimeUnit.NANOSECONDS) / 1000,
          hiveReadEntry.getTable().getDbName(), hiveReadEntry.getTable().getTableName());
    }
  }

  /**
   * Get the list of directories which contain the input files. This list is useful for explain plan purposes.
   *
   * @param hiveReadEntry {@link HiveReadEntry} containing the input table and/or partitions.
   */
  protected List<String> getInputDirectories(final HiveReadEntry hiveReadEntry) {
    if (isPartitionedTable) {
      final List<String> inputs = Lists.newArrayList();
      for(Partition p : hiveReadEntry.getPartitions()) {
        inputs.add(p.getSd().getLocation());
      }
      return inputs;
    }

    return Collections.singletonList(hiveReadEntry.getTable().getSd().getLocation());
  }

  /**
   * Get the stats from table properties. If not found -1 is returned for each stats field.
   * CAUTION: stats may not be up-to-date with the underlying data. It is always good to run the ANALYZE command on
   * Hive table to have up-to-date stats.
   * @param properties
   * @return
   */
  private HiveStats getStatsFromProps(final Properties properties) {
    long numRows = -1;
    long sizeInBytes = -1;
    try {
      final String numRowsProp = properties.getProperty(StatsSetupConst.ROW_COUNT);
      if (numRowsProp != null) {
          numRows = Long.valueOf(numRowsProp);
      }

      final String sizeInBytesProp = properties.getProperty(StatsSetupConst.TOTAL_SIZE);
      if (sizeInBytesProp != null) {
        sizeInBytes = Long.valueOf(numRowsProp);
      }
    } catch (final NumberFormatException e) {
      logger.error("Failed to parse Hive stats in metastore.", e);
      // continue with the defaults.
    }

    return new HiveStats(numRows, sizeInBytes);
  }

  /**
   * Estimate the stats from the given list of InputSplits.
   * @param inputSplits
   * @return
   * @throws IOException
   */
  private HiveStats getStatsEstimateFromInputSplits(final List<InputSplitWrapper> inputSplits) throws IOException {
    long data = 0;
    for (final InputSplitWrapper split : inputSplits) {
      data += split.getSplit().getLength();
    }

    return new HiveStats(data/RECORD_SIZE, data);
  }

  private List<InputSplitWrapper> splitInputWithUGI(final Properties properties, final StorageDescriptor sd,
      final Partition partition) throws Exception {
    watch.start();
    try {
      return ugi.doAs(new PrivilegedExceptionAction<List<InputSplitWrapper>>() {
        public List<InputSplitWrapper> run() throws Exception {
          final List<InputSplitWrapper> splits = Lists.newArrayList();
          final JobConf job = new JobConf(hiveConf);
          HiveUtilities.addConfToJob(job, properties);
          job.setInputFormat(HiveUtilities.getInputFormatClass(job, sd, hiveReadEntry.getTable()));
          final Path path = new Path(sd.getLocation());
          final FileSystem fs = path.getFileSystem(job);

          if (fs.exists(path)) {
            FileInputFormat.addInputPath(job, path);
            final InputFormat<?, ?> format = job.getInputFormat();
            for (final InputSplit split : format.getSplits(job, 1)) {
              splits.add(new InputSplitWrapper(split, partition));
            }
          }

          return splits;
        }
      });
    } catch (final InterruptedException | IOException e) {
      final String errMsg = String.format("Failed to create input splits: %s", e.getMessage());
      logger.error(errMsg, e);
      throw new DrillRuntimeException(errMsg, e);
    } finally {
      logger.trace("Took {} µs to get splits from {}", watch.elapsed(TimeUnit.NANOSECONDS) / 1000, sd.getLocation());
      watch.stop();
    }
  }

  /** Contains InputSplit along with the Partition. If non-partitioned tables, the partition field is null. */
  public static class InputSplitWrapper {
    private InputSplit split;
    private Partition partition;

    public InputSplitWrapper(final InputSplit split, final Partition partition) {
      this.split = split;
      this.partition = partition;
    }

    public InputSplit getSplit() {
      return split;
    }

    public Partition getPartition() {
      return partition;
    }
  }

  /** Contains stats. Currently only numRows and totalSizeInBytes are used. */
  public static class HiveStats {
    private long numRows;
    private long sizeInBytes;

    public HiveStats(final long numRows, final long sizeInBytes) {
      this.numRows = numRows;
      this.sizeInBytes = sizeInBytes;
    }

    public long getNumRows() {
      return numRows;
    }

    public long getSizeInBytes() {
      return sizeInBytes;
    }

    /** Both numRows and sizeInBytes are expected to be greater than 0 for stats to be valid */
    public boolean valid() {
      return numRows > 0 && sizeInBytes > 0;
    }

    public void add(HiveStats s) {
      numRows += s.numRows;
      sizeInBytes += s.sizeInBytes;
    }

    @Override
    public String toString() {
      return "numRows: " + numRows + ", sizeInBytes: " + sizeInBytes;
    }
  }
}
