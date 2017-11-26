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
package org.apache.drill.exec.store.hive;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import com.google.common.collect.TreeMultimap;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import org.apache.commons.codec.binary.Base64;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.util.ImpersonationUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
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
  private final Map<Partition, List<LogicalInputSplit>> partitionInputSplitMap;
  private final HiveConf hiveConf;
  private List<LogicalInputSplit> tableInputSplits;

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

    final HiveTableWithColumnCache table = hiveReadEntry.getTable();
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
        for(HivePartition partition : hiveReadEntry.getPartitions()) {
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
  private List<LogicalInputSplit> getTableInputSplits() throws Exception {
    Preconditions.checkState(!isPartitionedTable, "Works only for non-partitioned tables");
    if (tableInputSplits != null) {
      return tableInputSplits;
    }

    final Properties properties = HiveUtilities.getTableMetadata(hiveReadEntry.getTable());
    tableInputSplits = splitInputWithUGI(properties, hiveReadEntry.getTable().getSd(), null);

    return tableInputSplits;
  }

  /** Helper method which returns the InputSplits for given partition. InputSplits are cached to speed up subsequent
   * metadata cache requests for the same partition(s).
   */
  private List<LogicalInputSplit> getPartitionInputSplits(final HivePartition partition) throws Exception {
    if (partitionInputSplitMap.containsKey(partition)) {
      return partitionInputSplitMap.get(partition);
    }

    final Properties properties = HiveUtilities.getPartitionMetadata(partition, hiveReadEntry.getTable());
    final List<LogicalInputSplit> splits = splitInputWithUGI(properties, partition.getSd(), partition);
    partitionInputSplitMap.put(partition, splits);

    return splits;
  }

  /**
   * Return {@link LogicalInputSplit}s for given {@link HiveReadEntry}. First splits are looked up in cache, if not
   * found go through {@link InputFormat#getSplits(JobConf, int)} to find the splits.
   *
   * @param hiveReadEntry Subset of the {@link HiveReadEntry} used when creating this object.
   * @return list of logically grouped input splits
   */
  public List<LogicalInputSplit> getInputSplits(final HiveReadEntry hiveReadEntry) {
    final Stopwatch timeGetSplits = Stopwatch.createStarted();
    try {
      if (!isPartitionedTable) {
        return getTableInputSplits();
      }

      final List<LogicalInputSplit> splits = Lists.newArrayList();
      for (HivePartition p : hiveReadEntry.getPartitions()) {
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
   *
   * @param properties the source of table stats
   * @return {@link HiveStats} instance with rows number and size in bytes from specified properties
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
        sizeInBytes = Long.valueOf(sizeInBytesProp);
      }
    } catch (final NumberFormatException e) {
      logger.error("Failed to parse Hive stats in metastore.", e);
      // continue with the defaults.
    }

    return new HiveStats(numRows, sizeInBytes);
  }

  /**
   * Estimate the stats from the given list of logically grouped input splits.
   *
   * @param inputSplits list of logically grouped input splits
   * @return hive stats usually numRows and totalSizeInBytes which used
   */
  private HiveStats getStatsEstimateFromInputSplits(final List<LogicalInputSplit> inputSplits) throws IOException {
    long data = 0;
    for (final LogicalInputSplit split : inputSplits) {
      data += split.getLength();
    }

    return new HiveStats(data/RECORD_SIZE, data);
  }

  /**
   * Gets list of input splits based on table location.
   * These input splits are grouped logically by file name
   * if skip header / footer logic should be applied later on.
   *
   * @param properties table or partition properties
   * @param sd storage descriptor
   * @param partition hive partition
   * @return list of logically grouped input splits
   */
  private List<LogicalInputSplit> splitInputWithUGI(final Properties properties, final StorageDescriptor sd, final Partition partition) {
    watch.start();
    try {
      return ugi.doAs(new PrivilegedExceptionAction<List<LogicalInputSplit>>() {
        public List<LogicalInputSplit> run() throws Exception {
          final List<LogicalInputSplit> splits = Lists.newArrayList();
          final JobConf job = new JobConf(hiveConf);
          HiveUtilities.addConfToJob(job, properties);
          job.setInputFormat(HiveUtilities.getInputFormatClass(job, sd, hiveReadEntry.getTable()));
          final Path path = new Path(sd.getLocation());
          final FileSystem fs = path.getFileSystem(job);
          if (fs.exists(path)) {
            FileInputFormat.addInputPath(job, path);
            final InputFormat<?, ?> format = job.getInputFormat();
            InputSplit[] inputSplits = format.getSplits(job, 1);

            // if current table with text input format and has header / footer,
            // we need to make sure that splits of the same file are grouped together
            if (TextInputFormat.class.getCanonicalName().equals(sd.getInputFormat()) &&
                HiveUtilities.hasHeaderOrFooter(hiveReadEntry.getTable())) {
              Multimap<Path, FileSplit> inputSplitMultimap = transformFileSplits(inputSplits);
              for (Collection<FileSplit> logicalInputSplit : inputSplitMultimap.asMap().values()) {
                splits.add(new LogicalInputSplit(logicalInputSplit, partition));
              }
            } else {
              for (final InputSplit split : inputSplits) {
                splits.add(new LogicalInputSplit(split, partition));
              }
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

  /**
   * <p>
   * Groups input splits by file path. Each inout split group is ordered by starting bytes
   * to ensure file parts in correct order.
   * </p>
   * <p>
   * Example:
   * <pre>
   * hdfs:///tmp/table/file_1.txt  -> hdfs:///tmp/table/file_1.txt:0+10000
   *                                  hdfs:///tmp/table/file_1.txt:10001+20000
   * hdfs:///tmp/table/file_2.txt  -> hdfs:///tmp/table/file_2.txt:0+10000
   * </pre>
   * </p>
   * @param inputSplits input splits
   * @return multimap where key is file path and value is group of ordered file splits
   */
  private Multimap<Path, FileSplit> transformFileSplits(InputSplit[] inputSplits) {
    Multimap<Path, FileSplit> inputSplitGroups = TreeMultimap.create(Ordering.<Path>natural(),
        new Comparator<FileSplit>() {
      @Override
      public int compare(FileSplit f1, FileSplit f2) {
        return Long.compare(f1.getStart(), f2.getStart());
      }
    });

    for (InputSplit inputSplit : inputSplits) {
      FileSplit fileSplit = (FileSplit) inputSplit;
      inputSplitGroups.put(fileSplit.getPath(), fileSplit);
    }
    return inputSplitGroups;
  }

  /**
   * Contains group of input splits along with the partition. For non-partitioned tables, the partition field is null.
   * Input splits can be logically grouped together, for example, in case of when table has header / footer.
   * In this case all splits from the same file should be processed together.
   */
  public static class LogicalInputSplit {

    private final Collection<InputSplit> inputSplits = new ArrayList<>();
    private final Partition partition;

    public LogicalInputSplit(InputSplit inputSplit, Partition partition) {
      inputSplits.add(inputSplit);
      this.partition = partition;
    }

    public LogicalInputSplit(Collection<? extends InputSplit> inputSplits, Partition partition) {
      this.inputSplits.addAll(inputSplits);
      this.partition = partition;
    }

    public Collection<InputSplit> getInputSplits() {
      return inputSplits;
    }

    public Partition getPartition() {
      return partition;
    }

    /**
     * @return returns total length of all stored input splits
     */
    public long getLength() throws IOException {
      long length = 0L;
      for (InputSplit inputSplit: inputSplits) {
        length += inputSplit.getLength();
      }
      return length;
    }

    /**
     * @return collection of unique locations where inout splits are stored
     */
    public Collection<String> getLocations() throws IOException {
      Set<String> locations = new HashSet<>();
      for (InputSplit inputSplit: inputSplits) {
        Collections.addAll(locations, inputSplit.getLocations());
      }
      return locations;
    }

    /**
     * Serializes each input split to string using Base64 encoding.
     *
     * @return list of serialized input splits
     */
    public List<String> serialize() throws IOException {
      List<String> serializedInputSplits = new ArrayList<>();
      for (InputSplit inputSplit : inputSplits) {
        final ByteArrayDataOutput byteArrayOutputStream = ByteStreams.newDataOutput();
        inputSplit.write(byteArrayOutputStream);
        final String encoded = Base64.encodeBase64String(byteArrayOutputStream.toByteArray());
        logger.debug("Encoded split string for split {} : {}", inputSplit, encoded);
        serializedInputSplits.add(encoded);
      }
      return serializedInputSplits;
    }

    /**
     * @return returns input split class name if at least one input split is present, null otherwise.
     */
    public String getType() {
      if (inputSplits.isEmpty()) {
        return null;
      }
      return inputSplits.iterator().next().getClass().getName();
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
