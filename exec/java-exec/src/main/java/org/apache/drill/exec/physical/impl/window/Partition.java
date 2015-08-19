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
package org.apache.drill.exec.physical.impl.window;

/**
 * Used internally to keep track of partitions and frames
 */
public class Partition {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Partition.class);

  private final long length; // size of this partition
  private long remaining;

  private int peers; // remaining non-processed peers in current frame

  // we keep these attributes public because the generated code needs to access them
  public int row_number;
  public int rank;
  public int dense_rank;
  public double percent_rank;
  public double cume_dist;

  /**
   * @return number of rows not yet aggregated in this partition
   */
  public long getRemaining() {
    return remaining;
  }

  /**
   * @return peer rows not yet aggregated in current frame
   */
  public int getPeers() {
    return peers;
  }

  public Partition(long length) {
    this.length = length;
    remaining = length;
    row_number = 1;
  }

  public void rowAggregated() {
    remaining--;
    peers--;

    row_number++;
  }

  public void newFrame(int peers) {
    this.peers = peers;

    rank = row_number; // rank = row number of 1st peer
    dense_rank++;
    percent_rank = length > 1 ? (double) (rank - 1) / (length - 1) : 0;
    cume_dist = (double)(rank + peers - 1) / length;
  }

  public boolean isDone() {
    return remaining == 0;
  }

  public int ntile(int numTiles) {
    long mod = length % numTiles;
    double ceil = Math.ceil((double) length / numTiles);

    int out;
    if (row_number <= mod * ceil) {
      out = (int) Math.ceil(row_number / ceil);
    } else {
      double floor = Math.floor((double) length / numTiles);
      out = (int) Math.ceil((row_number - mod) / floor);
    }

    logger.trace("NTILE(row_number = {}, nt = {}, ct = {}) = {}", row_number, numTiles, length, out);
    return out;
  }

  public boolean isFrameDone() {
    return peers == 0;
  }

  @Override
  public String toString() {
    return String.format("{length: %d, remaining partition: %d, remaining peers: %d}", length, remaining, peers);
  }
}
