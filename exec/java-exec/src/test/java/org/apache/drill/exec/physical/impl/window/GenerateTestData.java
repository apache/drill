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

import org.apache.drill.common.util.TestTools;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class GenerateTestData {
  private static final int SUB_MUL = 1;
  private static final int BATCH_SIZE = 20;

  private static class Partition {
    Partition previous;
    final int length;
    final int[] subs;

    public Partition(int length, int[] subs) {
      this.length = length;
      this.subs = subs;
    }

    /**
     * @return total number of rows since first partition, this partition included
     */
    public int cumulLength() {
      int prevLength = previous != null ? previous.cumulLength() : 0;
      return length + prevLength;
    }

    public boolean isPartOf(int rowNumber) {
      int prevLength = previous != null ? previous.cumulLength() : 0;
      return rowNumber >= prevLength && rowNumber < cumulLength();
    }

    public int getSubIndex(final int sub) {
      return Arrays.binarySearch(subs, sub);
    }

    public int getSubSize(int sub) {
      if (sub != subs[subs.length-1]) {
        return sub * SUB_MUL;
      } else {
        //last sub has enough rows to reach partition length
        int size = length;
        for (int i = 0; i < subs.length-1; i++) {
          size -= subs[i] * SUB_MUL;
        }
        return size;
      }
    }

    /**
     * @return sub id of the sub that contains rowNumber
     */
    public int getSubId(int rowNumber) {
      assert isPartOf(rowNumber) : "row "+rowNumber+" isn't part of this partition";

      int prevLength = previous != null ? previous.cumulLength() : 0;
      rowNumber -= prevLength; // row num from start of this partition

      for (int s : subs) {
        if (rowNumber < subRunningCount(s)) {
          return s;
        }
      }

      throw new RuntimeException("should never happen!");
    }

    /**
     * @return running count of rows from first row of the partition to current sub, this sub included
     */
    public int subRunningCount(int sub) {
      int count = 0;
      for (int s : subs) {
        count += getSubSize(s);
        if (s == sub) {
          break;
        }
      }
      return count;
    }

    /**
     * @return running sum of salaries from first row of the partition to current sub, this sub included
     */
    public int subRunningSum(int sub) {
      int sum = 0;
      for (int s : subs) {
        sum += (s+10) * getSubSize(s);
        if (s == sub) {
          break;
        }
      }
      return sum;
    }

    /**
     * @return sum of salaries for all rows of the partition
     */
    public int totalSalary() {
      return subRunningSum(subs[subs.length-1]);
    }

  }

  private static Partition[] dataB1P1() {
    // partition rows 20, subs [1, 2, 3, 4, 5, 6]
    return new Partition[] {
      new Partition(20, new int[]{1, 2, 3, 4, 5, 6})
    };
  }

  private static Partition[] dataB1P2() {
    // partition rows 10, subs [1, 2, 3, 4]
    // partition rows 10, subs [4, 5, 6]
    return new Partition[] {
      new Partition(10, new int[]{1, 2, 3, 4}),
      new Partition(10, new int[]{4, 5, 6}),
    };
  }

  private static Partition[] dataB2P2() {
    // partition rows 20, subs [3, 5, 9]
    // partition rows 20, subs [9, 10]
    return new Partition[] {
      new Partition(20, new int[]{3, 5, 9}),
      new Partition(20, new int[]{9, 10}),
    };
  }

  private static Partition[] dataB2P4() {
    // partition rows 5, subs [1, 2, 3]
    // partition rows 10, subs [3, 4, 5]
    // partition rows 15, subs [5, 6, 7]
    // partition rows 10, subs [7, 8]
    return new Partition[] {
      new Partition(5, new int[]{1, 2, 3}),
      new Partition(10, new int[]{3, 4, 5}),
      new Partition(15, new int[]{5, 6, 7}),
      new Partition(10, new int[]{7, 8}),
    };
  }

  private static Partition[] dataB3P2() {
    // partition rows 5, subs [1, 2, 3]
    // partition rows 55, subs [4, 5, 7, 8, 9, 10, 11, 12]
    return new Partition[] {
      new Partition(5, new int[]{1, 2, 3}),
      new Partition(55, new int[]{4, 5, 7, 8, 9, 10, 11, 12}),
    };
  }

  private static Partition[] dataB4P4() {
    // partition rows 10, subs [1, 2, 3]
    // partition rows 30, subs [3, 4, 5, 6, 7, 8]
    // partition rows 20, subs [8, 9, 10]
    // partition rows 20, subs [10, 11]
    return new Partition[] {
      new Partition(10, new int[]{1, 2, 3}),
      new Partition(30, new int[]{3, 4, 5, 6, 7, 8}),
      new Partition(20, new int[]{8, 9, 10}),
      new Partition(20, new int[]{10, 11}),
    };
  }

  private static void generateData(final String tableName, final Partition[] partitions) throws FileNotFoundException {
    final String WORKING_PATH = TestTools.getWorkingPath();
    final String TEST_RES_PATH = WORKING_PATH + "/src/test/resources";
    //TODO command line arguments contain file name
    final String path = TEST_RES_PATH+"/window/" + tableName;

    final File pathFolder = new File(path);
    if (!pathFolder.exists()) {
      if (!pathFolder.mkdirs()) {
        System.err.printf("Couldn't create folder %s, exiting%n", path);
      }
    }

    // expected results for query without order by clause
    final PrintStream resultStream = new PrintStream(path + ".tsv");
    // expected results for query with order by clause
    final PrintStream resultOrderStream = new PrintStream(path + ".subs.tsv");

    // data file(s)
    int fileId = 0;
    PrintStream dataStream = new PrintStream(path + "/" + fileId + ".data.json");

    for (Partition p : partitions) {
      dataStream.printf("// partition rows %d, subs %s%n", p.length, Arrays.toString(p.subs));
    }

    // set previous partitions
    for (int i = 1; i < partitions.length; i++) {
      partitions[i].previous = partitions[i - 1];
    }

    // total number of rows
    int total = partitions[partitions.length - 1].cumulLength();

    // create data rows in randome order
    List<Integer> emp_ids = new ArrayList<>(total);
    for (int i = 0; i < total; i++) {
      emp_ids.add(i);
    }
    Collections.shuffle(emp_ids);

    int emp_idx = 0;
    for (int id : emp_ids) {
      int p = 0;
      while (!partitions[p].isPartOf(id)) { // emp x is @ row x-1
        p++;
      }

      int sub = partitions[p].getSubId(id);
      int salary = 10 + sub;

      dataStream.printf("{ \"employee_id\":%d, \"position_id\":%d, \"sub\":%d, \"salary\":%d }%n", id, p + 1, sub, salary);
      emp_idx++;
      if ((emp_idx % BATCH_SIZE)==0 && emp_idx < total) {
        System.out.printf("total: %d, emp_idx: %d, fileID: %d%n", total, emp_idx, fileId);
        dataStream.close();
        fileId++;
        dataStream = new PrintStream(path + "/" + fileId + ".data.json");
      }
    }

    dataStream.close();

    for (int p = 0, idx = 0; p < partitions.length; p++) {
      for (int i = 0; i < partitions[p].length; i++, idx++) {
        final Partition partition = partitions[p]; //TODO change for p loop to for over partitions

        final int sub = partition.getSubId(idx);
        final int rowNumber = i + 1;
        final int rank = 1 + partition.subRunningCount(sub) - partition.getSubSize(sub);
        final int denseRank = partition.getSubIndex(sub) + 1;
        final double cumeDist = (double) partition.subRunningCount(sub) / partition.length;
        final double percentRank = partition.length == 1 ? 0 : (double)(rank - 1)/(partition.length - 1);

        // each line has: count(*)  sum(salary)  row_number()  rank()  dense_rank()  cume_dist()  percent_rank()
        resultOrderStream.printf("%d\t%d\t%d\t%d\t%d\t%s\t%s%n",
          partition.subRunningCount(sub), partition.subRunningSum(sub),
          rowNumber, rank, denseRank, Double.toString(cumeDist), Double.toString(percentRank));

        // each line has: count(*)  sum(salary)
        resultStream.printf("%d\t%d%n", partition.length, partition.totalSalary());
      }
    }

    resultStream.close();
    resultOrderStream.close();
  }

  public static void main(String[] args) throws FileNotFoundException {
    generateData("b1.p1", dataB1P1());
    generateData("b1.p2", dataB1P2());
    generateData("b2.p2", dataB2P2());
    generateData("b2.p4", dataB2P4());
    generateData("b3.p2", dataB3P2());
    generateData("b4.p4", dataB4P4());
  }

}