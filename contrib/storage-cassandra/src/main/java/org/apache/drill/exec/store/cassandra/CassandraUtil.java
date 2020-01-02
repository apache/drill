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
package org.apache.drill.exec.store.cassandra;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class CassandraUtil {
  private static final Logger logger = LoggerFactory.getLogger(CassandraUtil.class);

  public static String[] getPartitionTokens(String partitionTechnique, int numberOfNodes) {

    logger.info("Getting partition bounds for nodes. PartitionScheme: {}, Node Count: {}.", partitionTechnique, numberOfNodes);

    switch (partitionTechnique) {
      case "org.apache.cassandra.dht.Murmur3Partitioner":
        return getMurmur3PartitionTokens(numberOfNodes);

      case "org.apache.cassandra.dht.RandomPartitioner":
        return getRandomPartitionTokens(numberOfNodes);

      default:
        logger.error("Cassandra partition scheme {} not supported.", partitionTechnique);
        return null;
    }
  }

  private static String[] getMurmur3PartitionTokens(int numberOfNodes) {

    String[] tokens = new String[numberOfNodes];

    BigDecimal two = new BigDecimal(2);
    BigDecimal nodes = new BigDecimal(numberOfNodes);
    BigDecimal val = two.pow(64).divide(nodes, RoundingMode.DOWN);
    BigDecimal twoPow63 = two.pow(63);
    BigDecimal index;

    for (int i = 0; i < numberOfNodes; i++) {
      index = new BigDecimal(i);
      tokens[i] = index.multiply(val).subtract(twoPow63).toPlainString();
    }
    return tokens;
  }

  private static String[] getRandomPartitionTokens(int numberOfNodes) {

    String[] tokens = new String[numberOfNodes];

    BigDecimal two = new BigDecimal(2);
    BigDecimal nodes = new BigDecimal(numberOfNodes);
    BigDecimal val = two.pow(127).divide(nodes, RoundingMode.DOWN);

    BigDecimal index;

    for (int i = 0; i < numberOfNodes; i++) {
      index = new BigDecimal(i);
      tokens[i] = index.multiply(val).toPlainString();
    }
    return tokens;
  }

  public static void main(String[] args) {

    String[] out = new CassandraUtil().getPartitionTokens("org.apache.cassandra.dht.Murmur3Partitioner", 5);

    for (String s : out) {
      System.out.println(s);
    }
  }


}
