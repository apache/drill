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

import com.beust.jcommander.internal.Lists;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Iterators;
import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteStreams;
import org.apache.commons.codec.binary.Base64;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.exec.physical.OperatorCost;
import org.apache.drill.exec.physical.base.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class HiveSubScan extends AbstractBase implements SubScan {

  @JsonProperty("splits")
  public List<String> encodedSplits;
  @JsonProperty("hive-table")
  public Table table;
  @JsonProperty("partitions")
  public List<Partition> partitions;
  @JsonIgnore
  private List<InputSplit> inputSplits = Lists.newArrayList();
  @JsonProperty("splitClass")
  public List<String> splitClasses;

  @JsonProperty("columns")
  public List<FieldReference> columns;

  @JsonCreator
  public HiveSubScan(@JsonProperty("hive-table") Table table,
                     @JsonProperty("partition") List<Partition> partitions,
                     @JsonProperty("splits") List<String> encodedSplits,
                     @JsonProperty("splitClasses") List<String> splitClasses,
                     @JsonProperty("columns") List<FieldReference> columns) throws IOException, ReflectiveOperationException {
    this.table = table;
    this.partitions = partitions;
    this.encodedSplits = encodedSplits;
    this.splitClasses = splitClasses;
    this.columns = columns;

    for (int i = 0; i < encodedSplits.size(); i++) {
      inputSplits.add(deserializeInputSplit(encodedSplits.get(i), splitClasses.get(i)));
    }
  }

  public static InputSplit deserializeInputSplit(String base64, String className) throws IOException, ReflectiveOperationException{
    InputSplit split;
    if (Class.forName(className) == FileSplit.class) {
      split = new FileSplit((Path) null, 0, 0, (String[])null);
    } else {
      split = (InputSplit) Class.forName(className).getConstructor().newInstance();
    }
    ByteArrayDataInput byteArrayDataInput = ByteStreams.newDataInput(Base64.decodeBase64(base64));
    split.readFields(byteArrayDataInput);
    return split;
  }

  public List<FieldReference> getColumns() {
    return columns;
  }

  public List<InputSplit> getInputSplits() {
    return inputSplits;
  }

  @Override
  public OperatorCost getCost() {
    return new OperatorCost(1, 2, 1, 1);
  }

  @Override
  public Size getSize() {
    // TODO - this is wrong, need to populate correctly
    return new Size(10,10);
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitSubScan(this, value);
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
    try {
      return new HiveSubScan(table, partitions, encodedSplits, splitClasses, columns);
    } catch (IOException | ReflectiveOperationException e) {
      throw new ExecutionSetupException(e);
    }
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return Iterators.emptyIterator();
  }
}
