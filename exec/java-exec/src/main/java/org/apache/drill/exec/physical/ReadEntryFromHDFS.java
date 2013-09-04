/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.exec.physical;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.drill.exec.physical.base.Size;

public class ReadEntryFromHDFS extends ReadEntryWithPath {

  private long start;
  private long length;

  @JsonCreator
  public ReadEntryFromHDFS(@JsonProperty("path") String path,@JsonProperty("start") long start, @JsonProperty("length") long length) {
    this.path = path;
    this.start = start;
    this.length = length;
  }

  @Override
  public OperatorCost getCost() {
    return new OperatorCost(1, 2, 1, 1);
  }

  @Override
  public Size getSize() {
    // TODO - these values are wrong, I cannot know these until after I read a file
    return new Size(10, 10);
  }

  public long getStart() {
    return start;
  }

  public long getLength() {
    return length;
  }
}
