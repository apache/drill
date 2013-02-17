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
package org.apache.drill.common.logical.data;

import org.apache.drill.common.expression.FieldReference;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("windowframe")
public class WindowFrame extends SingleInputOperator{
  
  
  private final FieldReference within;
  private final FrameRef frame;
  private final long start;
  private final long end;
  

  @JsonCreator
  public WindowFrame(@JsonProperty("within") FieldReference within, @JsonProperty("ref") FrameRef frame, @JsonProperty("start") Long start, @JsonProperty("end") Long end) {
    super();
    this.within = within;
    this.frame = frame;
    this.start = start == null ? Long.MIN_VALUE : start;
    this.end = end == null ? Long.MIN_VALUE : end;
  }


  @JsonProperty("ref")
  public FrameRef getFrame() {
    return frame;
  }

  public FieldReference getWithin() {
    return within;
  }

  public long getStart() {
    return start;
  }

  public long getEnd() {
    return end;
  }

  public static class FrameRef{
    private final FieldReference segment;
    private final FieldReference position;
    
    @JsonCreator
    public FrameRef(@JsonProperty("segment") FieldReference segment, @JsonProperty("position") FieldReference position) {
      super();
      this.segment = segment;
      this.position = position;
    }
    
    public FieldReference getSegment() {
      return segment;
    }
    public FieldReference getPosition() {
      return position;
    }
    
    
  }
}
