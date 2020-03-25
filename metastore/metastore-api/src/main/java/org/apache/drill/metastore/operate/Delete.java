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
package org.apache.drill.metastore.operate;

import org.apache.drill.metastore.expressions.FilterExpression;
import org.apache.drill.metastore.metadata.MetadataType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;

/**
 * Delete operation holder, it includes filter by which Metastore data will be deleted
 * and list of metadata types to which filter will be applied.
 *
 * Note: providing at list one metadata type is required.
 * If delete operation should be applied to all metadata types,
 * {@link MetadataType#ALL} can be indicated.
 */
public class Delete {

  private final List<MetadataType> metadataTypes;
  private final FilterExpression filter;

  private Delete(Builder builder) {
    this.metadataTypes = builder.metadataTypes;
    this.filter = builder.filter;
  }

  public static Builder builder() {
    return new Builder();
  }

  public List<MetadataType> metadataTypes() {
    return metadataTypes;
  }

  public FilterExpression filter() {
    return filter;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", Delete.class.getSimpleName() + "[", "]")
      .add("metadataTypes=" + metadataTypes)
      .add("filter=" + filter)
      .toString();
  }

  public static class Builder {
    private final List<MetadataType> metadataTypes = new ArrayList<>();
    private FilterExpression filter;

    public Builder metadataTypes(List<MetadataType> metadataTypes) {
      this.metadataTypes.addAll(metadataTypes);
      return this;
    }

    public Builder metadataType(MetadataType... metadataTypes) {
      return metadataTypes(Arrays.asList(metadataTypes));
    }

    public Builder filter(FilterExpression filter) {
      this.filter = filter;
      return this;
    }

    public Delete build() {
      return new Delete(this);
    }
  }
}
