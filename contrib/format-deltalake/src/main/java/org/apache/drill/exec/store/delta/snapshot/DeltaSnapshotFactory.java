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
package org.apache.drill.exec.store.delta.snapshot;

public class DeltaSnapshotFactory {
  public static final DeltaSnapshotFactory INSTANCE = new DeltaSnapshotFactory();

  public DeltaSnapshot create(SnapshotContext context) {
    if (context.getSnapshotAsOfVersion() != null) {
      return new DeltaSnapshotByVersion(context.getSnapshotAsOfVersion());
    } else if (context.getSnapshotAsOfTimestamp() != null) {
      return new DeltaSnapshotByTimestamp(context.getSnapshotAsOfTimestamp());
    } else {
      return DeltaLatestSnapshot.INSTANCE;
    }
  }

  public static class SnapshotContext {
    private final Long snapshotAsOfVersion;

    private final Long snapshotAsOfTimestamp;

    SnapshotContext(SnapshotContextBuilder builder) {
      this.snapshotAsOfVersion = builder.snapshotAsOfVersion;
      this.snapshotAsOfTimestamp = builder.snapshotAsOfTimestamp;
    }

    public static SnapshotContextBuilder builder() {
      return new SnapshotContextBuilder();
    }

    public Long getSnapshotAsOfVersion() {
      return this.snapshotAsOfVersion;
    }

    public Long getSnapshotAsOfTimestamp() {
      return this.snapshotAsOfTimestamp;
    }

    public static class SnapshotContextBuilder {
      private Long snapshotAsOfVersion;

      private Long snapshotAsOfTimestamp;

      public SnapshotContextBuilder snapshotAsOfVersion(Long snapshotAsOfVersion) {
        this.snapshotAsOfVersion = snapshotAsOfVersion;
        return this;
      }

      public SnapshotContextBuilder snapshotAsOfTimestamp(Long snapshotAsOfTime) {
        this.snapshotAsOfTimestamp = snapshotAsOfTime;
        return this;
      }

      public SnapshotContext build() {
        return new SnapshotContext(this);
      }
    }
  }
}
