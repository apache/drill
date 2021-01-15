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

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Clause;
import org.apache.drill.common.PlanStringBuilder;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.base.AbstractBase;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;

import java.util.Objects;

@JsonTypeName("cassandra-subscan")
public class CassandraSubScan extends AbstractBase implements SubScan {
  private static final Logger logger = LoggerFactory.getLogger(CassandraSubScan.class);

  @JsonProperty
  private final CassandraStoragePluginConfig cassandraPluginConfig;

  @JsonProperty
  private final List<SchemaPath> columns;

  @JsonProperty
  private final List<CassandraSubScanSpec> chunkScanSpecList;

  @JsonIgnore
  private final CassandraStoragePlugin cassandraStoragePlugin;

  @JsonIgnore
  private Cluster cluster;

  @JsonIgnore
  private Session session;


  @JsonCreator
  public CassandraSubScan(@JacksonInject("registry") StoragePluginRegistry registry,
                          @JsonProperty("cassandraPluginConfig") StoragePluginConfig cassandraPluginConfig,
                          @JsonProperty("chunkScanSpecList") LinkedList<CassandraSubScanSpec> chunkScanSpecList,
                          @JsonProperty("columns") List<SchemaPath> columns
                          ) throws ExecutionSetupException {

    this.columns = columns;
    this.cassandraPluginConfig = (CassandraStoragePluginConfig) cassandraPluginConfig;
    this.cassandraStoragePlugin = (CassandraStoragePlugin) registry.getPlugin(cassandraPluginConfig);
    this.chunkScanSpecList = chunkScanSpecList;
  }

  public CassandraSubScan(CassandraStoragePlugin storagePlugin,
                          CassandraStoragePluginConfig storagePluginConfig,
                          List<CassandraSubScanSpec> chunkScanSpecList,
                          List<SchemaPath> columns) {
    this.cassandraStoragePlugin = storagePlugin;
    this.cassandraPluginConfig = storagePluginConfig;
    this.columns = columns;
    this.chunkScanSpecList = chunkScanSpecList;
  }

  public CassandraSubScan(CassandraStoragePlugin storagePlugin,
                          CassandraStoragePluginConfig storagePluginConfig,
                          List<CassandraSubScanSpec> chunkScanSpecList,
                          List<SchemaPath> columns,
                          Cluster cluster,
                          Session session) {
    this.cassandraStoragePlugin = storagePlugin;
    this.cassandraPluginConfig = storagePluginConfig;
    this.columns = columns;
    this.chunkScanSpecList = chunkScanSpecList;
    this.cluster = cluster;
    this.session = session;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitSubScan(this, value);
  }

  @JsonIgnore
  public CassandraStoragePluginConfig getCassandraPluginConfig() {
    return cassandraPluginConfig;
  }

  @JsonIgnore
  public CassandraStoragePlugin getCassandraStoragePlugin() {
    return cassandraStoragePlugin;
  }

  public List<SchemaPath> getColumns() {
    return columns;
  }

  public List<CassandraSubScanSpec> getChunkScanSpecList() {
    return chunkScanSpecList;
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
    Preconditions.checkArgument(children.isEmpty());
    return new CassandraSubScan(cassandraStoragePlugin, cassandraPluginConfig, chunkScanSpecList, columns, cluster, session);
  }

  @Override
  public int getOperatorType() {
    return UserBitShared.CoreOperatorType.CASSANDRA_SUB_SCAN_VALUE;
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return ImmutableSet.<PhysicalOperator>of().iterator();
  }

  public static class CassandraSubScanSpec {

    public String keyspace;

    public String table;

    public List<String> hosts;

    public int port;

    public String startToken;

    public String endToken;

    @JsonIgnore
    protected Cluster cluster;

    @JsonIgnore
    protected Session session;

    @JsonIgnore // TODO Filters not set
    protected List<Clause> filter;

    @JsonCreator
    public CassandraSubScanSpec(@JsonProperty("keyspace") String keyspace,
                                @JsonProperty("table") String table,
                                @JsonProperty("hosts") List<String> hosts,
                                @JsonProperty("port") int port,
                                @JsonProperty("startToken") String startToken,
                                @JsonProperty("endToken") String endToken,
                                @JacksonInject("filter") List<Clause> filter,
                                @JacksonInject Cluster cluster,
                                @JacksonInject Session session) {
      this.keyspace = keyspace;
      this.table = table;
      this.hosts = hosts;
      this.port = port;
      this.startToken = startToken;
      this.endToken = endToken;
      this.filter = filter;
      this.session = session;
      this.cluster = cluster;
    }

    // Needed for Group Scan
    public CassandraSubScanSpec() {}

    public String getKeyspace() {
      return keyspace;
    }

    public CassandraSubScanSpec setKeyspace(String keyspace) {
      this.keyspace = keyspace;
      return this;
    }

    public String getTable() {
      return table;
    }

    public CassandraSubScanSpec setTable(String table) {
      this.table = table;
      return this;
    }

    public List<String> getHosts() {
      return hosts;
    }

    public CassandraSubScanSpec setHosts(List<String> hosts) {
      this.hosts = hosts;
      return this;
    }

    public int getPort() {
      return port;
    }

    public CassandraSubScanSpec setPort(int port) {
      this.port = port;
      return this;
    }

    public CassandraSubScanSpec setFilter(List<Clause> filter) {
      this.filter = filter;
      return this;
    }

    public String getStartToken() {
      return startToken;
    }

    public CassandraSubScanSpec setStartToken(String startToken) {
      this.startToken = startToken;
      return this;
    }

    public CassandraSubScanSpec setCluster(Cluster cluster) {
      this.cluster = cluster;
      return this;
    }

    public Cluster getCluster() {
      return cluster;
    }

    public Session getSession() {
      return session;
    }

    public String getEndToken() {
      return endToken;
    }

    public CassandraSubScanSpec setEndToken(String endToken) {
      this.endToken = endToken;
      return this;
    }

    @Override
    public String toString() {
      return new PlanStringBuilder(this)
        .field("keyspace", keyspace)
        .field("table", table)
        .field("host", hosts)
        .field("port", port)
        .field("startToken", startToken)
        .field("endToken", endToken)
        .field("filter", filter)
        .toString();
    }

    @Override
    public int hashCode() {
      return Objects.hash(keyspace, table, hosts, port, startToken, endToken, filter);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      CassandraSubScanSpec other = (CassandraSubScanSpec) obj;
      return Objects.equals(keyspace, other)
        && Objects.equals(table, other.table)
        && Objects.equals(hosts, other.hosts)
        && Objects.equals(port, other.port)
        && Objects.equals(startToken, other.startToken)
        && Objects.equals(endToken, other.endToken)
        && Objects.equals(filter, other.filter);
    }
  }
}
