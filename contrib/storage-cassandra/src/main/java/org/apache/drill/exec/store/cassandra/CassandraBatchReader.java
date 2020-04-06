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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import org.apache.drill.common.exceptions.CustomErrorContext;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.framework.SchemaNegotiator;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.store.cassandra.connection.CassandraConnectionManager;
import org.apache.drill.exec.util.Utilities;
import org.apache.drill.shaded.guava.com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class CassandraBatchReader implements ManagedReader<SchemaNegotiator> {

  private static final Logger logger = LoggerFactory.getLogger(CassandraBatchReader.class);

  private final CassandraStoragePluginConfig config;

  private final CassandraStoragePlugin plugin;

  private final CassandraSubScan subScan;

  private final List<SchemaPath> projectedColumns;

  private Cluster cluster;

  private Session session;

  private ResultSetLoader resultLoader;

  private ResultSet cassandraResultset;

  public CassandraBatchReader(CassandraStoragePluginConfig conf, CassandraSubScan subScam) {
    this.config = conf;
    this.subScan = subScam;
    this.projectedColumns = subScam.getColumns();
    this.plugin = subScan.getCassandraStoragePlugin();
  }

  @Override
  public boolean open(SchemaNegotiator negotiator) {
    CustomErrorContext parentErrorContext = negotiator.parentErrorContext();

    // Set up the Cassandra Cluster
    setupCluster();

    // Setup the query
    setup();

    resultLoader = negotiator.build();
    return true;
  }

   public boolean next() {
    Stopwatch watch = Stopwatch.createUnstarted();
    watch.start();


    return false;
  }


  @Override
  public void close() {

  }

  private void setupCluster() {
    // Cassandra sessions are expensive to open, so the connection is opened in the
    // Storage plugin class and closed when Drill is shut down OR when the storage plugin
    // is disabled.
    if (plugin.getCluster() == null || plugin.getCluster().isClosed() ) {
      cluster = CassandraConnectionManager.getCluster(config);
      session = cluster.connect();
    } else {
      cluster = plugin.getCluster();
      session = plugin.getSession();
    }
  }

  private void setup() {

    try {
      List<ColumnMetadata> partitioncols = cluster
        .getMetadata()
        .getKeyspace(subScanSpec.getKeyspace())
        .getTable(subScanSpec.getTable())
        .getPartitionKey();

      String[] partitionkeys = new String[partitioncols.size()];
      for (int index = 0; index < partitioncols.size(); index++) {
        partitionkeys[index] = partitioncols.get(index).getName();
      }

      Statement q;

      /* Project only required columns */
      Select.Where where;
      Select.Selection select = QueryBuilder.select();
      if (Utilities.isStarQuery(projectedColumns)) {
        where = select.all().from(subScanSpec.getKeyspace(), subScanSpec.getTable()).allowFiltering().where();
      } else {
        for (SchemaPath path : projectedColumns) {
          if (path.getAsNamePart().getName().equals("**")) {
            continue;
          } else {
            select = select.column(path.getAsNamePart().getName());
          }
        }
        where = select.from(subScanSpec.getKeyspace(), subScanSpec.getTable()).allowFiltering().where();
      }

      if (subScanSpec.getStartToken() != null) {
        where = where.and(QueryBuilder.gte(QueryBuilder.token(partitionkeys), new Long(subScanSpec.getStartToken())));
      }
      if (subScanSpec.getEndToken() != null) {
        where = where.and(QueryBuilder.lt(QueryBuilder.token(partitionkeys), new Long(subScanSpec.getEndToken())));
      }

      if (subScanSpec.filter != null && subScanSpec.filter.size() > 0) {
        logger.debug("Filters: {}", subScanSpec.filter.toString());
        for (Clause filter : subScanSpec.filter) {
          logger.debug("In loop: {} ", filter.toString());
          where = where.and(filter);
        }
      }

      q = where;
      logger.debug("Query sent to Cassandra: {}", q);
      cassandraResultset = session.execute(q);

      // Build the schema

      for (SchemaPath column: projectedColumns) {
        String colName = column.rootName();
        String dataType = cassandraResultset
          .getColumnDefinitions()
          .getType(colName)
          .toString();
      }


    } catch (Exception e) {
      throw UserException
        .resourceError(e)
        .message("Error starting Cassandra Storage Plugin: %s", e.getMessage())
        .build(logger);
    }
  }
}
