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
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import org.apache.drill.common.exceptions.CustomErrorContext;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.framework.SchemaNegotiator;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;
import org.apache.drill.exec.record.metadata.MetadataUtils;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.cassandra.connection.CassandraConnectionManager;
import org.apache.drill.exec.store.cassandra.CassandraSubScan.CassandraSubScanSpec;
import org.apache.drill.exec.util.Utilities;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
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

  private RowSetLoader rowWriter;

  private ResultSet cassandraResultset;

  private List<ScalarWriter> columnWriters;

  private List<CassandraColumnWriter> cassandraColumnWriters;


  public CassandraBatchReader(CassandraStoragePluginConfig conf, CassandraSubScan subScan) {
    this.config = conf;
    this.subScan = subScan;
    this.projectedColumns = subScan.getColumns();
    this.plugin = subScan.getCassandraStoragePlugin();
  }

  @Override
  public boolean open(SchemaNegotiator negotiator) {
    CustomErrorContext parentErrorContext = negotiator.parentErrorContext();

    // Set up the Cassandra Cluster
    setupCluster();

    resultLoader = negotiator.build();
    rowWriter = resultLoader.writer();

    // Setup the query
    setup(negotiator);

    return true;
  }

  // TODO Start here... map rows to EVF Vectors
   public boolean next() {
     while (!rowWriter.isFull()) {
       if (!processRow(rowWriter)) {
         return false;
       }
     }
     return true;
  }

  private boolean processRow(RowSetLoader rowWriter) {

    if (!cassandraResultset.iterator().hasNext()) {
      return false;
    }
    Row currentRow = cassandraResultset.iterator().next();
    rowWriter.start();
    int colPosition = 0;
    for (ColumnDefinitions.Definition def : currentRow.getColumnDefinitions()) {
      cassandraColumnWriters.get(colPosition).load(currentRow, def.getName());
      colPosition++;
    }
    rowWriter.save();
    return true;
  }


  @Override
  public void close() {
    // Do nothing.
    // We want to keep the Cassandra session open until Drill closes
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

  private void setup(SchemaNegotiator negotiator) {
    CassandraSubScanSpec subScanSpec = subScan.getChunkScanSpecList().get(0); // TODO get the right subscan.. for now, get the first one.

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

      // Apply projected columns to query
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

      // Apply start/end tokens
      if (subScanSpec.getStartToken() != null) {
        where = where.and(QueryBuilder.gte(QueryBuilder.token(partitionkeys), new Long(subScanSpec.getStartToken())));
      }
      if (subScanSpec.getEndToken() != null) {
        where = where.and(QueryBuilder.lt(QueryBuilder.token(partitionkeys), new Long(subScanSpec.getEndToken())));
      }

      // Push down filters
      if (subScanSpec.filter != null && subScanSpec.filter.size() > 0) {
        logger.debug("Filters: {}", subScanSpec.filter.toString());
        for (Clause filter : subScanSpec.filter) {
          logger.debug("In loop: {} ", filter.toString());
          where = where.and(filter);
        }
      }

      // Execute query
      logger.debug("Query sent to Cassandra: {}", where);
      cassandraResultset = session.execute(where);

      // Get the first row of data to build the Drill schema
      Row firstRow = cassandraResultset.one();

      columnWriters = new ArrayList<>(firstRow.getColumnDefinitions().size());
      cassandraColumnWriters = new ArrayList<>();

      // Set the schema
      negotiator.tableSchema(buildDrillSchema(firstRow), true);

    } catch (Exception e) {
      throw UserException
        .resourceError(e)
        .message("Error starting Cassandra Storage Plugin: %s", e.getMessage())
        .build(logger);
    }
  }


  /**
   * Builds the schema from the first row of data in the Cassandra data set.
   * @param firstRow The first row of the Cassandra dataset
   * @return A TupleMetadata object of the built schema
   */
  private TupleMetadata buildDrillSchema(Row firstRow) {
    int colPosition = 0;
    SchemaBuilder builder = new SchemaBuilder();
    for (ColumnDefinitions.Definition def : firstRow.getColumnDefinitions()) {
      String colName = def.getName();
      String dataType = def.getType().toString();

      // Create Schema
      // TODO Add additional datatypes
      switch (dataType) {
        case "varchar":
          builder.addNullable(colName, TypeProtos.MinorType.VARCHAR);
          addColumnToArray(rowWriter, colName, TypeProtos.MinorType.VARCHAR);
          break;
        case "int":
          builder.addNullable(colName, TypeProtos.MinorType.INT);
          addColumnToArray(rowWriter, colName, TypeProtos.MinorType.INT);
          break;
        case "bigint":
          builder.addNullable(colName, TypeProtos.MinorType.BIGINT);
          addColumnToArray(rowWriter, colName, TypeProtos.MinorType.BIGINT);
          break;
      }

      colPosition++;
    }
    return builder.build();

  }

  private void addColumnToArray(TupleWriter rowWriter, String name, TypeProtos.MinorType type) {
    int index = rowWriter.tupleSchema().index(name);
    if (index == -1) {
      org.apache.drill.exec.record.metadata.ColumnMetadata colSchema = MetadataUtils.newScalar(name, type, TypeProtos.DataMode.OPTIONAL);
      index = rowWriter.addColumn(colSchema);
    } else {
      return;
    }
    columnWriters.add(rowWriter.scalar(index));
    if (type == TypeProtos.MinorType.VARCHAR) {
      cassandraColumnWriters.add(new StringColumnWriter(columnWriters.get(index)));
    } else if (type == TypeProtos.MinorType.INT) {
      cassandraColumnWriters.add(new IntColumnWriter(columnWriters.get(index)));
    } else if (type == TypeProtos.MinorType.BIGINT) {
      cassandraColumnWriters.add(new BigIntColumnWriter(columnWriters.get(index)));
    }
  }


  public abstract class CassandraColumnWriter {

    protected Row row;

    protected String colName;

    ScalarWriter columnWriter;

    public CassandraColumnWriter(ScalarWriter columnWriter) {
      this.columnWriter = columnWriter;
    }

    public abstract void load(Row row, String colName);
  }

  public class StringColumnWriter extends CassandraColumnWriter {

    StringColumnWriter(ScalarWriter columnWriter) {
      super(columnWriter);
    }

    public void load(Row row, String colName) {
      String value = row.getString(colName);
      if (value == null) {
        columnWriter.setNull();
      } else {
        columnWriter.setString(value);
      }
    }
  }

  public class IntColumnWriter extends CassandraColumnWriter {

    IntColumnWriter(ScalarWriter columnWriter) {
      super(columnWriter);
    }

    public void load(Row row, String colName) {
      int value = row.getInt(colName);
      columnWriter.setInt(value);
    }
  }

  public class BigIntColumnWriter extends CassandraColumnWriter {

    BigIntColumnWriter(ScalarWriter columnWriter) {
      super(columnWriter);
    }

    public void load(Row row, String colName) {
      long value = row.getLong(colName);
      columnWriter.setLong(value);
    }
  }

}
