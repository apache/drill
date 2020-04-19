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
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import org.apache.drill.common.exceptions.CustomErrorContext;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.framework.SchemaNegotiator;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.cassandra.connection.CassandraConnectionManager;
import org.apache.drill.exec.store.cassandra.CassandraSubScan.CassandraSubScanSpec;
import org.apache.drill.exec.util.Utilities;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.joda.time.Instant;
import org.joda.time.LocalTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

public class CassandraBatchReader implements ManagedReader<SchemaNegotiator> {

  private static final Logger logger = LoggerFactory.getLogger(CassandraBatchReader.class);

  private final CassandraStoragePluginConfig config;

  private final CassandraStoragePlugin plugin;

  private final CassandraSubScan subScan;

  private final List<SchemaPath> projectedColumns;

  private final List<CassandraColumnWriter> cassandraColumnWriters;

  private Cluster cluster;

  private Session session;

  private CassandraSubScanSpec subScanSpec;

  private RowSetLoader rowWriter;

  private ResultSet cassandraResultset;

  private Row firstRow;


  public CassandraBatchReader(CassandraStoragePluginConfig conf, CassandraSubScan subScan) {
    this.config = conf;
    this.subScan = subScan;
    this.projectedColumns = subScan.getColumns();
    this.plugin = subScan.getCassandraStoragePlugin();
    this.subScanSpec = subScan.getChunkScanSpecList().get(0); // TODO get the right subscan.. for now, get the first one.

    cassandraColumnWriters = new ArrayList<>();

  }

  @Override
  public boolean open(SchemaNegotiator negotiator) {
    CustomErrorContext parentErrorContext = negotiator.parentErrorContext();

    // Set up the Cassandra Cluster
    setupCluster();
    firstRow = getFirstRow();

    // Build the Schema
    SchemaBuilder builder = new SchemaBuilder();
    TupleMetadata drillSchema = buildDrillSchema(builder, firstRow);
    negotiator.tableSchema(drillSchema, true);
    ResultSetLoader resultLoader = negotiator.build();

    // Create ScalarWriters
    rowWriter = resultLoader.writer();
    populateWriterArray(firstRow);

    return true;
  }

  @Override
  public boolean next() {
    // Process the first row
    Row currentRow = firstRow;

    while (!rowWriter.isFull()) {

      // Case for empty set
      if (firstRow == null) {
        return false;
      }

      processRow(currentRow);

      // If there are no more rows left, stop processing
      if (!cassandraResultset.iterator().hasNext()) {
        return false;
      } else {
        currentRow = cassandraResultset.iterator().next();
      }
    }
    return true;
  }

  private void processRow(Row currentRow) {
    rowWriter.start();
    for (int i = 0; i < currentRow.getColumnDefinitions().size(); i++) {
      CassandraColumnWriter writer = cassandraColumnWriters.get(i);
      writer.load(currentRow);
    }
    rowWriter.save();
  }

  @Override
  public void close() {
    // Do nothing.
    // We want to keep the Cassandra session open until Drill closes
  }

  /**
   * Initializes the Cassandra cluster and sessions
   */
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

  private Row getFirstRow() {
    // Get Cassandra Partition Keys
    String[] partitionkeys = getPartitionKeys();

    // Define query
    Statement cassandraQuery = buildQuery(partitionkeys);

    // Execute query
    logger.debug("Query sent to Cassandra: {}", cassandraQuery);
    cassandraResultset = session.execute(cassandraQuery);

    // Get the first row of data to build the Drill schema
    return cassandraResultset.one();
  }

  private String[] getPartitionKeys() {
    List<ColumnMetadata> partitioncols = cluster
      .getMetadata()
      .getKeyspace(subScanSpec.getKeyspace())
      .getTable(subScanSpec.getTable())
      .getPartitionKey();

    String[] partitionkeys = new String[partitioncols.size()];
    for (int index = 0; index < partitioncols.size(); index++) {
      partitionkeys[index] = partitioncols.get(index).getName();
    }
    return partitionkeys;
  }

  /**
   * Build the query which is sent to Cassandra.  This method also includes pushdowns from Drill
   * @param partitionkeys These are the Cassandra Partition keys to indicate where the data is physically stored.
   * @return A fully built Cassandra query
   */
  private Statement buildQuery(String[] partitionkeys) {

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
    return where;
  }

  /**
   * Builds the schema from the first row of data in the Cassandra data set.
   * @param firstRow The first row of the Cassandra dataset
   * @return A TupleMetadata object of the built schema
   */
  private TupleMetadata buildDrillSchema(SchemaBuilder builder, Row firstRow) {
    // Case for empty result set
    if (firstRow == null || firstRow.getColumnDefinitions().size() == 0) {
      return builder.buildSchema();
    }

    for (ColumnDefinitions.Definition def : firstRow.getColumnDefinitions()) {
      String colName = def.getName();
      String dataType = def.getType().toString();

      // TODO Add all other datatypes supported by Cassandra
      switch (dataType) {
        case "ascii":
        case "text":
        case "varchar":
          builder.addNullable(colName, TypeProtos.MinorType.VARCHAR);
          break;
        case "int":
          builder.addNullable(colName, TypeProtos.MinorType.INT);
          break;
        case "varint":
        case "bigint":
          builder.addNullable(colName, TypeProtos.MinorType.BIGINT);
          break;
        case "smallint":
        case "tinyint":
          builder.addNullable(colName, TypeProtos.MinorType.SMALLINT);
          break;
        case "float":
          builder.addNullable(colName, TypeProtos.MinorType.FLOAT4);
          break;
        case "double":
          builder.addNullable(colName, TypeProtos.MinorType.FLOAT8);
          break;
        case "timestamp":
          builder.addNullable(colName, TypeProtos.MinorType.TIMESTAMP);
          break;
        case "date":
          builder.addNullable(colName, TypeProtos.MinorType.DATE);
          break;
        case "time":
          builder.addNullable(colName, TypeProtos.MinorType.TIME);
          break;
        case "boolean":
          builder.addNullable(colName, TypeProtos.MinorType.BIT);
          break;
        default:
          logger.warn("Unknown data type: {} for column {}", dataType, colName);
      }
    }
    return builder.buildSchema();
  }

  private void populateWriterArray(Row firstRow) {
    // Case for empty result set
    if (firstRow == null || firstRow.getColumnDefinitions().size() == 0) {
      return;
    }

    for (ColumnDefinitions.Definition def : firstRow.getColumnDefinitions()) {
      String colName = def.getName();
      String dataType = def.getType().toString();
      switch (dataType) {
        case "ascii":
        case "text":
        case "varchar":
          cassandraColumnWriters.add(new StringColumnWriter(colName, rowWriter));
          break;
        case "int":
          cassandraColumnWriters.add(new IntColumnWriter(colName, rowWriter));
          break;
        case "varint":
        case "bigint":
          cassandraColumnWriters.add(new BigIntColumnWriter(colName, rowWriter));
          break;
        case "smallint":
        case "tinyint":
          cassandraColumnWriters.add(new SmallIntColumnWriter(colName, rowWriter));
          break;
        case "float":
          cassandraColumnWriters.add(new FloatColumnWriter(colName, rowWriter));
          break;
        case "double":
          cassandraColumnWriters.add(new DoubleColumnWriter(colName, rowWriter));
          break;
        case "decimal":
          cassandraColumnWriters.add(new DecimalColumnWriter(colName, rowWriter));
          break;
        case "timestamp":
          cassandraColumnWriters.add(new TimestampColumnWriter(colName, rowWriter));
          break;
        case "time":
          cassandraColumnWriters.add(new TimeColumnWriter(colName, rowWriter));
          break;
        case "date":
          cassandraColumnWriters.add(new DateColumnWriter(colName, rowWriter));
          break;
        case "boolean":
          cassandraColumnWriters.add(new BooleanColumnWriter(colName, rowWriter));
          break;
        default:
          logger.warn("Unknown data type: {} for column {}", dataType, colName);
      }
    }
  }

  public abstract static class CassandraColumnWriter {

    protected String colName;

    ScalarWriter columnWriter;

    public void load(Row row) {};
  }

  public static class StringColumnWriter extends CassandraColumnWriter {

    StringColumnWriter(String colName, RowSetLoader rowWriter) {
      this.colName = colName;
      columnWriter = rowWriter.scalar(colName);
    }

    @Override
    public void load(Row row) {
      String value = row.getString(colName);
      if (value == null) {
        columnWriter.setNull();
      } else {
        columnWriter.setString(value);
      }
    }
  }

  public static class IntColumnWriter extends CassandraColumnWriter {

    IntColumnWriter(String colName, RowSetLoader rowWriter) {
      this.colName = colName;
      columnWriter = rowWriter.scalar(colName);
    }

    @Override
    public void load(Row row) {
      int value = row.getInt(colName);
      columnWriter.setInt(value);
    }
  }

  public static class BigIntColumnWriter extends CassandraColumnWriter {

    BigIntColumnWriter(String colName, RowSetLoader rowWriter) {
      this.colName = colName;
      columnWriter = rowWriter.scalar(colName);
    }

    @Override
    public void load(Row row) {
      long value = row.getLong(colName);
      columnWriter.setLong(value);
    }
  }

  public static class SmallIntColumnWriter extends CassandraColumnWriter {

    SmallIntColumnWriter(String colName, RowSetLoader rowWriter) {
      this.colName = colName;
      columnWriter = rowWriter.scalar(colName);
    }

    @Override
    public void load(Row row) {
      short value = row.getShort(colName);
      columnWriter.setInt(value);
    }
  }

  public static class FloatColumnWriter extends CassandraColumnWriter {

    FloatColumnWriter(String colName, RowSetLoader rowWriter) {
      this.colName = colName;
      columnWriter = rowWriter.scalar(colName);
    }

    @Override
    public void load(Row row) {
      float value = row.getFloat(colName);
      columnWriter.setDouble(value);
    }
  }
  public static class DoubleColumnWriter extends CassandraColumnWriter {

    DoubleColumnWriter(String colName, RowSetLoader rowWriter) {
      this.colName = colName;
      columnWriter = rowWriter.scalar(colName);
    }

    @Override
    public void load(Row row) {
      double value = row.getDouble(colName);
      columnWriter.setDouble(value);
    }
  }

  public static class DecimalColumnWriter extends CassandraColumnWriter {

    DecimalColumnWriter(String colName, RowSetLoader rowWriter) {
      this.colName = colName;
      columnWriter = rowWriter.scalar(colName);
    }

    @Override
    public void load(Row row) {
      BigDecimal value = row.getDecimal(colName);
      columnWriter.setDecimal(value);
    }
  }

  public static class TimestampColumnWriter extends CassandraColumnWriter {

    TimestampColumnWriter(String colName, RowSetLoader rowWriter) {
      this.colName = colName;
      columnWriter = rowWriter.scalar(colName);
    }

    @Override
    public void load(Row row) {
      Instant value = new Instant(row.getTimestamp(colName));
      columnWriter.setTimestamp(value);
    }
  }

  public static class DateColumnWriter extends CassandraColumnWriter {

    DateColumnWriter(String colName, RowSetLoader rowWriter) {
      this.colName = colName;
      columnWriter = rowWriter.scalar(colName);
    }

    @Override
    public void load(Row row) {
      LocalDate value = row.getDate(colName);
      org.joda.time.LocalDate d = new org.joda.time.LocalDate(value.getYear(), value.getMonth(), value.getDay());
      columnWriter.setDate(d);
    }
  }

  public static class TimeColumnWriter extends CassandraColumnWriter {

    TimeColumnWriter(String colName, RowSetLoader rowWriter) {
      this.colName = colName;
      columnWriter = rowWriter.scalar(colName);
    }

    @Override
    public void load(Row row) {
      long value = row.getTime(colName);
      columnWriter.setTime(new LocalTime(value));
    }
  }

  public static class BooleanColumnWriter extends CassandraColumnWriter {

    BooleanColumnWriter(String colName, RowSetLoader rowWriter) {
      this.colName = colName;
      columnWriter = rowWriter.scalar(colName);
    }

    @Override
    public void load(Row row) {
      boolean value = row.getBool(colName);
      columnWriter.setBoolean(value);
    }
  }
}
