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

import java.math.BigDecimal;

import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

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
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.cassandra.connection.CassandraConnectionManager;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.ValueVector;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.drill.shaded.guava.com.google.common.base.Stopwatch;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;

public class CassandraRecordReader extends AbstractRecordReader implements DrillCassandraConstants {
  private static final Logger logger = LoggerFactory.getLogger(CassandraRecordReader.class);

  private ResultSet rs;

  private OutputMutator outputMutator;

  private CassandraStoragePlugin plugin;

  private final String cassandraTableName;

  private final CassandraSubScan.CassandraSubScanSpec subScanSpec;

  private final String cassandraKeySpace;

  private final CassandraStoragePluginConfig cassandraConf;

  private final List<SchemaPath> projectedColumns;

  private NullableVarCharVector vector;

  private List<ValueVector> vectors = Lists.newArrayList();

  private OperatorContext operatorContext;

  public CassandraRecordReader(CassandraStoragePluginConfig conf, CassandraSubScan.CassandraSubScanSpec subScanSpec, List<SchemaPath> projectedColumns, FragmentContext context,
                               CassandraStoragePlugin plugin) {
    this.cassandraTableName = Preconditions.checkNotNull(subScanSpec, "Cassandra reader needs a sub-scan spec").getTable();
    this.cassandraKeySpace = Preconditions.checkNotNull(subScanSpec, "Cassandra reader needs a sub-scan spec").getKeyspace();
    this.subScanSpec = subScanSpec;
    this.projectedColumns = projectedColumns;
    this.cassandraConf = conf;
    this.plugin = plugin;

    setColumns(projectedColumns);
  }

  @Override
  protected Collection<SchemaPath> transformColumns(Collection<SchemaPath> columns) {
    //TODO:
    return columns;
  }


  @Override
  public void setup(OperatorContext context, OutputMutator output) throws ExecutionSetupException {
    this.outputMutator = output;
    this.operatorContext = context;

    try {
      logger.debug("Opening scanner for Cassandra table '{}'.", cassandraTableName);

      List<String> host = subScanSpec.getHosts();
      int port = subScanSpec.getPort();


      // Cassandra sessions are expensive to open, so the connection is opened in the
      // Storage plugin class and closed when Drill is shut down OR when the storage plugin
      // is disabled.
      Cluster cluster;
      Session session;
      if (plugin.getCluster() == null || plugin.getCluster().isClosed() ) {
        cluster = CassandraConnectionManager.getCluster(cassandraConf);
        session = cluster.connect();
      } else {
        cluster = plugin.getCluster();
        session = plugin.getSession();
      }

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
      if (isStarQuery()) {
        where = select.all().from(subScanSpec.getKeyspace(), subScanSpec.getTable()).allowFiltering().where();
      } else {
        for (SchemaPath path : getColumns()) {
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
      rs = session.execute(q);

      for (SchemaPath column : getColumns()) {
        if (isStarQuery()) {
          Iterator<ColumnDefinitions.Definition> iter = rs.getColumnDefinitions().iterator();

          /* Add all columns to ValueVector */
          while (iter.hasNext()) {
            ColumnDefinitions.Definition def = iter.next();
            MaterializedField field = MaterializedField.create(def.getName(), COLUMN_TYPE);
            vector = outputMutator.addField(field, NullableVarCharVector.class);
          }
        } else {
          MaterializedField field = MaterializedField.create(column.getRootSegment().getPath(), COLUMN_TYPE);
          vector = outputMutator.addField(field, NullableVarCharVector.class);
        }
      }
    } catch (SchemaChangeException e) {
      throw new ExecutionSetupException("Failure in Cassandra Record Reader setup. Cause: ", e);
    }
  }

  @Override
  public int next() {
    Stopwatch watch = Stopwatch.createUnstarted();
    watch.start();
    int rowCount = 0;
    Row row = null;
    int start, end, batchsize = 0;
    start = end = -1;
    try {
      vectors = Lists.newArrayList();
      // TODO: Use Batch Size - TARGET_RECORD_COUNT(3000)
      for (; rs.iterator().hasNext(); rowCount++) {

        if (operatorContext != null) {
          operatorContext.getStats().startWait();
        }
        try {
          if (rs.iterator().hasNext()) {
            row = rs.iterator().next();
          }
        } finally {
          if (operatorContext != null) {
            operatorContext.getStats().stopWait();
          }
        }
        if (row == null) {
          break;
        }

        for (SchemaPath col : getColumns()) {
          if (isStarQuery()) {
            /* Add all columns to ValueVector */
            for (ColumnDefinitions.Definition def : row.getColumnDefinitions()) {
              updateValueVector(row, def.getName(), rowCount);
            }
          } else {
            updateValueVector(row, col.getAsNamePart().getName(), rowCount);
          }
        }
        logger.debug("text scan batch size {}", batchsize);
      }

      for (ValueVector v : vectors) {
        v.getMutator().setValueCount(rowCount);
      }
      logger.debug("Took {} ms to get {} records", watch.elapsed(TimeUnit.MILLISECONDS), rowCount);
      return rowCount;
    } catch (Exception e) {
      if (operatorContext != null) {
        operatorContext.getStats().stopWait();
      }
      throw new DrillRuntimeException(e);
    }
  }

  private void updateValueVector(Row row, String colname, int rowCount) {
    try {
      String val = getAsString(row, colname);
      int start = 0;
      int end = val.length();

      MaterializedField field = MaterializedField.create(colname, COLUMN_TYPE);
      vector = outputMutator.addField(field, NullableVarCharVector.class);

      vector.getMutator().setSafe(rowCount, val.getBytes(), start, end - start);
      vectors.add(vector);
    } catch (Exception e) {
      throw UserException
        .dataReadError()
        .message("Error reading data from Cassandra: %s", e.getMessage())
        .build(logger);
    }
  }


  @Override
  public void close() {
    // Do nothing.
    // We want to keep the Cassandra session open until Drill closes
  }

  /**
   * Utility function to get the type of the column and return its String value.
   * TODO: Convert to appropriate Drill Type.
   *
   * @param r
   * @param colname
   * @return
   */
  public String getAsString(Row r, String colname) {
    String value;
    try {
      Class clazz = r.getColumnDefinitions().getType(colname).getClass();

      String dataType = r.getColumnDefinitions().getType(colname).asFunctionParameterString();
      logger.debug("Colname: {}, Datatype: {}", colname, dataType);

      if (dataType.equalsIgnoreCase("long")) {
        value = String.valueOf(r.getLong(colname));
      } else if (clazz.isInstance(Boolean.FALSE)) {
        value = String.valueOf(r.getBool(colname));
      } else if (clazz.isInstance(Byte.MIN_VALUE)) {
        value = String.valueOf(r.getBytes(colname));
      } else if (clazz.isInstance(new Date())) {
        value = String.valueOf(r.getDate(colname));
      } else if (clazz.isInstance(BigDecimal.ZERO)) {
        value = String.valueOf(r.getDecimal(colname));
      } else if (dataType.equalsIgnoreCase("double")) {
        value = String.valueOf(r.getDouble(colname));
      } else if (dataType.equalsIgnoreCase("float")) {
        value = String.valueOf(r.getFloat(colname));
      } else if (dataType.equalsIgnoreCase("int")) {
        value = String.valueOf(r.getInt(colname));
      } else if (dataType.equalsIgnoreCase("varchar")) {
        value = r.getString(colname);
      } else if (dataType.equalsIgnoreCase("bigint")) {
        long bigInt = r.getLong(colname);
        value = String.valueOf(bigInt);
      } else {
        value = null;  // TODO Should not return null..
      }
    } catch (Exception e) {
      throw UserException
        .dataReadError()
        .message("Unable to get Cassandra column: %s, of type: %s.", colname,
          r.getColumnDefinitions().getType(colname).asFunctionParameterString())
        .addContext(e.getMessage())
        .build(logger);
    }
    return value;
  }
}
