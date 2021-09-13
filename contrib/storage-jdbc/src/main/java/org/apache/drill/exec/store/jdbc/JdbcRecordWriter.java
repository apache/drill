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

package org.apache.drill.exec.store.jdbc;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.expr.holders.NullableIntHolder;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.store.AbstractRecordWriter;
import org.apache.drill.exec.store.EventBasedRecordWriter.FieldConverter;
import org.apache.drill.exec.store.jdbc.utils.JdbcQueryBuilder;
import org.apache.drill.exec.vector.complex.reader.FieldReader;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.Map;

public class JdbcRecordWriter extends AbstractRecordWriter {

  private static final Logger logger = LoggerFactory.getLogger(JdbcRecordWriter.class);
  public static final ImmutableMap<MinorType, Integer> JDBC_TYPE_MAPPINGS;
  private final DataSource source;
  private final String name;
  private final Connection connection;

  /*
   * This map maps JDBC data types to their Drill equivalents.  The basic strategy is that if there
   * is a Drill equivalent, then do the mapping as expected.  All flavors of INT (SMALLINT, TINYINT etc)
   * are mapped to INT in Drill, with the exception of BIGINT.
   *
   * All flavors of character fields are mapped to VARCHAR in Drill. All versions of binary fields are
   * mapped to VARBINARY.
   *
   */
  static {
    JDBC_TYPE_MAPPINGS = ImmutableMap.<MinorType, Integer>builder()
      .put(MinorType.FLOAT8, java.sql.Types.DOUBLE)
      .put(MinorType.FLOAT4 ,java.sql.Types.FLOAT)
      .put(MinorType.TINYINT, java.sql.Types.TINYINT)
      .put(MinorType.SMALLINT, java.sql.Types.SMALLINT)
      .put(MinorType.INT, java.sql.Types.INTEGER)
      .put(MinorType.BIGINT, java.sql.Types.BIGINT)

      .put(MinorType.VARCHAR, java.sql.Types.VARCHAR)
      .put(MinorType.VARBINARY, java.sql.Types.VARBINARY)

      .put( MinorType.VARDECIMAL, java.sql.Types.DECIMAL)

      .put(MinorType.DATE, java.sql.Types.DATE)
      .put(MinorType.TIME, java.sql.Types.TIME)
      .put(MinorType.TIMESTAMP, java.sql.Types.TIMESTAMP)

      .put(MinorType.BIT, java.sql.Types.BOOLEAN)
      .build();
  }

  public JdbcRecordWriter(DataSource source, OperatorContext context, String name) {
    this.source = source;
    this.name = name;
    try {
      this.connection = source.getConnection();
    } catch (SQLException e) {
      throw UserException.connectionError()
        .message("Unable to open JDBC connection for writing.")
        .addContext(e.getSQLState())
        .build(logger);
    }
  }

  @Override
  public void init(Map<String, String> writerOptions) throws IOException {

  }

  @Override
  public void updateSchema(VectorAccessible batch) throws IOException {
    BatchSchema schema = batch.getSchema();
    String columnName;
    MinorType type;
    String sql;
    Statement statement;
    boolean nullable = false;
    JdbcQueryBuilder queryBuilder = new JdbcQueryBuilder(name);

    for (Iterator<MaterializedField> it = schema.iterator(); it.hasNext(); ) {
      MaterializedField field = it.next();
      columnName = field.getName();
      type = field.getType().getMinorType();
      logger.debug("Adding column {} of type {}.", columnName, type);

      if (field.getType().getMode() == DataMode.OPTIONAL) {
        nullable = true;
      }
      queryBuilder.addColumn(columnName, field.getType().getMinorType(), nullable);
    }
    sql = queryBuilder.getCreateTableQuery();
    logger.debug("Final query: {}", sql);

    // Execute the query to build the schema
    try {
      statement = connection.createStatement();
      statement.execute(sql);
    } catch (SQLException e) {
      throw UserException.dataReadError(e)
        .message("The JDBC storage plugin failed while trying to create the schema. ")
        .addContext("Sql", sql)
        .build(logger);
    }
  }

  @Override
  public void startRecord() throws IOException {

  }

  @Override
  public void endRecord() throws IOException {

  }

  @Override
  public void abort() throws IOException {

  }

  @Override
  public void cleanup() throws IOException {
    try {
      connection.close();
    } catch (SQLException e) {
      throw new IOException();
    }
  }

  @Override
  public FieldConverter getNewIntConverter(int fieldId, String fieldName, FieldReader reader) {
    return new NullableIntJDBCConverter(fieldId, fieldName, reader);
  }

  public static class NullableIntJDBCConverter extends FieldConverter {
    private final NullableIntHolder holder = new NullableIntHolder();

    public NullableIntJDBCConverter(int fieldID, String fieldName, FieldReader reader) {
      super(fieldID, fieldName, reader);
    }

    @Override
    public void writeField() {
      reader.read(holder);
    }
  }
}
