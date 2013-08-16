package org.apache.drill.sql.client.full;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import net.hydromatic.linq4j.QueryProvider;
import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.Table;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;

import org.apache.drill.common.logical.StorageEngineConfig;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.store.SchemaProvider;
import org.apache.drill.jdbc.DrillTable;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

public class FileSystemSchema implements Schema{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FileSystemSchema.class);
  
  private ConcurrentMap<String, TableInSchema> tables = Maps.newConcurrentMap();

  private final JavaTypeFactory typeFactory;
  private final Schema parentSchema;
  private final String name;
  private final Expression expression;
  private final QueryProvider queryProvider;
  private final SchemaProvider schemaProvider;
  private final DrillClient client;
  private final StorageEngineConfig config;
  
  public FileSystemSchema(DrillClient client, StorageEngineConfig config, SchemaProvider schemaProvider, JavaTypeFactory typeFactory, Schema parentSchema, String name, Expression expression,
      QueryProvider queryProvider) {
    super();
    this.client = client;
    this.typeFactory = typeFactory;
    this.parentSchema = parentSchema;
    this.name = name;
    this.expression = expression;
    this.queryProvider = queryProvider;
    this.schemaProvider = schemaProvider;
    this.config = config;
  }

  @Override
  public Schema getSubSchema(String name) {
    return null;
  }

  @Override
  public JavaTypeFactory getTypeFactory() {
    return typeFactory;
  }

  @Override
  public Schema getParentSchema() {
    return parentSchema;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Expression getExpression() {
    return expression;
  }

  @Override
  public QueryProvider getQueryProvider() {    
    return queryProvider;
  }


  @Override
  public Collection<TableFunctionInSchema> getTableFunctions(String name) {
    return Collections.emptyList();
  }
  
  @Override
  public Multimap<String, TableFunctionInSchema> getTableFunctions() {
    return ArrayListMultimap.create();
  }

  @Override
  public Collection<String> getSubSchemaNames() {
    return Collections.EMPTY_LIST;
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public <E> Table<E> getTable(String name, Class<E> elementType) {
    if( !elementType.isAssignableFrom(DrillTable.class)) throw new UnsupportedOperationException();
    TableInfo info = (TableInfo) tables.get(name);
    if(info != null) return (Table<E>) info.table;
    Object selection = schemaProvider.getSelectionBaseOnName(name);
    if(selection == null) return null;
    
    DrillTable table = DrillTable.createTable(client, typeFactory, this, name, config, selection);
    info = new TableInfo(name, table);
    TableInfo oldInfo = (TableInfo) tables.putIfAbsent(name, info);
    if(oldInfo != null) return (Table<E>) oldInfo.table;
    return (Table<E>) table;
  }

  @Override
  public Map<String, TableInSchema> getTables() {
    return this.tables;
  }
  
  private class TableInfo extends TableInSchema{
    
    final DrillTable table;

    public TableInfo(String name, DrillTable table) {
      super(FileSystemSchema.this, name, TableType.TABLE);
      this.table = table;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <E> Table<E> getTable(Class<E> elementType) {
      if( !elementType.isAssignableFrom(DrillTable.class)) throw new UnsupportedOperationException();
      return (Table<E>) table;
    }
    
    
  }
  
}
