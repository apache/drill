package org.apache.drill.exec.store.mongo.schema;

import java.util.List;
import java.util.Set;

import net.hydromatic.optiq.Table;

import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.mongo.MongoStoragePluginConfig;
import org.apache.drill.exec.store.mongo.schema.MongoSchemaFactory.MongoSchema;

import com.google.common.collect.Sets;

public class MongoDatabaseSchema extends AbstractSchema {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory
      .getLogger(MongoDatabaseSchema.class);
  private final MongoSchema mongoSchema;
  private final Set<String> tables;

  public MongoDatabaseSchema(List<String> tableList, MongoSchema mongoSchema,
      String name) {
    super(mongoSchema.getSchemaPath(), name);
    this.mongoSchema = mongoSchema;
    this.tables = Sets.newHashSet(tableList);
  }

  @Override
  public Table getTable(String tableName) {
    return mongoSchema.getDrillTable(this.name, tableName);
  }

  @Override
  public Set<String> getTableNames() {
    return tables;
  }

  @Override
  public String getTypeName() {
    return MongoStoragePluginConfig.NAME;
  }

}
