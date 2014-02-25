package org.apache.drill.exec.store.ischema;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.SchemaPlus;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.logical.data.Scan;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaHolder;

import com.google.hive12.common.collect.ImmutableMap;
import com.google.hive12.common.collect.Maps;

public class InfoSchemaStoragePlugin extends AbstractStoragePlugin{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(InfoSchemaStoragePlugin.class);

  private final InfoSchemaConfig config;
  private final DrillbitContext context;
  private final String name;
  
  public InfoSchemaStoragePlugin(InfoSchemaConfig config, DrillbitContext context, String name){
    this.config = config;
    this.context = context;
    this.name = name;
  }
  
  @Override
  public boolean supportsRead() {
    return true;
  }

  @Override
  public InfoSchemaGroupScan getPhysicalScan(Scan scan) throws IOException {
    SelectedTable table = scan.getSelection().getWith(context.getConfig(),  SelectedTable.class);
    return new InfoSchemaGroupScan(table);
  }

  @Override
  public Schema createAndAddSchema(SchemaPlus parent) {
    Schema s = new ISchema(parent);
    parent.add(s);
    return s;
  }
  
  private class ISchema extends AbstractSchema{
    private Map<String, InfoSchemaDrillTable> tables;
    public ISchema(SchemaPlus parent){
      super(new SchemaHolder(parent), "INFORMATION_SCHEMA");
      Map<String, InfoSchemaDrillTable> tbls = Maps.newHashMap();
      for(SelectedTable tbl : SelectedTable.values()){
        tbls.put(tbl.name(), new InfoSchemaDrillTable("INFORMATION_SCHEMA", tbl, config));  
      }
      this.tables = ImmutableMap.copyOf(tbls);
    }
    
    @Override
    public DrillTable getTable(String name) {
      return tables.get(name);
    }
    
    @Override
    public Set<String> getTableNames() {
      return tables.keySet();
    }
    
  }
}
