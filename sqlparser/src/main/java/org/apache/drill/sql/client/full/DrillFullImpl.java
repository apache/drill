package org.apache.drill.sql.client.full;

import java.util.List;

import net.hydromatic.linq4j.Enumerator;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.proto.UserProtos;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.jdbc.DrillTable;

public class DrillFullImpl<E>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillFullImpl.class);

  private final String plan;
  final DrillConfig config;
  private final List<String> fields;

  
  public DrillFullImpl(String plan, DrillConfig config, List<String> fields) {
    super();
    this.plan = plan;
    this.config = config;
    this.fields = fields;
  }

  @SuppressWarnings("unchecked")
  public Enumerator<E> enumerator(DrillClient client) {
    
    BatchListener listener = new BatchListener();

    // TODO: use a completion service from the container
    QueryRequestRunner runner = new QueryRequestRunner(plan, client, listener);
    runner.start();
    
    return (Enumerator<E>) new ResultEnumerator(listener, client, fields);
    
  }
  
  public class QueryRequestRunner extends Thread{
    final String plan;
    final DrillClient client;
    final BatchListener listener;
    
    public QueryRequestRunner(String plan, DrillClient client, BatchListener listener) {
      super();
      this.setDaemon(true);
      this.plan = plan;
      this.client = client;
      this.listener = listener;
    }

    @Override
    public void run() {
      client.runQuery(UserProtos.QueryType.LOGICAL, plan, listener);
    }
  }
}
