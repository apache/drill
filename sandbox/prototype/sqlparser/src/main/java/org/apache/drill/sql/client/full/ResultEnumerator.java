package org.apache.drill.sql.client.full;

import java.util.List;

import net.hydromatic.linq4j.Enumerator;
import net.hydromatic.optiq.DataContext;

import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.rpc.RpcException;

/**
   * Enumerator used for full execution engine.
   */
  class ResultEnumerator implements Enumerator<Object> {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ResultEnumerator.class);
    
    private final BatchLoaderMap loaderMap;
    private Object current;
    
    public ResultEnumerator(BatchListener listener, DrillClient client, List<String> fields) {
      this.loaderMap = new BatchLoaderMap(fields, listener, client);
    }

    public Object current() {
      return current;
    }

    public boolean moveNext() {
      
      try {
        boolean succ = loaderMap.next();
        if(succ){
          current = loaderMap.getCurrentObject();         
        }
        return succ;
        
      } catch (InterruptedException e) {
        Thread.interrupted();
        logger.error("Exception during query", e);
        throw new RuntimeException(e);
      } catch (RpcException | SchemaChangeException e) {
        logger.error("Exception during query", e);
        throw new RuntimeException(e);
      }
    }

    public void reset() {
      throw new UnsupportedOperationException();
    }
    
    public void close(){
      
    }
  }