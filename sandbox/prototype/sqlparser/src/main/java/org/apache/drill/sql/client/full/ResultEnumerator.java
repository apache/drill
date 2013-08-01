package org.apache.drill.sql.client.full;

import java.util.List;
import java.util.Map;

import net.hydromatic.linq4j.Enumerator;

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.server.DrillbitContext;

/**
   * Enumerator used for full execution engine.
   */
  class ResultEnumerator implements Enumerator<Object> {

    private final BatchLoaderMap loaderMap;
    private Object current;
    
    public ResultEnumerator(BatchListener listener, DrillbitContext context, List<String> fields) {
      this.loaderMap = new BatchLoaderMap(fields, listener, context);
    }

    public Object current() {
      return current;
    }

    public boolean moveNext() {
      
      try {
        boolean succ = loaderMap.next();
        if(succ){
          current = loaderMap.getCurrentAsObjectArray();          
        }
        return succ;
        
      } catch (InterruptedException e) {
        Thread.interrupted();
        throw new RuntimeException(e);
      } catch (RpcException | SchemaChangeException e) {
        throw new RuntimeException(e);
      }
    }

    public void reset() {
      throw new UnsupportedOperationException();
    }
  }