package org.apache.drill.sql.client.full;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import jline.internal.Preconditions;
import net.hydromatic.optiq.DataContext;

import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarBinaryVector;
import org.apache.drill.exec.vector.VarCharVector;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.common.collect.Maps;

@JsonSerialize(using = BatchLoaderMap.Se.class)
public class BatchLoaderMap implements Map<String, Object> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BatchLoaderMap.class);

  private BatchListener listener;
  private RecordBatchLoader loader;
  private final List<String> requestedFields;
  private final Map<String, ValueVector> fields = Maps.newHashMap();
  private int index;
  private Object[] objArr;
  private JsonHelper helper;
  private boolean loaded;

  public BatchLoaderMap(List<String> requestedFields, BatchListener listener, DrillClient client, DataContext context) {
    this.listener = listener;
    this.requestedFields = requestedFields;
    this.objArr = new Object[requestedFields.size()];
    this.loader = new RecordBatchLoader(client.getAllocator());
    this.helper = new JsonHelper(client.getConfig());
    FileSystemSchema fsSchema = (FileSystemSchema) context;
  }

  private void load(QueryResultBatch batch) throws SchemaChangeException {
    boolean schemaChanged = loader.load(batch.getHeader().getDef(), batch.getData());
    if (schemaChanged) {
      fields.clear();
      for (VectorWrapper<?> v : loader) {
        fields.put(v.getField().getName(), v.getValueVector());
      }
    } else {
      logger.debug("Schema didn't change. {}", batch);
    }
  }

  public boolean next() throws SchemaChangeException, RpcException, InterruptedException {
    index++;
    if (index < loader.getRecordCount()) {
      return true;
    } else {
      logger.debug("Starting next query result batch.");
      QueryResultBatch qrb;
      while( (qrb = listener.getNext()) != null && !qrb.hasData()){
        qrb = listener.getNext();
      }
      
      if (qrb == null) {
        logger.debug("No more batches found.");
        index = -1;
        return false;
      } else {
        load(qrb);
        logger.debug("New batch found and loaded. {}", qrb.getHeader().getDef());
        index = 0;
        return true;
      }

    }
  }


  
  public Object getCurrentObject() {
    if(requestedFields.size() == 1){
      if(requestedFields.iterator().next().equals("_MAP")){
        return helper.get(fields, index); 
      }else{
        return getObj(fields.get(requestedFields.get(0)));
      }
      
    }
      
    for (int i = 0; i < requestedFields.size(); i++) {
      ValueVector vv = fields.get(requestedFields.get(i));
      if (vv == null) {
        logger.debug("Failure while retrieving field of {}.  Didn't exist in returned set of {}", requestedFields.get(i), fields.keySet());
        objArr[i] = null;
      } else {
        objArr[i] = getObj(vv);
      }
    }
    return objArr;

  }

  private Object getObj(ValueVector vv){
    if(vv instanceof VarBinaryVector || vv instanceof VarCharVector){
      return new String( (byte[]) vv.getAccessor().getObject(index));
    }else{
      return vv.getAccessor().getObject(index);  
    }
  }
  
  @Override
  public int size() {
    return fields.size();
  }

  @Override
  public boolean isEmpty() {
    return fields.isEmpty();
  }

  @Override
  public boolean containsKey(Object key) {
    if (!(key instanceof String))
      return false;
    return fields.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object get(Object key) {
    ValueVector v = fields.get(key);
    Preconditions.checkNotNull(v);
    return v.getAccessor().getObject(index);
  }

  @Override
  public Object put(String key, Object value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object remove(Object key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putAll(Map<? extends String, ? extends Object> m) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear() {
  }

  @Override
  public Set<String> keySet() {
    return fields.keySet();
  }

  @Override
  public Collection<Object> values() {
    throw new UnsupportedOperationException();
  }

  public String toString() {
    try {
      return new ObjectMapper().writeValueAsString(this);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Set<java.util.Map.Entry<String, Object>> entrySet() {
    throw new UnsupportedOperationException();
  }

  public static class Se extends StdSerializer<BatchLoaderMap> {

    public Se() {
      super(BatchLoaderMap.class);
    }

    @Override
    public void serialize(BatchLoaderMap value, JsonGenerator jgen, SerializerProvider provider) throws IOException,
        JsonGenerationException {
      jgen.writeStartObject();
      assert value.index > -1 && value.index < value.loader.getRecordCount();
      for (Map.Entry<String, ValueVector> me : value.fields.entrySet()) {
        jgen.writeObjectField(me.getKey(), me.getValue().getAccessor().getObject(value.index));
      }
      jgen.writeEndObject();
    }

  }
}
