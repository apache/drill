package org.apache.drill.exec.work.foreman;

import java.util.Map;

import org.apache.drill.exec.cache.DistributedCache;
import org.apache.drill.exec.proto.BitControl.FragmentStatus;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.helper.QueryIdHelper;

import com.google.common.collect.Maps;

public class QueryStatus {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(QueryStatus.class);

  public Map<FragmentHandle, FragmentData> map = Maps.newHashMap(); // doesn't need to be thread safe as map is generated in a single thread and then accessed by multiple threads for reads only.

  private final String queryId;

  public QueryStatus(QueryId id, DistributedCache cache){
    this.queryId = QueryIdHelper.getQueryId(id);
    cache.getMultiMap(QueryStatus.class);

  }

  void add(FragmentHandle handle, FragmentData data){
    if(map.put(handle,  data) != null) throw new IllegalStateException();
  }

  void update(FragmentStatus status){
    map.get(status.getHandle()).setStatus(status);
  }
}
