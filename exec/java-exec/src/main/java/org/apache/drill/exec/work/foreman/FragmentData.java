package org.apache.drill.exec.work.foreman;

import org.apache.drill.exec.proto.BitControl.FragmentStatus;
import org.apache.drill.exec.proto.BitControl.FragmentStatus.FragmentState;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;

public class FragmentData {
  private final boolean isLocal;
  private volatile FragmentStatus status;
  private volatile long lastStatusUpdate = 0;
  private final DrillbitEndpoint endpoint;

  public FragmentData(FragmentHandle handle, DrillbitEndpoint endpoint, boolean isLocal) {
    super();
    this.status = FragmentStatus.newBuilder().setHandle(handle).setState(FragmentState.SENDING).build();
    this.endpoint = endpoint;
    this.isLocal = isLocal;
  }

  public void setStatus(FragmentStatus status){
    this.status = status;
    lastStatusUpdate = System.currentTimeMillis();
  }

  public FragmentStatus getStatus() {
    return status;
  }

  public boolean isLocal() {
    return isLocal;
  }

  public long getLastStatusUpdate() {
    return lastStatusUpdate;
  }

  public DrillbitEndpoint getEndpoint() {
    return endpoint;
  }


}