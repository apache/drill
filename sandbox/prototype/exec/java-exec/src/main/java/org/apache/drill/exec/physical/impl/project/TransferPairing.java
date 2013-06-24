package org.apache.drill.exec.physical.impl.project;

import org.apache.drill.exec.record.vector.ValueVector;

public class TransferPairing<T extends ValueVector<T>> {
  
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TransferPairing.class);
  
  final T from;
  final T to;
  
  protected TransferPairing(T from, T to) {
    super();
    this.from = from;
    this.to = to;
  }

  public void transfer(){
    from.transferTo(to);
  }
  
  public static <T extends ValueVector<T>> TransferPairing<T> getTransferPairing(T from, T to){
    return new TransferPairing<T>(from, to);
  }

  public T getFrom() {
    return from;
  }

  public T getTo() {
    return to;
  }
  
  
}
