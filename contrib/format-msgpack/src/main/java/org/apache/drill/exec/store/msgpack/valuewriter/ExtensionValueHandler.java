package org.apache.drill.exec.store.msgpack.valuewriter;

public interface ExtensionValueHandler extends ValueWriter {

  /**
   * 0 to 127 are application specific types.
   * @return
   */
  public byte getExtensionTypeNumber();
}
