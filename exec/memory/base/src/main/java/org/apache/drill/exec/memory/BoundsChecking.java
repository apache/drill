package org.apache.drill.exec.memory;

public class BoundsChecking {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BoundsChecking.class);

  public static final boolean BOUNDS_CHECKING_ENABLED;

  static {
    boolean isAssertEnabled = false;
    assert isAssertEnabled = true;
    BOUNDS_CHECKING_ENABLED = isAssertEnabled
        || !"true".equals(System.getProperty("drill.enable_unsafe_memory_access"));
  }

  private BoundsChecking() {
  }
}
