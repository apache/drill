package org.apache.drill.exec.ref.rops;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang.NotImplementedException;
import org.apache.drill.common.logical.data.SingleInputOperator;
import org.apache.drill.exec.ref.RecordIterator;
import org.apache.drill.exec.ref.RecordPointer;

/**
 * For simplification purposes, the Windowing reference implementation takes the lazy approach of finishing a window
 * before it outputs any values from that window. While this is necessary in the ALL:ALL scenario, other scenarios could
 * be implemented more efficiently with an appropriately size open window.
 */
public class WindowingROP extends SingleInputROPBase {

  private List<RecordPointer> records = new LinkedList<RecordPointer>();
  private WindowManager[] windows;
  private Window[] windowPerKey;

  // the place where we should start the next batch.
  private int internalWindowPosition;

  public WindowingROP(SingleInputOperator config) {
    super(config);
    throw new NotImplementedException();
  }

  @Override
  protected void setInput(RecordIterator incoming) {
  }

  @Override
  protected RecordIterator getIteratorInternal() {
    return null;
  }

  private class Window {

    int ending;
    List<RecordPointer> records = new ArrayList<RecordPointer>();

    public void reset(int curPos) {

    }
  }

  private class WindowManager {
    private void increment() {

    }

  }
}
