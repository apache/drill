package org.apache.drill.exec.expr.fn.impl;

import org.apache.drill.exec.vector.NullableVarBinaryHolder;
import org.apache.drill.exec.vector.NullableVarCharHolder;
import org.apache.drill.exec.vector.VarBinaryHolder;
import org.apache.drill.exec.vector.VarCharHolder;

public class VarHelpers {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(VarHelpers.class);

  public static final int compare(VarBinaryHolder left, VarCharHolder right) {
    for (int l = left.start, r = right.start; l < left.end && r < right.end; l++, r++) {
      byte leftByte = left.buffer.getByte(l);
      byte rightByte = right.buffer.getByte(r);
      if (leftByte != rightByte) {
        return ((leftByte & 0xFF) - (rightByte & 0xFF)) > 0 ? 1 : -1;
      }
    }

    int l = (left.end - left.start) - (right.end - right.start);
    if (l > 0) {
      return 1;
    } else if (l == 0) {
      return 0;
    } else {
      return -1;
    }

  }

  public static final int compare(VarCharHolder left, VarCharHolder right) {
    for (int l = left.start, r = right.start; l < left.end && r < right.end; l++, r++) {
      byte leftByte = left.buffer.getByte(l);
      byte rightByte = right.buffer.getByte(r);
      if (leftByte != rightByte) {
        return ((leftByte & 0xFF) - (rightByte & 0xFF)) > 0 ? 1 : -1;
      }
    }

    int l = (left.end - left.start) - (right.end - right.start);
    if (l > 0) {
      return 1;
    } else if (l == 0) {
      return 0;
    } else {
      return -1;
    }

  }

  public static final int compare(NullableVarBinaryHolder left, NullableVarBinaryHolder right) {
    if (left.isSet == 0) {
      if (right.isSet == 0)
        return 0;
      return -1;
    } else if (right.isSet == 0) {
      return 1;
    }

    for (int l = left.start, r = right.start; l < left.end && r < right.end; l++, r++) {
      byte leftByte = left.buffer.getByte(l);
      byte rightByte = right.buffer.getByte(r);
      if (leftByte != rightByte) {
        return ((leftByte & 0xFF) - (rightByte & 0xFF)) > 0 ? 1 : -1;
      }
    }

    int l = (left.end - left.start) - (right.end - right.start);
    if (l > 0) {
      return 1;
    } else if (l == 0) {
      return 0;
    } else {
      return -1;
    }

  }

  public static final int compare(NullableVarBinaryHolder left, NullableVarCharHolder right) {
    if (left.isSet == 0) {
      if (right.isSet == 0)
        return 0;
      return -1;
    } else if (right.isSet == 0) {
      return 1;
    }

    for (int l = left.start, r = right.start; l < left.end && r < right.end; l++, r++) {
      byte leftByte = left.buffer.getByte(l);
      byte rightByte = right.buffer.getByte(r);
      if (leftByte != rightByte) {
        return ((leftByte & 0xFF) - (rightByte & 0xFF)) > 0 ? 1 : -1;
      }
    }

    int l = (left.end - left.start) - (right.end - right.start);
    if (l > 0) {
      return 1;
    } else if (l == 0) {
      return 0;
    } else {
      return -1;
    }

  }

  public static final int compare(NullableVarCharHolder left, NullableVarCharHolder right) {
    if (left.isSet == 0) {
      if (right.isSet == 0)
        return 0;
      return -1;
    } else if (right.isSet == 0) {
      return 1;
    }

    for (int l = left.start, r = right.start; l < left.end && r < right.end; l++, r++) {
      byte leftByte = left.buffer.getByte(l);
      byte rightByte = right.buffer.getByte(r);
      if (leftByte != rightByte) {
        return ((leftByte & 0xFF) - (rightByte & 0xFF)) > 0 ? 1 : -1;
      }
    }

    int l = (left.end - left.start) - (right.end - right.start);
    if (l > 0) {
      return 1;
    } else if (l == 0) {
      return 0;
    } else {
      return -1;
    }

  }

  public static final int compare(VarBinaryHolder left, VarBinaryHolder right) {
    for (int l = left.start, r = right.start; l < left.end && r < right.end; l++, r++) {
      byte leftByte = left.buffer.getByte(l);
      byte rightByte = right.buffer.getByte(r);
      if (leftByte != rightByte) {
        return ((leftByte & 0xFF) - (rightByte & 0xFF)) > 0 ? 1 : -1;
      }
    }

    int l = (left.end - left.start) - (right.end - right.start);
    if (l > 0) {
      return 1;
    } else if (l == 0) {
      return 0;
    } else {
      return -1;
    }

  }

}
