package org.apache.drill.common.storage;


public interface ValueModifier {
    public boolean modifyValue(ValueHolder value);
}
