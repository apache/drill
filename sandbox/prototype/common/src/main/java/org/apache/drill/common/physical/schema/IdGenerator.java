package org.apache.drill.common.physical.schema;

/**
 * Created with IntelliJ IDEA.
 * User: tnachen
 * Date: 1/2/13
 * Time: 10:50 PM
 * To change this template use File | Settings | File Templates.
 */
public interface IdGenerator<T> {
    public T getNextId();
    public void reset();
}
