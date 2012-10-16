package org.apache.drill.plan.physical.operators;

/**
* Created with IntelliJ IDEA.
* User: tdunning
* Date: 10/15/12
* Time: 5:39 PM
* To change this template use File | Settings | File Templates.
*/
class InvalidData extends Throwable {
    public InvalidData(String msg) {
        super(msg);
    }
}
