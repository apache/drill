package org.apache.drill.common.logical.data.visitors;

import org.apache.drill.common.logical.data.*;

/**
 * Created with IntelliJ IDEA.
 * User: jaltekruse
 * Date: 6/10/13
 * Time: 1:55 PM
 * To change this template use File | Settings | File Templates.
 */
public abstract class AbstractLogicalVisitor<T, X, E extends Throwable> implements LogicalVisitor<T, X, E> {

    public T visitOp(LogicalOperator op, X value) throws E{
        throw new UnsupportedOperationException(String.format(
                "The LogicalVisitor of type %s does not currently support visiting the PhysicalOperator type %s.", this
                .getClass().getCanonicalName(), op.getClass().getCanonicalName()));
    }

    @Override
    public T visitScan(Scan scan, X value) throws E {
        return visitOp(scan, value);
    }

    @Override
    public T visitStore(Store store, X value) throws E {
        return visitOp(store, value);
    }

    @Override
    public T visitFilter(Filter filter, X value) throws E {
        return visitOp(filter, value);
    }

    @Override
    public T visitFlatten(Flatten flatten, X value) throws E {
        return visitOp(flatten, value);
    }

    @Override
    public T visitProject(Project project, X value) throws E {
        return visitOp(project, value);
    }

    @Override
    public T visitOrder(Order order, X value) throws E {
        return visitOp(order, value);
    }

    @Override
    public T visitJoin(Join join, X value) throws E {
        return visitOp(join, value);
    }

    @Override
    public T visitLimit(Limit limit, X value) throws E {
        return visitOp(limit, value);
    }

    @Override
    public T visitRunningAggregate(RunningAggregate runningAggregate, X value) throws E {
        return visitOp(runningAggregate, value);
    }

    @Override
    public T visitSegment(Segment segment, X value) throws E {
        return visitOp(segment, value);
    }

    @Override
    public T visitSequence(Sequence sequence, X value) throws E {
        return visitOp(sequence, value);
    }

    @Override
    public T visitTransform(Transform transform, X value) throws E {
        return visitOp(transform, value);
    }

    @Override
    public T visitUnion(Union union, X value) throws E {
        return visitOp(union, value);
    }

    @Override
    public T visitCollapsingAggregate(CollapsingAggregate collapsingAggregate, X value) throws E {
        return visitOp(collapsingAggregate, value);
    }

    @Override
    public T visitWindowFrame(WindowFrame windowFrame, X value) throws E {
        return visitOp(windowFrame, value);
    }
}
