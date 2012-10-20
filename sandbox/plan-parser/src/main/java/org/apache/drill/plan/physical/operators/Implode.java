package org.apache.drill.plan.physical.operators;

import com.google.common.collect.Lists;
import org.apache.drill.plan.ast.Arg;
import org.apache.drill.plan.ast.Op;

import java.util.List;
import java.util.Map;

/**
 * Aggregate records into a list that is inserted into the parent record for the batch.
 */
public class Implode extends Operator implements DataListener, BatchListener {
    private Operator data;
    private List<Object> accumulator = Lists.newArrayList();
    private Schema schema   ;
    private String accumulatorVar;

    public static void define() {
        Operator.defineOperator("implode", Implode.class);
    }

    public Implode(Op op, Map<Integer, Operator> bindings) {
        // data-out, implode-var := implode data-in, batch-master
        super(op, bindings, 2, 2);
    }

    @Override
    public void link(Op op, Map<Integer, Operator> bindings) {
        List<Arg> in = op.getInputs();
        data = bindings.get(in.get(0).asSymbol().getInt());
        data.addDataListener(this);

//        schema = data.getSchema().overlay();
        schema = new JsonSchema();
        Operator batchController = bindings.get(in.get(1).asSymbol().getInt());
        batchController.addBatchListener(this);

        accumulatorVar = Operator.gensym();
    }

    @Override
    public Object eval() {
        return accumulatorVar;
    }

    @Override
    public Schema getSchema() {
        throw new UnsupportedOperationException("Default operation");
    }

    @Override
    public void notify(Object r) {
        accumulator.add(r);
    }

    @Override
    public void endBatch(Object parent) {
        schema.set(accumulatorVar, parent, accumulator);
        accumulator = Lists.newArrayList();
        emit(parent);
    }
}
