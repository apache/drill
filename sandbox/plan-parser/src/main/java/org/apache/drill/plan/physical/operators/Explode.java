package org.apache.drill.plan.physical.operators;

import org.apache.drill.plan.ast.Arg;
import org.apache.drill.plan.ast.Op;

import java.util.List;
import java.util.Map;

/**
 * Explode creates a secondary data flow that contains a batch of records for
 * each input record.  Typically, Explode is paired with Implode which gathers
 * the results back together or with Aggregate which gathers results in a different
 * way.  Cogroup, Explode, Aggregate gives a group by aggregate and
 * Explode, Filter, Implode gives sub-tree filtering.
 *
 * Explode has two outputs.  One is a data source and one is a batch controller (source of batch
 * boundaries).  Any downstream operator that delays emitting records should
 * listen to the batch boundaries to avoid having aggregate outputs that cross batch
 * boundaries.
 */
public class Explode extends Operator implements DataListener {
    private String variableToExplode;
    private Schema schema;
    private Schema subSchema;

    public static void define() {
        Operator.defineOperator("explode", Explode.class);
    }

    public Explode(Op op, Map<Integer, Operator> bindings) {
        // exploded-data-stream, batch-controller := explode data-in, variable-name-to-explode
        super(op, bindings, 2, 1);
    }

    @Override
    public void link(Op op, Map<Integer, Operator> bindings) {
        List<Arg> in = op.getInputs();
        Operator data = bindings.get(in.get(0).asSymbol().getInt());
        data.addDataListener(this);
        schema = data.getSchema();

        variableToExplode = in.get(1).asString();
        subSchema = schema.getSubSchema(variableToExplode);
    }

    @Override
    public void notify(Object row) {
        // for each input we get, we iterate through our exploding variable
        for (Object value : schema.getIterable(variableToExplode, row)) {
            emit(value);
        }
        finishBatch(row);
    }

    @Override
    public Schema getSchema() {
        return subSchema;
    }

}
