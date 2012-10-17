package org.apache.drill.plan.ast;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * Represents an SSA plan.  No meaning is attached to any of the operators, nor is the
 * mechanism for connected inputs and outputs defined.
 */
public class Plan {
    private List<Op> statements = Lists.newArrayList();

    public static Plan create(Op first) {
        Plan r = new Plan();
        return r.add(first);
    }

    public Plan add(Op next) {
        if (next != null) {
            statements.add(next);
        }
        return this;
    }

    public List<Op> getStatements() {
        return statements;
    }

    /**
     * Returns a collection of the outputs in a plan that are never consumed.
     * @return A collection of Arg's that are used as outputs but never as inputs.
     */
    public Collection<Arg> getOutputs() {
        Set<Arg> outputs = Sets.newHashSet();

        for (Op op : statements) {
            outputs.addAll(op.getOutputs());
        }

        for (Op op : statements) {
            outputs.removeAll(op.getInputs());
        }

        return outputs;
    }
}
