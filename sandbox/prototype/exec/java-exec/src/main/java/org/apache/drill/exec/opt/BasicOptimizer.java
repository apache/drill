package org.apache.drill.exec.opt;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.drill.common.PlanProperties;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.logical.data.Project;
import org.apache.drill.common.logical.data.Scan;
import org.apache.drill.common.logical.data.SinkOperator;
import org.apache.drill.common.logical.data.Store;
import org.apache.drill.common.logical.data.visitors.AbstractLogicalVisitor;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.exception.OptimizerException;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.config.MockScanPOP;
import org.apache.drill.exec.physical.config.Screen;

import com.fasterxml.jackson.core.type.TypeReference;

/**
 * Created with IntelliJ IDEA.
 * User: jaltekruse
 * Date: 6/11/13
 * Time: 5:32 PM
 * To change this template use File | Settings | File Templates.
 */
public class BasicOptimizer extends Optimizer{

    private DrillConfig config;
    private QueryContext context;

    public BasicOptimizer(DrillConfig config, QueryContext context){
        this.config = config;
        this.context = context;
    }

    @Override
    public void init(DrillConfig config) {

    }

    @Override
    public PhysicalPlan optimize(OptimizationContext context, LogicalPlan plan) {
        Object obj = new Object();
        Collection<SinkOperator> roots = plan.getGraph().getRoots();
        List<PhysicalOperator> physOps = new ArrayList<PhysicalOperator>(roots.size());
        LogicalConverter converter = new LogicalConverter();
        for ( SinkOperator op : roots){
            try {
                PhysicalOperator pop  = op.accept(converter, obj);
                System.out.println(pop);
                physOps.add(pop);
            } catch (OptimizerException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            } catch (Throwable throwable) {
                throwable.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }

        PlanProperties props = new PlanProperties();
        props.type = PlanProperties.PlanType.APACHE_DRILL_PHYSICAL;
        props.version = plan.getProperties().version;
        props.generator = plan.getProperties().generator;
        return new PhysicalPlan(props, physOps);
    }

    @Override
    public void close() {

    }

    public static class BasicOptimizationContext implements OptimizationContext {

        @Override
        public int getPriority() {
            return 1;
        }
    }


    private class LogicalConverter extends AbstractLogicalVisitor<PhysicalOperator, Object, OptimizerException> {

        @Override
        public MockScanPOP visitScan(Scan scan, Object obj) throws OptimizerException {
            List<MockScanPOP.MockScanEntry> myObjects;

            try {
                if ( scan.getStorageEngine().equals("local-logs")){
                    myObjects = scan.getSelection().getListWith(config,
                            new TypeReference<ArrayList<MockScanPOP.MockScanEntry>>() {
                    });
                }
                else{
                    myObjects = new ArrayList<>();
                    MockScanPOP.MockColumn[] cols = { new MockScanPOP.MockColumn("blah", MinorType.INT, DataMode.REQUIRED,4,4,4),
                            new MockScanPOP.MockColumn("blah_2", MinorType.INT, DataMode.REQUIRED,4,4,4)};
                    myObjects.add(new MockScanPOP.MockScanEntry(50, cols));
                }
            } catch (IOException e) {
                e.printStackTrace();
                throw new OptimizerException("Error reading selection attribute of Scan node in Logical to Physical plan conversion.");
            }

            return new MockScanPOP("http://apache.org", myObjects);
        }

        @Override
        public Screen visitStore(Store store, Object obj) throws OptimizerException {
            if ( ! store.iterator().hasNext()){
                throw new OptimizerException("Store node in logical plan does not have a child.");
            }
            return new Screen(store.iterator().next().accept(this, obj), context.getCurrentEndpoint());
        }

        @Override
        public PhysicalOperator visitProject(Project project, Object obj) throws OptimizerException {
            return project.getInput().accept(this, obj);
        }
    }
}
