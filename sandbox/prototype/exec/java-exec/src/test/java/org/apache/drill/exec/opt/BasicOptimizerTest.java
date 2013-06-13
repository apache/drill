package org.apache.drill.exec.opt;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.util.FileUtils;
import org.junit.Test;

/**
 * Created with IntelliJ IDEA.
 * User: jaltekruse
 * Date: 6/12/13
 * Time: 12:09 PM
 * To change this template use File | Settings | File Templates.
 */
public class BasicOptimizerTest {

    @Test
    public void parseSimplePlan() throws Exception{
        DrillConfig c = DrillConfig.create();
        LogicalPlan plan = LogicalPlan.parse(c, FileUtils.getResourceAsString("/scan_screen_logical.json"));
        System.out.println(plan.unparse(c));
        //System.out.println( new BasicOptimizer(DrillConfig.create()).convert(plan).unparse(c.getMapper().writer()));
    }
}
