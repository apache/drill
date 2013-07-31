package org.apache.drill.exec.physical.impl;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.yammer.metrics.MetricRegistry;
import mockit.Injectable;
import mockit.NonStrictExpectations;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.FragmentRoot;
import org.apache.drill.exec.planner.PhysicalPlanReader;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.proto.ExecProtos;
import org.apache.drill.exec.rpc.user.UserServer;
import org.apache.drill.exec.server.DrillbitContext;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestComparisonFunctions {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestComparisonFunctions.class);

    DrillConfig c = DrillConfig.create();

    @Test
    public void testIntEqual(@Injectable final DrillbitContext bitContext,
                        @Injectable UserServer.UserClientConnection connection) throws Throwable{

        new NonStrictExpectations(){{
            bitContext.getMetrics(); result = new MetricRegistry("test");
            bitContext.getAllocator(); result = BufferAllocator.getAllocator(c);
        }};

        PhysicalPlanReader reader = new PhysicalPlanReader(c, c.getMapper(), CoordinationProtos.DrillbitEndpoint.getDefaultInstance());
        PhysicalPlan plan = reader.readPhysicalPlan(Files.toString(FileUtils.getResourceAsFile("/functions/intEqual.json"), Charsets.UTF_8));
        FunctionImplementationRegistry registry = new FunctionImplementationRegistry(c);
        FragmentContext context = new FragmentContext(bitContext, ExecProtos.FragmentHandle.getDefaultInstance(), connection, null, registry);
        SimpleRootExec exec = new SimpleRootExec(ImplCreator.getExec(context, (FragmentRoot) plan.getSortedOperators(false).iterator().next()));

        while(exec.next()){
            assertEquals(100, exec.getSelectionVector2().getCount());
        }

        if(context.getFailureCause() != null){
            throw context.getFailureCause();
        }

        assertTrue(!context.isFailed());

    }

    @Test
    public void testLongEqual(@Injectable final DrillbitContext bitContext,
                              @Injectable UserServer.UserClientConnection connection) throws Throwable{

        new NonStrictExpectations(){{
            bitContext.getMetrics(); result = new MetricRegistry("test");
            bitContext.getAllocator(); result = BufferAllocator.getAllocator(c);
        }};

        PhysicalPlanReader reader = new PhysicalPlanReader(c, c.getMapper(), CoordinationProtos.DrillbitEndpoint.getDefaultInstance());
        PhysicalPlan plan = reader.readPhysicalPlan(Files.toString(FileUtils.getResourceAsFile("/functions/longEqual.json"), Charsets.UTF_8));
        FunctionImplementationRegistry registry = new FunctionImplementationRegistry(c);
        FragmentContext context = new FragmentContext(bitContext, ExecProtos.FragmentHandle.getDefaultInstance(), connection, null, registry);
        SimpleRootExec exec = new SimpleRootExec(ImplCreator.getExec(context, (FragmentRoot) plan.getSortedOperators(false).iterator().next()));

        while(exec.next()){
            assertEquals(100, exec.getSelectionVector2().getCount());
        }

        if(context.getFailureCause() != null){
            throw context.getFailureCause();
        }

        assertTrue(!context.isFailed());

    }

    @Test
    public void testIntNotEqual(@Injectable final DrillbitContext bitContext,
                              @Injectable UserServer.UserClientConnection connection) throws Throwable{

        new NonStrictExpectations(){{
            bitContext.getMetrics(); result = new MetricRegistry("test");
            bitContext.getAllocator(); result = BufferAllocator.getAllocator(c);
        }};

        PhysicalPlanReader reader = new PhysicalPlanReader(c, c.getMapper(), CoordinationProtos.DrillbitEndpoint.getDefaultInstance());
        PhysicalPlan plan = reader.readPhysicalPlan(Files.toString(FileUtils.getResourceAsFile("/functions/intNotEqual.json"), Charsets.UTF_8));
        FunctionImplementationRegistry registry = new FunctionImplementationRegistry(c);
        FragmentContext context = new FragmentContext(bitContext, ExecProtos.FragmentHandle.getDefaultInstance(), connection, null, registry);
        SimpleRootExec exec = new SimpleRootExec(ImplCreator.getExec(context, (FragmentRoot) plan.getSortedOperators(false).iterator().next()));

        while(exec.next()){
            assertEquals(0, exec.getSelectionVector2().getCount());
        }

        if(context.getFailureCause() != null){
            throw context.getFailureCause();
        }

        assertTrue(!context.isFailed());

    }

    @Test
    public void testLongNotEqual(@Injectable final DrillbitContext bitContext,
                                @Injectable UserServer.UserClientConnection connection) throws Throwable{

        new NonStrictExpectations(){{
            bitContext.getMetrics(); result = new MetricRegistry("test");
            bitContext.getAllocator(); result = BufferAllocator.getAllocator(c);
        }};

        PhysicalPlanReader reader = new PhysicalPlanReader(c, c.getMapper(), CoordinationProtos.DrillbitEndpoint.getDefaultInstance());
        PhysicalPlan plan = reader.readPhysicalPlan(Files.toString(FileUtils.getResourceAsFile("/functions/longNotEqual.json"), Charsets.UTF_8));
        FunctionImplementationRegistry registry = new FunctionImplementationRegistry(c);
        FragmentContext context = new FragmentContext(bitContext, ExecProtos.FragmentHandle.getDefaultInstance(), connection, null, registry);
        SimpleRootExec exec = new SimpleRootExec(ImplCreator.getExec(context, (FragmentRoot) plan.getSortedOperators(false).iterator().next()));

        while(exec.next()){
            assertEquals(0, exec.getSelectionVector2().getCount());
        }

        if(context.getFailureCause() != null){
            throw context.getFailureCause();
        }

        assertTrue(!context.isFailed());

    }

    @Test
    public void testIntGreaterThan(@Injectable final DrillbitContext bitContext,
                                   @Injectable UserServer.UserClientConnection connection) throws Throwable{

        new NonStrictExpectations(){{
            bitContext.getMetrics(); result = new MetricRegistry("test");
            bitContext.getAllocator(); result = BufferAllocator.getAllocator(c);
        }};

        PhysicalPlanReader reader = new PhysicalPlanReader(c, c.getMapper(), CoordinationProtos.DrillbitEndpoint.getDefaultInstance());
        PhysicalPlan plan = reader.readPhysicalPlan(Files.toString(FileUtils.getResourceAsFile("/functions/intGreaterThan.json"), Charsets.UTF_8));
        FunctionImplementationRegistry registry = new FunctionImplementationRegistry(c);
        FragmentContext context = new FragmentContext(bitContext, ExecProtos.FragmentHandle.getDefaultInstance(), connection, null, registry);
        SimpleRootExec exec = new SimpleRootExec(ImplCreator.getExec(context, (FragmentRoot) plan.getSortedOperators(false).iterator().next()));

        while(exec.next()){
            assertEquals(0, exec.getSelectionVector2().getCount());
        }

        if(context.getFailureCause() != null){
            throw context.getFailureCause();
        }

        assertTrue(!context.isFailed());

    }

    @Test
    public void testLongGreaterThan(@Injectable final DrillbitContext bitContext,
                                   @Injectable UserServer.UserClientConnection connection) throws Throwable{

        new NonStrictExpectations(){{
            bitContext.getMetrics(); result = new MetricRegistry("test");
            bitContext.getAllocator(); result = BufferAllocator.getAllocator(c);
        }};

        PhysicalPlanReader reader = new PhysicalPlanReader(c, c.getMapper(), CoordinationProtos.DrillbitEndpoint.getDefaultInstance());
        PhysicalPlan plan = reader.readPhysicalPlan(Files.toString(FileUtils.getResourceAsFile("/functions/longGreaterThan.json"), Charsets.UTF_8));
        FunctionImplementationRegistry registry = new FunctionImplementationRegistry(c);
        FragmentContext context = new FragmentContext(bitContext, ExecProtos.FragmentHandle.getDefaultInstance(), connection, null, registry);
        SimpleRootExec exec = new SimpleRootExec(ImplCreator.getExec(context, (FragmentRoot) plan.getSortedOperators(false).iterator().next()));

        while(exec.next()){
            assertEquals(0, exec.getSelectionVector2().getCount());
        }

        if(context.getFailureCause() != null){
            throw context.getFailureCause();
        }

        assertTrue(!context.isFailed());

    }

    @Test
    public void testIntGreaterThanEqual(@Injectable final DrillbitContext bitContext,
                                        @Injectable UserServer.UserClientConnection connection) throws Throwable{

        new NonStrictExpectations(){{
            bitContext.getMetrics(); result = new MetricRegistry("test");
            bitContext.getAllocator(); result = BufferAllocator.getAllocator(c);
        }};

        PhysicalPlanReader reader = new PhysicalPlanReader(c, c.getMapper(), CoordinationProtos.DrillbitEndpoint.getDefaultInstance());
        PhysicalPlan plan = reader.readPhysicalPlan(Files.toString(FileUtils.getResourceAsFile("/functions/intGreaterThanEqual.json"), Charsets.UTF_8));
        FunctionImplementationRegistry registry = new FunctionImplementationRegistry(c);
        FragmentContext context = new FragmentContext(bitContext, ExecProtos.FragmentHandle.getDefaultInstance(), connection, null, registry);
        SimpleRootExec exec = new SimpleRootExec(ImplCreator.getExec(context, (FragmentRoot) plan.getSortedOperators(false).iterator().next()));

        while(exec.next()){
            assertEquals(100, exec.getSelectionVector2().getCount());
        }

        if(context.getFailureCause() != null){
            throw context.getFailureCause();
        }

        assertTrue(!context.isFailed());

    }

    @Test
    public void testLongGreaterThanEqual(@Injectable final DrillbitContext bitContext,
                                        @Injectable UserServer.UserClientConnection connection) throws Throwable{

        new NonStrictExpectations(){{
            bitContext.getMetrics(); result = new MetricRegistry("test");
            bitContext.getAllocator(); result = BufferAllocator.getAllocator(c);
        }};

        PhysicalPlanReader reader = new PhysicalPlanReader(c, c.getMapper(), CoordinationProtos.DrillbitEndpoint.getDefaultInstance());
        PhysicalPlan plan = reader.readPhysicalPlan(Files.toString(FileUtils.getResourceAsFile("/functions/longGreaterThanEqual.json"), Charsets.UTF_8));
        FunctionImplementationRegistry registry = new FunctionImplementationRegistry(c);
        FragmentContext context = new FragmentContext(bitContext, ExecProtos.FragmentHandle.getDefaultInstance(), connection, null, registry);
        SimpleRootExec exec = new SimpleRootExec(ImplCreator.getExec(context, (FragmentRoot) plan.getSortedOperators(false).iterator().next()));

        while(exec.next()){
            assertEquals(100, exec.getSelectionVector2().getCount());
        }

        if(context.getFailureCause() != null){
            throw context.getFailureCause();
        }

        assertTrue(!context.isFailed());

    }

    @Test
    public void testIntLessThan(@Injectable final DrillbitContext bitContext,
                                   @Injectable UserServer.UserClientConnection connection) throws Throwable{

        new NonStrictExpectations(){{
            bitContext.getMetrics(); result = new MetricRegistry("test");
            bitContext.getAllocator(); result = BufferAllocator.getAllocator(c);
        }};

        PhysicalPlanReader reader = new PhysicalPlanReader(c, c.getMapper(), CoordinationProtos.DrillbitEndpoint.getDefaultInstance());
        PhysicalPlan plan = reader.readPhysicalPlan(Files.toString(FileUtils.getResourceAsFile("/functions/intLessThan.json"), Charsets.UTF_8));
        FunctionImplementationRegistry registry = new FunctionImplementationRegistry(c);
        FragmentContext context = new FragmentContext(bitContext, ExecProtos.FragmentHandle.getDefaultInstance(), connection, null, registry);
        SimpleRootExec exec = new SimpleRootExec(ImplCreator.getExec(context, (FragmentRoot) plan.getSortedOperators(false).iterator().next()));

        while(exec.next()){
            assertEquals(0, exec.getSelectionVector2().getCount());
        }

        if(context.getFailureCause() != null){
            throw context.getFailureCause();
        }

        assertTrue(!context.isFailed());

    }

    @Test
    public void testLongLessThan(@Injectable final DrillbitContext bitContext,
                                @Injectable UserServer.UserClientConnection connection) throws Throwable{

        new NonStrictExpectations(){{
            bitContext.getMetrics(); result = new MetricRegistry("test");
            bitContext.getAllocator(); result = BufferAllocator.getAllocator(c);
        }};

        PhysicalPlanReader reader = new PhysicalPlanReader(c, c.getMapper(), CoordinationProtos.DrillbitEndpoint.getDefaultInstance());
        PhysicalPlan plan = reader.readPhysicalPlan(Files.toString(FileUtils.getResourceAsFile("/functions/longLessThan.json"), Charsets.UTF_8));
        FunctionImplementationRegistry registry = new FunctionImplementationRegistry(c);
        FragmentContext context = new FragmentContext(bitContext, ExecProtos.FragmentHandle.getDefaultInstance(), connection, null, registry);
        SimpleRootExec exec = new SimpleRootExec(ImplCreator.getExec(context, (FragmentRoot) plan.getSortedOperators(false).iterator().next()));

        while(exec.next()){
            assertEquals(0, exec.getSelectionVector2().getCount());
        }

        if(context.getFailureCause() != null){
            throw context.getFailureCause();
        }

        assertTrue(!context.isFailed());

    }

    @Test
    public void testIntLessThanEqual(@Injectable final DrillbitContext bitContext,
                                        @Injectable UserServer.UserClientConnection connection) throws Throwable{

        new NonStrictExpectations(){{
            bitContext.getMetrics(); result = new MetricRegistry("test");
            bitContext.getAllocator(); result = BufferAllocator.getAllocator(c);
        }};

        PhysicalPlanReader reader = new PhysicalPlanReader(c, c.getMapper(), CoordinationProtos.DrillbitEndpoint.getDefaultInstance());
        PhysicalPlan plan = reader.readPhysicalPlan(Files.toString(FileUtils.getResourceAsFile("/functions/intLessThanEqual.json"), Charsets.UTF_8));
        FunctionImplementationRegistry registry = new FunctionImplementationRegistry(c);
        FragmentContext context = new FragmentContext(bitContext, ExecProtos.FragmentHandle.getDefaultInstance(), connection, null, registry);
        SimpleRootExec exec = new SimpleRootExec(ImplCreator.getExec(context, (FragmentRoot) plan.getSortedOperators(false).iterator().next()));

        while(exec.next()){
            assertEquals(100, exec.getSelectionVector2().getCount());
        }

        if(context.getFailureCause() != null){
            throw context.getFailureCause();
        }

        assertTrue(!context.isFailed());

    }

    @Test
    public void testLongLessThanEqual(@Injectable final DrillbitContext bitContext,
                                     @Injectable UserServer.UserClientConnection connection) throws Throwable{

        new NonStrictExpectations(){{
            bitContext.getMetrics(); result = new MetricRegistry("test");
            bitContext.getAllocator(); result = BufferAllocator.getAllocator(c);
        }};

        PhysicalPlanReader reader = new PhysicalPlanReader(c, c.getMapper(), CoordinationProtos.DrillbitEndpoint.getDefaultInstance());
        PhysicalPlan plan = reader.readPhysicalPlan(Files.toString(FileUtils.getResourceAsFile("/functions/longLessThanEqual.json"), Charsets.UTF_8));
        FunctionImplementationRegistry registry = new FunctionImplementationRegistry(c);
        FragmentContext context = new FragmentContext(bitContext, ExecProtos.FragmentHandle.getDefaultInstance(), connection, null, registry);
        SimpleRootExec exec = new SimpleRootExec(ImplCreator.getExec(context, (FragmentRoot) plan.getSortedOperators(false).iterator().next()));

        while(exec.next()){
            assertEquals(100, exec.getSelectionVector2().getCount());
        }

        if(context.getFailureCause() != null){
            throw context.getFailureCause();
        }

        assertTrue(!context.isFailed());

    }

    @After
    public void tearDown() throws Exception{
        // pause to get logger to catch up.
        Thread.sleep(1000);
    }
}
