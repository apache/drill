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
import org.junit.AfterClass;
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
    public void testBigIntEqual(@Injectable final DrillbitContext bitContext,
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
    public void testFloat4Equal(@Injectable final DrillbitContext bitContext,
                                @Injectable UserServer.UserClientConnection connection) throws Throwable{

        new NonStrictExpectations(){{
            bitContext.getMetrics(); result = new MetricRegistry("test");
            bitContext.getAllocator(); result = BufferAllocator.getAllocator(c);
        }};

        PhysicalPlanReader reader = new PhysicalPlanReader(c, c.getMapper(), CoordinationProtos.DrillbitEndpoint.getDefaultInstance());
        PhysicalPlan plan = reader.readPhysicalPlan(Files.toString(FileUtils.getResourceAsFile("/functions/float4Equal.json"), Charsets.UTF_8));
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
    public void testFloat8Equal(@Injectable final DrillbitContext bitContext,
                                @Injectable UserServer.UserClientConnection connection) throws Throwable{

        new NonStrictExpectations(){{
            bitContext.getMetrics(); result = new MetricRegistry("test");
            bitContext.getAllocator(); result = BufferAllocator.getAllocator(c);
        }};

        PhysicalPlanReader reader = new PhysicalPlanReader(c, c.getMapper(), CoordinationProtos.DrillbitEndpoint.getDefaultInstance());
        PhysicalPlan plan = reader.readPhysicalPlan(Files.toString(FileUtils.getResourceAsFile("/functions/float8Equal.json"), Charsets.UTF_8));
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
    public void testBigIntNotEqual(@Injectable final DrillbitContext bitContext,
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
    public void testFloat4NotEqual(@Injectable final DrillbitContext bitContext,
                                   @Injectable UserServer.UserClientConnection connection) throws Throwable{

        new NonStrictExpectations(){{
            bitContext.getMetrics(); result = new MetricRegistry("test");
            bitContext.getAllocator(); result = BufferAllocator.getAllocator(c);
        }};

        PhysicalPlanReader reader = new PhysicalPlanReader(c, c.getMapper(), CoordinationProtos.DrillbitEndpoint.getDefaultInstance());
        PhysicalPlan plan = reader.readPhysicalPlan(Files.toString(FileUtils.getResourceAsFile("/functions/float4NotEqual.json"), Charsets.UTF_8));
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
    public void testFloat8NotEqual(@Injectable final DrillbitContext bitContext,
                                   @Injectable UserServer.UserClientConnection connection) throws Throwable{

        new NonStrictExpectations(){{
            bitContext.getMetrics(); result = new MetricRegistry("test");
            bitContext.getAllocator(); result = BufferAllocator.getAllocator(c);
        }};

        PhysicalPlanReader reader = new PhysicalPlanReader(c, c.getMapper(), CoordinationProtos.DrillbitEndpoint.getDefaultInstance());
        PhysicalPlan plan = reader.readPhysicalPlan(Files.toString(FileUtils.getResourceAsFile("/functions/float8NotEqual.json"), Charsets.UTF_8));
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
    public void testBigIntGreaterThan(@Injectable final DrillbitContext bitContext,
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
    public void testFloat4GreaterThan(@Injectable final DrillbitContext bitContext,
                                      @Injectable UserServer.UserClientConnection connection) throws Throwable{

        new NonStrictExpectations(){{
            bitContext.getMetrics(); result = new MetricRegistry("test");
            bitContext.getAllocator(); result = BufferAllocator.getAllocator(c);
        }};

        PhysicalPlanReader reader = new PhysicalPlanReader(c, c.getMapper(), CoordinationProtos.DrillbitEndpoint.getDefaultInstance());
        PhysicalPlan plan = reader.readPhysicalPlan(Files.toString(FileUtils.getResourceAsFile("/functions/float4GreaterThan.json"), Charsets.UTF_8));
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
    public void testFloat8GreaterThan(@Injectable final DrillbitContext bitContext,
                                      @Injectable UserServer.UserClientConnection connection) throws Throwable{

        new NonStrictExpectations(){{
            bitContext.getMetrics(); result = new MetricRegistry("test");
            bitContext.getAllocator(); result = BufferAllocator.getAllocator(c);
        }};

        PhysicalPlanReader reader = new PhysicalPlanReader(c, c.getMapper(), CoordinationProtos.DrillbitEndpoint.getDefaultInstance());
        PhysicalPlan plan = reader.readPhysicalPlan(Files.toString(FileUtils.getResourceAsFile("/functions/Float8GreaterThan.json"), Charsets.UTF_8));
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
    public void testBigIntGreaterThanEqual(@Injectable final DrillbitContext bitContext,
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
    public void testFloat4GreaterThanEqual(@Injectable final DrillbitContext bitContext,
                                           @Injectable UserServer.UserClientConnection connection) throws Throwable{

        new NonStrictExpectations(){{
            bitContext.getMetrics(); result = new MetricRegistry("test");
            bitContext.getAllocator(); result = BufferAllocator.getAllocator(c);
        }};

        PhysicalPlanReader reader = new PhysicalPlanReader(c, c.getMapper(), CoordinationProtos.DrillbitEndpoint.getDefaultInstance());
        PhysicalPlan plan = reader.readPhysicalPlan(Files.toString(FileUtils.getResourceAsFile("/functions/float4GreaterThanEqual.json"), Charsets.UTF_8));
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
    public void testFloat8GreaterThanEqual(@Injectable final DrillbitContext bitContext,
                                           @Injectable UserServer.UserClientConnection connection) throws Throwable{

        new NonStrictExpectations(){{
            bitContext.getMetrics(); result = new MetricRegistry("test");
            bitContext.getAllocator(); result = BufferAllocator.getAllocator(c);
        }};

        PhysicalPlanReader reader = new PhysicalPlanReader(c, c.getMapper(), CoordinationProtos.DrillbitEndpoint.getDefaultInstance());
        PhysicalPlan plan = reader.readPhysicalPlan(Files.toString(FileUtils.getResourceAsFile("/functions/float8GreaterThanEqual.json"), Charsets.UTF_8));
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
    public void testBigIntLessThan(@Injectable final DrillbitContext bitContext,
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
    public void testFloat4LessThan(@Injectable final DrillbitContext bitContext,
                                   @Injectable UserServer.UserClientConnection connection) throws Throwable{

        new NonStrictExpectations(){{
            bitContext.getMetrics(); result = new MetricRegistry("test");
            bitContext.getAllocator(); result = BufferAllocator.getAllocator(c);
        }};

        PhysicalPlanReader reader = new PhysicalPlanReader(c, c.getMapper(), CoordinationProtos.DrillbitEndpoint.getDefaultInstance());
        PhysicalPlan plan = reader.readPhysicalPlan(Files.toString(FileUtils.getResourceAsFile("/functions/float4LessThan.json"), Charsets.UTF_8));
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
    public void testFloat8LessThan(@Injectable final DrillbitContext bitContext,
                                   @Injectable UserServer.UserClientConnection connection) throws Throwable{

        new NonStrictExpectations(){{
            bitContext.getMetrics(); result = new MetricRegistry("test");
            bitContext.getAllocator(); result = BufferAllocator.getAllocator(c);
        }};

        PhysicalPlanReader reader = new PhysicalPlanReader(c, c.getMapper(), CoordinationProtos.DrillbitEndpoint.getDefaultInstance());
        PhysicalPlan plan = reader.readPhysicalPlan(Files.toString(FileUtils.getResourceAsFile("/functions/float8LessThan.json"), Charsets.UTF_8));
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
    public void testBigIntLessThanEqual(@Injectable final DrillbitContext bitContext,
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

    @Test
    public void testFloat4LessThanEqual(@Injectable final DrillbitContext bitContext,
                                        @Injectable UserServer.UserClientConnection connection) throws Throwable{

        new NonStrictExpectations(){{
            bitContext.getMetrics(); result = new MetricRegistry("test");
            bitContext.getAllocator(); result = BufferAllocator.getAllocator(c);
        }};

        PhysicalPlanReader reader = new PhysicalPlanReader(c, c.getMapper(), CoordinationProtos.DrillbitEndpoint.getDefaultInstance());
        PhysicalPlan plan = reader.readPhysicalPlan(Files.toString(FileUtils.getResourceAsFile("/functions/float4LessThanEqual.json"), Charsets.UTF_8));
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
    public void testFloat8LessThanEqual(@Injectable final DrillbitContext bitContext,
                                        @Injectable UserServer.UserClientConnection connection) throws Throwable{

        new NonStrictExpectations(){{
            bitContext.getMetrics(); result = new MetricRegistry("test");
            bitContext.getAllocator(); result = BufferAllocator.getAllocator(c);
        }};

        PhysicalPlanReader reader = new PhysicalPlanReader(c, c.getMapper(), CoordinationProtos.DrillbitEndpoint.getDefaultInstance());
        PhysicalPlan plan = reader.readPhysicalPlan(Files.toString(FileUtils.getResourceAsFile("/functions/float8LessThanEqual.json"), Charsets.UTF_8));
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

    @AfterClass
    public static void tearDown() throws Exception{
        // pause to get logger to catch up.
        Thread.sleep(1000);
    }
}
