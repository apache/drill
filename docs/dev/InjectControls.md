# InjectControls (DRILL-2383)
_This is the copy of the following doc:
https://docs.google.com/document/d/1qwsV5Uq2ftJn6ZMzjydxGl8lqeIiS4gmUTMcmdz4gDY/edit <p>_
This feature is useful for developers to test Drill resiliency and to verify behavior when an exception is thrown. Use pauses to test cancellations.
Injection sites can be added to classes that can get hold of ExecutionControls from either FragmentContext, QueryContext or OperatorContext. Injection sites can be chained.
1) Declare an injector just like a Logger:
   private final static ExecutionControlsInjector injector =
   ExecutionControlsInjector.getInjector(FragmentExecutor.class);

2) Add an injection site (unique descriptor per site):
   // ...code...
   // function signature: (controls, descriptor, exception)
   injector.injectChecked(fragmentContext.getExecutionControls(),     
   "fragment-execution-checked", IOException.class);
   // function signature: (controls, descriptor)
   injector.injectUnchecked(fragmentContext.getExecutionControls(),     
   "fragment-execution-unchecked");
   // ...code...
   OR
   // ...code...
   // function signature: (controls, descriptor, logger)
   injector.injectPause(queryContext.getExecutionControls(), "pause-run-plan",
   logger);
   // ...code...

To set controls:
> ALTER SESSION SET `drill.exec.testing.controls`='{
"injections":[ {
"siteClass": "org.apache.drill.exec.work.fragment.FragmentExecutor",
"desc": "fragment-execution-checked",
"nSkip": 0,
"nFire": 1,
"type":"exception",
"exceptionClass": "java.io.IOException"
}, {
"siteClass": "org.apache.drill.exec.work.foreman.Foreman",
"desc": "pause-run-plan",
"nSkip": 0,
"nFire": 1,
"address": "10.10.10.10",
"port": 31019,
"type":"pause"
}
] }';
> SELECT * FROM sys.memory;
...

For the above query, an IOException with fragment-execution message will be thrown on all drillbits. Also, if the foreman is on 10.10.10.10:31019, it will pause at pause-run-plan. For other examples, see TestDrillbitResilience, TestExceptionInjection and TestPauseInjection.

_NOTE:_
Controls are fired only if assertions are enabled.
Controls are fired only on one query after the alter session query.
If controls are specified, they are passed to every fragment as part of options.
address and port fields are optional.
If these fields are set, controls will be fired only on specified drillbit.
If they are not set, the injection will be fired on EVERY drillbit that reaches the site.

