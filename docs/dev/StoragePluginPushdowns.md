# Generic storage plugin pushdown framework

Storage plugins may support specific set of operations that have corresponding Drill operators.
The goal of this framework is to simplify the process of creating conversions from drill operators to 
plugin-specific operations (pushdowns).

## How it works

A specific plugin that uses this framework, defines a list of pushdowns it supports.
The framework will add all required rules based on this list to the planner, 
so if at any point of the planning process, the planner will find out that it is possible to do 
a specific optimization, corresponding part of the plan will be marked as plugin operation.

## How to use it

Storage plugin should define logic of converting specific operators to plugin expressions by implementing
`org.apache.drill.exec.store.plan.PluginImplementor` interface
(or extending `org.apache.drill.exec.store.plan.AbstractPluginImplementor`) class.
This implementation should have both `canImplement(RelNode)` and `implement(PluginRel)` methods
to be able to support specific pushdown. The first one checks whether it is possible to convert
the operation to plugin-specific expression, and the second one is responsible for the conversion itself.

Storage plugin should instantiate `org.apache.drill.exec.store.StoragePluginRulesSupplier` and specify for it
list of optimizations it wants to enable (it may be done configurable here), specify previously defined 
implementation of `org.apache.drill.exec.store.plan.PluginImplementor`, and `org.apache.calcite.plan.Convention`
that includes plugin name.
This `org.apache.drill.exec.store.StoragePluginRulesSupplier` should be used in 
`AbstractStoragePlugin.getOptimizerRules()` overridden method by calling 
`org.apache.drill.exec.store.StoragePluginRulesSupplier.getOptimizerRules()`

Please use `MongoStoragePlugin` as an example, it supports almost all of supported optimizations.
