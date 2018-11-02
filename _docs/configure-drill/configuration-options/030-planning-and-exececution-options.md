---
title: "Planning and Execution Options"
date: 2018-11-02
parent: "Configuration Options"
---
You can set Drill query planning and execution options per cluster, at the
system or session level. Options set at the session level only apply to
queries that you run during the current Drill connection. Options set at the
system level affect the entire system and persist between restarts. Session
level settings override system level settings.

## Configuring Planning and Execution Options

Use the [ALTER SYSTEM]({{site.baseurl}}/docs/alter-system/) or [SET]({{site.baseurl}}/docs/set/) commands to set planning and execution options. Typically,
you set the options at the session level unless you want the setting to
persist across all sessions.

The [summary of system options]({{site.baseurl}}/docs/configuration-options-introduction) lists default values and a short description of the planning and execution options. The planning option names have a planning preface. Execution options have an exe preface. For more information, see the section, "Performance Turning".The following descriptions provide more detail on some of these options:
