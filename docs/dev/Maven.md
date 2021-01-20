# Maven

## Introduction

The Drill project uses Apache maven as the build tool.
The project has been split up into a number of separate maven modules to isolate the various components and plugins.

## Naming new modules.

### The artifact name
The artifactId is the name that is used as the name of the output of the build.

In general the new name of an artifact should follow the pattern of `drill-<what>-<kind>`
So a `format` plugin for `something` should become `drill-something-storage`

If a module is really just a combination of other modules then let the name end with `-parent` and the module must be `<packaging>pom</packaging>`.

### The logging name
To ensure the build remains readable for the developers building the system we want to have the module show a name
that is easy to read and easy to understand which part of the project is being built.

When creating a new module please make sure the new module has a name that follows the pattern shown in the other modules.

Some basic patterns of those names:
- All start with `Drill : `
- Various parts are separated by ` : `
- A `pom` module name ends with ` : `
- A `jar` module name ends with the name of that module.

Please make sure the names are concise, unique and easy to read.
