# License headers and notices

Drill uses the standard Apache license header checker:

* [Apache RAT Plugin](http://creadur.apache.org/rat/apache-rat-plugin/)

## Doing license header checks

The license checks are enabled by default, bound to the `validate` build
phase. They can be disabled using the Maven argumment `-Drat.skip=true`. RAT
will accept a license header appearing in a Javadoc comment (which opens with
`/**`) but these result in noise the generated Javadoc HTML. Any instances
of such headers can be found using a pattern, e.g.

```sh
grep -Pzo '/\*\*\n \* Licensed to the Apache Software Foundation' **/*.java
```

and corrected by replacing the opening `/**` with `/*`.

## Auto formatting license headers

The Maven build does not do automatic insertion of license headers to
avoid mislicensing code under other open source licenses that may have been
brought into the Drill codebase with the exception of generated code (see
the protocol module).

## Generating the binary distribution LICENSE

Drill makes use of org.codehaus.mojo:license-maven-plugin to
generate the LICENSE file bundled with its binary distribution
(distribution/src/main/resources/LICENSE). This plugin binds to
the generate-resources build phase and combines undetectable license
notices that are manually maintained in a base Freemarket template (see
distribution/src/main/resources/licenses/) with autodetected license
notices. It can be run manually as follows.

```sh
cd distribution
mvn license:add-third-party
```

If a dependency that the plugin cannot lookup a license for has been introduced
then it will fail with an instruction to add determine the license manually
and add it to the MISSING.properties file.
