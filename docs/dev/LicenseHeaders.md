# License Headers

Drill uses the standadrd Apache license header checker:

* [Apache RAT Plugin](http://creadur.apache.org/rat/apache-rat-plugin/)

## Doing License Checks

The license checks are disabled locally by default and are enabled on Github Actions. If you'd like to perform
license checks locally you can do the following:

```
 mvn license:check -Dlicense.skip=false
```

## Auto Formatting Headers

The Maven build does not do automatic insertion of license headers to avoid
mislicensing code under other open source licenses that may have been brought
into the Drill codebase.
If the license checks fail and you can't figure out what's wrong with your headers, you can auto-format
your license headers with the following command:

```
mvn license:format -Dlicense.skip=false
```

This command will also add license headers to files without them.
