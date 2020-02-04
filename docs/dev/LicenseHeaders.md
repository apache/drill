# License Headers

Drill uses two license header checkers:

* [Apache RAT Plugin](http://creadur.apache.org/rat/apache-rat-plugin/)
* [License Maven Plugin](http://code.mycila.com/license-maven-plugin/)

## Why Two?

[Apache RAT Plugin](http://creadur.apache.org/rat/apache-rat-plugin/) is used because it is the standard license header
checker for Apache projects. 

[License Maven Plugin](http://code.mycila.com/license-maven-plugin/) performs stricter license checks and supports disallowing license headers wrapped in `/**` and `**/`. This
allows us to inforce requiring all license headers to be wrapped only in `/*` and `*/`.

## Doing License Checks

The license checks are disabled locally by default and are enabled on Github Actions. If you'd like to perform
license checks locally you can do the following:

```
 mvn license:check -Dlicense.skip=false
```

## Auto Formatting Headers

If the license checks fail and you can't figure out what's wrong with your headers, you can auto-format
your license headers with the following command:

```
mvn license:format -Dlicense.skip=false
```

This command will also add license headers to files without them.
