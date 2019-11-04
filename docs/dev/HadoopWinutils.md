## Hadoop Winutils

Hadoop Winutils native libraries are required to run Drill on Windows. The last version present in maven repository is 2.7.1 and is not updated anymore.
That's why Winutils version matching Hadoop version used in Drill is located in distribution/src/main/resources.

Current Winutils version: *3.2.1.*

## References
- Official wiki: [Windows Problems](https://cwiki.apache.org/confluence/display/HADOOP2/WindowsProblems).
- Winutils compiling process is described [here](https://github.com/steveloughran/winutils).
- Actual versions are being uploaded [here](https://github.com/cdarlint/winutils)