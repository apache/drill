# Temp Directory Utilities

The two basic temp directory classes are:

 - [DirTestWatcher](../exec/java-exec/src/test/java/org/apache/drill/test/DirTestWatcher.java)
 - [BaseDirTestWatcher](../exec/java-exec/src/test/java/org/apache/drill/test/BaseDirTestWatcher.java)
  
These classes are used to create temp directories for each of your unit tests. The advantage to using
these temp directory classes are:

 - All files are deleted after a unit test completes. This prevents a build machine being polluted with a
 bunch of unit test files.
 - Each unit test outputs its files to a unique well defined location. This makes it easy to find files
 for debugging. Also since each temp directory is unique, it prevents multiple unit test runs from interferring
 with one another on a build machine.
 
For examples on how to use these classes, please read the javadoc for each class.
