---
title: "Apache Drill Contribution Guidelines"
parent: "Contribute to Drill"
---
Disclaimer: These contribution guidelines are largely based on Apache Hive
contribution guidelines.

This page describes the mechanics of _how_ to contribute software to Apache
Drill. For ideas about _what_ you might contribute, please see open tickets in
[Jira](https://issues.apache.org/jira/browse/DRILL).

## How to Contribute to Drill

These guidelines include the following topics:

* Getting the source code
  * Making Changes
    * Coding Convention
    * Formatter configuration
    * Understanding Maven
    * Creating a patch
    * Applying a patch
  * Where is a good place to start contributing?
  * Contributing your work
  * JIRA Guidelines
  * See Also

## Getting the source code

First, you need the Drill source code.

Get the source code on your local drive using Git. Most development is done on
"master":

    git clone https://git-wip-us.apache.org/repos/asf/drill.git

## Making Changes

Before you start, send a message to the [Drill developer mailing list](http://mail-archives.apache.org/mod_mbox/drill-dev/), or file a bug
report in [JIRA](https://issues.apache.org/jira/browse/DRILL). Describe your
proposed changes and check that they fit in with what others are doing and
have planned for the project. Be patient, it may take folks a while to
understand your requirements.

Modify the source code and add some features using your favorite IDE.

### Coding Convention

Please take care about the following points

  * All public classes and methods should have informative [Javadoc comments](http://www.oracle.com/technetwork/java/javase/documentation/index-137868.html).
    * Do not use @author tags.
  * Code should be formatted according to [Sun's conventions](http://www.oracle.com/technetwork/java/codeconvtoc-136057.html), with one exception:
    * Indent two (2) spaces per level, not four (4).
    * Line length limit is 120 chars, instead of 80 chars.
  * Contributions should not introduce new Checkstyle violations.
  * Contributions should pass existing unit tests.
  * New unit tests should be provided to demonstrate bugs and fixes. [JUnit](http://www.junit.org) 4.1 is our test framework:
    * You must implement a class that contain test methods annotated with JUnit's 4.x @Test annotation and whose class name ends with `Test`.
    * Define methods within your class whose names begin with `test`, and call JUnit's many assert methods to verify conditions; these methods will be executed when you run `mvn clean test`.

### Formatter configuration

Setting up IDE formatters is recommended and can be done by importing the
following settings into your browser:

IntelliJ IDEA formatter: [settings
jar](https://cwiki.apache.org/confluence/download/attachments/30757399/idea-settings.jar?version=1&modificationDate=1363022308000&api=v2)

Eclipse: [formatter xml](https://issues.apache.org/jira/secure/attachment/12474245/eclipse_formatter_apache.xml)

### Understanding Maven

Drill is built by Maven, a Java build tool.

  * Good Maven tutorial: <http://maven.apache.org/guides/getting-started/maven-in-five-minutes.html>

To build Drill, run
     
    mvn clean install 
    

### Creating a patch

Check to see what files you have modified:

    git status

Add any new files with:

    git add .../MyNewClass.java
	git add .../TestMyNewClass.java
	git add .../XXXXXX.q
	git add .../XXXXXX.q.out

In order to create a patch, type (from the base directory of drill):

    git format-patch origin/master --stdout > DRILL-1234.1.patch.txt

This will report all modifications done on Drill sources on your local disk
and save them into the _DRILL-1234.1.patch.txt_ file. Read the patch file.
Make sure it includes ONLY the modifications required to fix a single issue.

Please do not:

  * reformat code unrelated to the bug being fixed: formatting changes should be separate patches/commits.
  * comment out code that is now obsolete: just remove it.
  * insert comments around each change, marking the change: folks can use subversion to figure out what's changed and by whom.
  * make things public which are not required by end users.

Please do:

  * try to adhere to the coding style of files you edit;
  * comment code whose function or rationale is not obvious;
  * update documentation (e.g., _package.html_ files, this wiki, etc.)

Updating a patch

For patch updates, our convention is to number them like
DRILL-1856.1.patch.txt, DRILL-1856.2.patch.txt, etc. And then click the
"Submit Patch" button again when a new one is uploaded; this makes sure it
gets back into the review queue. Appending '.txt' to the patch file name makes
it easy to quickly view the contents of the patch in a web browser.

### Applying a patch

To apply a patch either you generated or found from JIRA, you can issue

    git am < cool_patch.patch

if you just want to check whether the patch applies you can run patch with
--dry-run option.

  

### Review Process

  * Use Hadoop's [code review checklist](http://wiki.apache.org/hadoop/CodeReviewChecklist) as a rough guide when doing reviews.
  * In JIRA, use attach file to notify that you've submitted a patch for that issue.
  * Create a Review Request in [Review Board](https://reviews.apache.org/r/). The review request's name should start with the JIRA issue number (e.g. DRILL-XX) and should be assigned to the "drill-git" group.
  * If a committer requests changes, set the issue status to 'Resume Progress', then once you're ready, submit an updated patch with necessary fixes and then request another round of review with 'Submit Patch' again.
  * Once your patch is accepted, be sure to upload a final version which grants rights to the ASF.

## Where is a good place to start contributing?

After getting the source code, building and running a few simple queries, one
of the simplest places to start is to implement a DrillFunc.  
DrillFuncs is way that Drill express all scalar functions (UDF or system).  
First you can put together a JIRA for one of the DrillFunc's we don't yet have
but should (referencing the capabilities of something like Postgres  
or SQL Server). Then try to implement one.

One example DrillFunc:

[ComparisonFunctions.java](https://github.com/apache/drill/blob/3f93454f014196a4da198ce012b605b70081fde0/exec/java-exec/src/main/codegen/templates/ComparisonFunctions.java)

Also one can visit the JIRA issues and implement one of those too. 

More contribution ideas are located on the [Contribution Ideas]({{ site.baseurl }}/docs/apache-drill-contribution-ideas) page.

### Contributing your work

Finally, patches should be _attached_ to an issue report in
[JIRA](http://issues.apache.org/jira/browse/DRILL) via the **Attach File**
link on the issue's JIRA. Please add a comment that asks for a code review.
Please note that the attachment should be granted license to ASF for inclusion
in ASF works (as per the [Apache
License](http://www.apache.org/licenses/LICENSE-2.0).

Folks should run `mvn clean install` before submitting a patch. Tests should
all pass. If your patch involves performance optimizations, they should be
validated by benchmarks that demonstrate an improvement.

If your patch creates an incompatibility with the latest major release, then
you must set the **Incompatible change** flag on the issue's JIRA 'and' fill
in the **Release Note** field with an explanation of the impact of the
incompatibility and the necessary steps users must take.

If your patch implements a major feature or improvement, then you must fill in
the **Release Note** field on the issue's JIRA with an explanation of the
feature that will be comprehensible by the end user.

A committer should evaluate the patch within a few days and either: commit it;
or reject it with an explanation.

Please be patient. Committers are busy people too. If no one responds to your
patch after a few days, please make friendly reminders. Please incorporate
other's suggestions into your patch if you think they're reasonable. Finally,
remember that even a patch that is not committed is useful to the community.

Should your patch receive a "-1" select the **Resume Progress** on the issue's
JIRA, upload a new patch with necessary fixes, and then select the **Submit
Patch** link again.

Committers: for non-trivial changes, it is best to get another committer to
review your patches before commit. Use **Submit Patch** link like other
contributors, and then wait for a "+1" from another committer before
committing. Please also try to frequently review things in the patch queue.

## JIRA Guidelines

Please comment on issues in JIRA, making their concerns known. Please also
vote for issues that are a high priority for you.

Please refrain from editing descriptions and comments if possible, as edits
spam the mailing list and clutter JIRA's "All" display, which is otherwise
very useful. Instead, preview descriptions and comments using the preview
button (on the right) before posting them. Keep descriptions brief and save
more elaborate proposals for comments, since descriptions are included in
JIRA's automatically sent messages. If you change your mind, note this in a
new comment, rather than editing an older comment. The issue should preserve
this history of the discussion.

## See Also

  * [Apache contributor documentation](http://www.apache.org/dev/contributors.html)
  * [Apache voting documentation](http://www.apache.org/foundation/voting.html)

