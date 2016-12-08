---
title: "Apache Drill Contribution Guidelines"
date: 2016-12-08 20:59:28 UTC
parent: "Contribute to Drill"
---
Disclaimer: These contribution guidelines are largely based on Apache Hive
contribution guidelines.

This page describes the mechanics of _how_ to contribute software to Apache
Drill. For ideas about _what_ you might contribute, please see open tickets in
[Jira](https://issues.apache.org/jira/browse/DRILL).

## Code Contribution Steps

The following steps outline the process for contributing code to the Apache Drill project:

* [Step 1: Get the source code.]({{site.baseurl}}/docs/apache-drill-contribution-guidelines/#step-1:-get-the-source-code.)
* [Step 2: Get approval and modify the source code.]({{site.baseurl}}/docs/apache-drill-contribution-guidelines/#step-2:-get-approval-and-modify-the-source-code.)
* [Step 3: Get your code reviewed and committed to the project. ]({{site.baseurl}}/docs/apache-drill-contribution-guidelines/#step-3:-get-your-code-reviewed-and-committed-to-the-project.) 

You may also be interested in the [additional information]({{site.baseurl}}/docs/apache-drill-contribution-guidelines/#additional-information) at the end of this document. 

## Step 1: Get the source code.

First, you need the Drill source code. You can use Git to put the source code on your local drive. Most development is done on "master."

    git clone https://git-wip-us.apache.org/repos/asf/drill.git

## Step 2: Get approval and modify the source code.

Before you start, send a message to the [Drill developer mailing list](http://mail-archives.apache.org/mod_mbox/drill-dev/) or file a bug report in [JIRA](https://issues.apache.org/jira/browse/DRILL) describing your proposed changes. Doing this helps to verify that your changes will work with what others are doing and have planned for the project. Be patient, it may take folks a while to understand your requirements. For detailed designs, the Drill team uses [this design document template](https://docs.google.com/document/d/1PnBiOMV5mYBi5N6fLci-bRTva1gieCuxwlSYH9crMhU/edit?usp=sharing).

Once your suggested changes are approved, you can modify the source code and add some features using your favorite IDE.

The following sections provide tips for working on the project:

### Coding Convention

Please adhere to the points outlined below:

  * All public classes and methods should have informative [Javadoc comments](http://www.oracle.com/technetwork/java/javase/documentation/index-137868.html). Do not use @author tags.
  * Code should be formatted according to [Sun's conventions](http://www.oracle.com/technetwork/java/codeconvtoc-136057.html), with the following exceptions:
    * Indent two (2) spaces per level, not four (4).
    * Line length limit is 120 chars, instead of 80 chars.
  * Contributions should not introduce new Checkstyle violations.
  * Contributions should pass existing unit tests.
  * New unit tests should be provided to demonstrate bugs and fixes. [JUnit](http://www.junit.org) 4.1 is our test framework which has the following requirements:
    * You must implement a class that contains test methods annotated with JUnit's 4.x @Test annotation and whose class name ends with `Test`.
    * Define methods within your class whose names begin with `test`, and call JUnit's many assert methods to verify conditions; these methods will be executed when you run `mvn clean test`.

### Formatter Configuration

Setting up IDE formatters is recommended and can be done by importing the
following settings into your browser:


* IntelliJ IDEA formatter: [settings
jar](https://cwiki.apache.org/confluence/download/attachments/29687985/intellij-idea-settings.jar?version=1&modificationDate=1381928827000&api=v2)
* Eclipse: [formatter xml](https://issues.apache.org/jira/secure/attachment/12474245/eclipse_formatter_apache.xml)

### Understanding Maven

You can use the Maven Java build tool to build Drill. To get started with Maven, see the [Maven tutorial](http://maven.apache.org/guides/getting-started/maven-in-five-minutes.html).

To build Drill with Maven, run the following command:
     
    mvn clean install 
    

## Step 3: Get your code reviewed and committed to the project.  

This section describes the GitHub pull request-based review process for Apache Drill.   

{% include startnote.html %}JIRA remains the primary site for discussions on issues. We are not using the GitHub issue tracker.{% include endnote.html %}

The following steps outline the code review and commit process required to contribute new code to the Apache Drill project:  

1. The contributor writes the code that addresses a specific JIRA report as a contribution to the Apache Drill project.
2.   	The contributor organizes (squashes) their code into commits that segregate out refactoring/reorg, as necessary, to enable efficient review. The following list identifies how to combine code into commits:  
       * Combine WIP and other small commits together.
       * Address multiple JIRAs, for smaller bug fixes or enhancements, with a single commit.
       * Use separate commits to allow efficient review, separating out formatting changes or simple refactoring from core changes or additions.
       * Rebase this chain of commits on top of the current master.  
{% include startnote.html %}The discussion that is automatically copied over from GitHub adds the review process into the Apache infrastructure. The final commit ends up in the Apache Git repo, which is the critical part. As such, there is no requirement to have your intermediate work placed anywhere outside of the GitHub pull request.{% include endnote.html %}  
3. The contributor opens a pull request against the GitHub mirror, based on the branch that contains their work, which has been squashed together as described in step 2.
       * Open the pull request against this repo: https://github.com/apache/drill/
       * Mention the JIRA number in the heading of the pull request, like “DRILL-3000” to automatically link to JIRA.
       * For more information about pull requests, see [Using Pull Requests](https://help.github.com/articles/using-pull-requests/).

4. The contributor asks a committer who has experience with the affected component for review.
This information can be found in the [component owners](https://issues.apache.org/jira/browse/DRILL/?selectedTab=com.atlassian.jira.jira-projects-plugin:components-panel) section of JIRA, or by running `git blame` on the primary files changed in the pull request. For pull requests that affect multiple areas, send a message to the dev list to find a reviewer.
5. The contributor sets the Reviewer field to the assigned reviewer and marks the status as REVIEWABLE.
6. The reviewer reviews the pull request in GitHub and adds comments or a +1 to the general discussion if the pull request is ready to commit.  
       * If there are issues to address, the reviewer changes the JIRA status to "In Progress."
       * If the reviewer gives a +1, the reviewer adds a "ready-to-commit" label to the Labels field in the Jira. The contributor should continue to step 9 in this process.
7. The contributor addresses review comments. This can be done with new commits on the branch or with work made on the branch locally, squashed into the commit(s) posted in the original pull request and force pushed to the branch the pull request is based on.
8. Return to step 5.
9. A Drill committer completes the following steps to commit the patch:
       * If the master branch has moved forward since the review, rebase the branch from the pull request on the latest master and re-run tests. 
       * If all tests pass, the committer amends the last commit message in the series to include "this closes #1234", where 1234 is the pull request number, not the JIRA number. This can be done with interactive rebase. When on the branch issue:  
       
              git rebase -i HEAD^  
       * Change where it says “pick” on the line with the last commit, replacing it with “r” or “reword”. It replays the commit giving you the opportunity the change the commit message.  
       * The committer pushes the commit(s) to the Apache repo (the GitHub repo is just a read-only mirror). 
       * The committer resolves the JIRA with a message like `"Fixed in <Git commit SHA>"`.


## Additional Information

### Where is a good place to start contributing?

After getting the source code, building and running a few simple queries, one
of the simplest places to start is to implement a DrillFunc. DrillFuncs are the way that Drill expresses all scalar functions (UDF or system).  

First you can put together a JIRA for one of the DrillFuncs that we don't yet have, but should (referencing the capabilities of something like Postgres  
or SQL Server). Then try to implement one.

See this example DrillFunc:

[ComparisonFunctions.java](https://github.com/apache/drill/blob/3f93454f014196a4da198ce012b605b70081fde0/exec/java-exec/src/main/codegen/templates/ComparisonFunctions.java)

Also, you can visit the JIRA issues and implement one of those too. 

More contribution ideas are located on the [Contribution Ideas]({{ site.baseurl }}/docs/apache-drill-contribution-ideas) page.


### What are the JIRA guidelines? 

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

### See Also

  * [Apache contributor documentation](http://www.apache.org/dev/contributors.html)
  * [Apache voting documentation](http://www.apache.org/foundation/voting.html)

