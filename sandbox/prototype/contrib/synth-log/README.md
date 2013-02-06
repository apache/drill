log-synth
=========

The basic idea here is to have a random log generator build fairly realistic log files for analysis. The analyses specified here are fairly typical use cases for trying to figure out where the load on a web-site is coming from.

How to Run It
============

Install Java 7, maven and get this software using git.

On a mac, this can help get the right version of Java

    export JAVA_HOME=$(/usr/libexec/java_home)

Then do this to build a jar file with all dependencies included

    mvn package

Then use this to write one million log lines into the file "log" and to write the associated user database into the file "users".

    java -cp target/log-synth-0.1-SNAPSHOT-jar-with-dependencies.jar org.apache.drill.synth.Main 1M log users

This program will produce a line of output on the standard output for each 10,000 lines of log produced.  Each line will contain the number of log lines produced so far and the number of unique users in the user profile database.


The Data Source
==============
The data source here is a set of heavily biased random numbers to generate traffic sources, response times and queries. In order to give a realistic long-tail experience the data are generated using special random number generators available in the Mahout library.

There are three basic entities involved in the random process that generates these logs that are IP addresses, users and queries. Users have a basic traffic rate and a variable number of users sit behind each IP address. Queries are composed of words which are generated somewhat differently by each user. The response time for each query is determined based on the terms in the queries with a very few terms causing much longer queries than others. Each log line contains an IP address, a user cookie, a query and a response time.

Logs of various sizes can be generated using the generator tools.

The Queries
==============
The general goal of the queries is to find out what and/or who is causing long query times and where lots of traffic is coming from.

The questions we would like to answer include:

* What are the top IP addresses by request count?
* What are the top IP addresses by unique user?
* What are the most common search terms?
* What are the most common search terms in the slowest 5% of the queries?
* What is the daily number of searches, (approximate) number of unique users, (approximate) number of unique IP addresses and distribution of response times (average, min, max, 25, 50 and 75%-iles).

Methods
========
The general process for generating log lines is to select a user, possibly one we have not seen before. If the user is new, then we need to select an IP address for the user. Otherwise, we remember the IP address for each user.

Queries have an overall frequency distribution that is long-tailed, but each user has a variation on that distribution. In order to model this, we sample each user's queries from a per-user Pittman-Yor process. In order to make users have similar query term distributions, each user's query term distribution is initialized from a Pittman-Yor process that has already been sampled a number of times.

We also need to maintain an average response time per term. The response time for each query is exponentially distributed with a mean equal to the sum of the average response times for the terms. Response times for words are sampled either from an exponential distribution, from a log-gamma distribution or from a gamma distribution with a moderately low shape parameter so that we can have interestingly long tails for response time.

Users are assigned to IP addresses using a Pittman-Yor process with a discount of 0.9. This gives long-tailed distribution to the number of users per IP address. This results in 90% of all IP addresses having only a single user.

