---
title: "String Distance Functions"
slug: "String Distance Functions"
parent: "SQL Functions"
---

**Introduced in release**: 1.14.

Drill provides a functions for calculating a variety well known string distance metrics.  Typically, you use string distance functions in the WHERE clause of a query to measure the difference between two strings.  For example, if you want to match a street address, but do not know how to spell a street name, you could execute a query on the data source with the street addresses:

       SELECT street_address
       FROM address-data
       WHERE cosine_distance( `street_address`, “1234 North Quail Ln” ) <  0.5; 

The search would return addresses from rows with street addresses similar to 1234 North Quail Ln, such as:   

       1234 N. Quail Lane
       1234 N Quaile Lan  

Drill supports the following string distance functions.

|Function|Return type|Description|
|-|-|-|
|COSINE_DISTANCE(string1, string2)|FLOAT8|Returns the cosine distance, a measurement of the angular distance between between two strings regarded as word vectors.|
|FUZZY_SCORE(string1, string2)|FLOAT8|Returns the score from a fuzzy string matching algorithm[^1].  Higher scores indicate greater similarity.|
|HAMMING_DISTANCE(string1, string2)|FLOAT8|Returns the [Hamming distance](http://en.wikipedia.org/wiki/Hamming_distance) between two strings of equal length, a measurement of the number of positions at which corresponding characters differ.|
|JACCARD_DISTANCE(string1, string2)|FLOAT8|Returns the [Jaccard distance](https://en.wikipedia.org/wiki/Jaccard_index) between two strings regarded as unordered sets of characters, a measurement of the overlap between two sets.|
|JARO_DISTANCE(string1, string2)|FLOAT8|Returns the [Jaro-Winkler distance](https://en.wikipedia.org/wiki/Jaro–Winkler_distance), a measurement of the fraction of matching characters between two strings.|
|LEVENSHTEIN_DISTANCE(string1, string2)|FLOAT8|Returns the [Levenshtein distance](https://en.wikipedia.org/wiki/Levenshtein_distance) between two strings, a measurement of the number of single character modifications needed change one string into another.|
|LONGEST\_COMMON\_SUBSTRING_DISTANCE(string1, string2)|FLOAT8|Returns the length of the [longest common substring](https://en.wikipedia.org/wiki/Longest_common_subsequence_problem) across two strings[^2].|


[^1]: Calculates the score from a matching algorithm similar to the searching algorithms implemented in editors such as Sublime Text, TextMate, Atom, and others.  One point is given for every matched character.  Subsequent matches yield two bonus points.

[^2]: Generally this algorithm is fairly inefficient, as for length m, n of the input CharSequence's left and right respectively, the runtime of the algorithm is O(m*n).  






