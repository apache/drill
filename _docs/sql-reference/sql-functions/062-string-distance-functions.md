---
title: "String Distance Functions"
date: 2018-07-20 01:25:29 UTC
parent: "SQL Functions"
---

Drill 1.1.4 and later supports string distance functions, which you typically use in the WHERE clause of a query, to measure the difference between two strings. For example, if you want to match a street address but do not know how to spell a street name, you could issue a query on the data source by using one possible spelling of that name:

       SELECT street_address
       FROM address-data
       WHERE cosine_distance( `street_address`, “1234 North Quail Ln” ) <  0.5; 

The search would return addresses from rows with street addresses similar to 1234 North Quail Ln:   

       1234 N. Quail Lane
       1234 N Quaile Lan  

Drill supports the following string distance functions:   

- [`cosine_distance(string1,string2)`]({{site.baseurl}}/docs/string-distance-functions/#cosine_distance(string1,string2))
- [`fuzzy_score(string1,string2)`]({{site.baseurl}}/docs/string-distance-functions/#fuzzy_score(string1,string2))
- [`hamming_distance(string1,string2)`]({{site.baseurl}}/docs/string-distance-functions/#hamming_distance-(string1,string2))
- [`jaccard_distance(string1,string2)`]({{site.baseurl}}/docs/string-distance-functions/#jaccard_distance-(string1,string2))
- [`jaro_distance(string1,string2)`]({{site.baseurl}}/docs/string-distance-functions/#jaro_distance-(string1,string2))
- [`levenshtein_distance(string1,string2)`]({{site.baseurl}}/docs/string-distance-functions/#levenshtein_distance-(string1,string2))
- [`longest_common_substring_distance(string1,string2)`]({{site.baseurl}}/docs/string-distance-functions/#longest_common_substring_distance(string1,string2))  



### cosine_distance(string1,string2)  
 
Calculates the cosine distance between two strings.  


### fuzzy_score(string1,string2)  

Calculates the cosine distance between two strings, using a matching algorithm that is similar to the searching algorithms implemented in editors such as Sublime Text, TextMate, Atom, and others. Every matched character is worth one point, with subsequent matches worth two bonus points. A higher score indicates a higher similarity. 
       

### hamming_distance (string1,string2)  

Calculates the hamming distance between two strings of equal length, which is the number of positions at which the corresponding symbols are different. For further explanation about the hamming distance, refer to http://en.wikipedia.org/wiki/Hamming_distance.   


### jaccard_distance (string1,string2)  

Measures the Jaccard distance of two sets of character sequences, which is the dissimilarity between two sets. It is the complement of Jaccard similarity.   


### jaro_distance (string1,string2)

Uses a similarity algorithm indicating the percentage of matched characters between two character sequences. The Jaro measure is the weighted sum of percentage of matched characters from each file and transposed characters. 

### levenshtein_distance (string1,string2)
Uses an algorithm to measure the difference between two character sequences, the difference being the number of changes needed to change one sequence into another, where each change is a single character modification (deletion, insertion, or substitution).


### longest\_common\_substring_distance(string1,string2)  

A calculation that returns the length of the longest subsequence that two strings have in common. Two strings that are entirely different return a value of 0, and two strings that return a value of the commonly shared length implies that the strings are completely the same in value and position. 

**Note:** Generally this algorithm is fairly inefficient, as for length m, n of the input
CharSequence's left and right respectively, the runtime of the algorithm is O(m*n).  






