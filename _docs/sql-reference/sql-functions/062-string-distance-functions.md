---
title: "String Distance Functions"
date: 2018-07-18 23:39:26 UTC
parent: "SQL Functions"
---

String distance functions measure the difference between two strings. Starting in version 1.14, Drill supports the following string distance functions: 

- [`cosine_distance(string1,string2)`]({{site.baseurl}}/docs/string-distance-functions/#cosine_distance(string1,string2))
- [`fuzzy_score(string1,string2)`]({{site.baseurl}}/docs/string-distance-functions/#fuzzy_score(string1,string2))
- [`hamming_distance(string1,string2)`]({{site.baseurl}}/docs/string-distance-functions/#hamming_distance-(string1,string2))
- [`jaccard_distance(string1,string2)`]({{site.baseurl}}/docs/string-distance-functions/#jaccard_distance-(string1,string2))
- [`jaro_distance(string1,string2)`]({{site.baseurl}}/docs/string-distance-functions/#jaro_distance-(string1,string2))
- [`levenshtein_distance(string1,string2)`]({{site.baseurl}}/docs/string-distance-functions/#levenshtein_distance-(string1,string2))
- [`longest_common_substring_distance(string1,string2)`]({{site.baseurl}}/docs/string-distance-functions/#longest_common_substring_distance(string1,string2))  

## Syntax  

       SELECT <string-distance-function>( string1, string2 ) FROM…
  

## Example Usage

       SELECT fuzzy_score( string1, string2 ) AS fuzzy_score FROM…

## Function Descriptions  
The following sections describe each of the string distance functions that Drill supports.   

### cosine_distance(string1,string2)  
 
Calculates the cosine distance between two strings.  


### fuzzy_score(string1,string2)  

Calculates the cosine distance between two strings. A matching algorithm that is similar to the searching algorithms implemented in editors such as Sublime Text, TextMate, Atom, and others. One point is given for every matched character. Subsequent matches yield two bonus points. A higher score indicates a higher similarity. 
       

### hamming_distance (string1,string2)  

The hamming distance between two strings of equal length is the number of positions at which the corresponding symbols are different. For further explanation about the Hamming Distance, refer to http://en.wikipedia.org/wiki/Hamming_distance.   


### jaccard_distance (string1,string2)  

Measures the Jaccard distance of two sets of character sequence. [Jaccard distance](https://en.wikipedia.org/wiki/Jaccard_index) is the dissimilarity between two sets. It is the complementary of Jaccard similarity.   


### jaro_distance (string1,string2)

A similarity algorithm indicating the percentage of matched characters between two character sequences. The Jaro measure is the weighted sum of percentage of matched characters from each file and transposed characters. Winkler increased this measure for matching initial characters. This implementation is based on the [Jaro Winkler similarity algorithm](https://en.wikipedia.org/wiki/Jaro–Winkler_distance).  


### levenshtein_distance (string1,string2)
An algorithm for measuring the difference between two character sequences. This is the number of changes needed to change one sequence into another, where each change is a single character modification (deletion, insertion, or substitution).


### longest\_common\_substring_distance(string1,string2)  

Returns the length of the longest sub-sequence that two strings have in common.
Two strings that are entirely different, return a value of 0, and two strings that return a value of the commonly shared length implies that the strings are completely the same in value and position. This implementation is based on the [Longest Commons Substring algorithm](https://en.wikipedia.org/wiki/Longest_common_subsequence_problem).  
 

**Note:** Generally this algorithm is fairly inefficient, as for length m, n of the input
CharSequence's left and right respectively, the runtime of the algorithm is O(m*n).  






