---
title: "Phonetic Functions"
date: 2018-07-20 01:25:28 UTC
parent: "SQL Functions"
---

Starting in version 1.14, Drill supports phonetic functions. Typically, you use phonetic functions in the WHERE clause of a query to find words that sound similar. For example, to find all the people named Jaime in a data source, you could issue the following query on the data source: 

       SELECT first_name
       FROM name_data
       WHERE soundex( `first_name` ) = soundex( “Jayme” );

The search would return data from rows where the first name field contains names that sound similar to Jayme, such as Jaime, Jaymee, and so on.   
 
Drill supports the following phonetic matching functions that map text to a number or string based on how a word sounds:  

- [`caverphone1(string)`]({{site.baseurl}}/docs/phonetic-functions/#caverphone1(string))  
- [`caverphone2(string)`]({{site.baseurl}}/docs/phonetic-functions/#caverphone2(string))  
- [`cologne_phonetic(string)`]({{site.baseurl}}/docs/phonetic-functions/#cologne_phonetic(string))  
- [`dm_soundex(string)`]({{site.baseurl}}/docs/phonetic-functions/#dm_soundex(string))  
- [`double_metaphone(string)`]({{site.baseurl}}/docs/phonetic-functions/#double_metaphone(string))  
- [`match_rating_encoder(string)`]({{site.baseurl}}/docs/phonetic-functions/#match_rating_encoder(string))  
- [`metaphone(string)`]({{site.baseurl}}/docs/phonetic-functions/#metaphone(string))  
- [`nysiis(string)`]({{site.baseurl}}/docs/phonetic-functions/#nysiis(string))  
- [`refined_soundex(string)`]({{site.baseurl}}/docs/phonetic-functions/#refined_soundex(string))  
- [`soundex(string)`]({{site.baseurl}}/docs/phonetic-functions/#soundex(string))  
   

## Function Descriptions  
The following sections describe each of the phonetic functions that Drill supports. Each function has a different algorithm that may work better for certain words.  

### caverphone1(string)  

An algorithm created by the Caversham Project at the University of Otago. It implements the Caverphone 1.0 algorithm.  
 
### caverphone2(string)  

An algorithm created by the Caversham Project at the University of Otago. It implements the Caverphone 2.0 algorithm.

### cologne_phonetic(string)  

Encodes a string into a Cologne Phonetic value. Implements the Kölner Phonetik (Cologne Phonetic) algorithm issued by Hans Joachim Postel in 1969. The Kölner Phonetik is a phonetic algorithm which is optimized for the German language. It is related to the well-known soundex algorithm.

### dm_soundex(string)  

Encodes a string into a Daitch-Mokotoff Soundex value. The Daitch-Mokotoff Soundex algorithm is a refinement of the Russell and American Soundex algorithms, yielding greater accuracy in matching especially Slavish and Yiddish surnames with similar pronunciation, but differences in spelling. The main differences compared to the other soundex variants are:  

- coded names are 6 digits long  
- the initial character of the name is coded 
- rules to encoded multi-character n-grams  
- multiple possible encodings for the same name (branching)

### double_metaphone(string)  

Implements the Double [Metaphone](https://en.wikipedia.org/wiki/Metaphone) phonetic algorithm and calculates a given string's Double Metaphone value.  

### match_rating_encoder(string)
Match Rating Approach Phonetic Algorithm Developed by Western Airlines in 1977.

### metaphone(string)  

Implements the [Metaphone](https://en.wikipedia.org/wiki/Metaphone) phonetic algorithm and calculates a given string's Metaphone value.  

### nysiis(string)  

Encodes a string into a NYSIIS value. NYSIIS is an encoding used to relate similar names, but can also be used as a general purpose scheme to find word with similar phonemes. The New York State Identification and Intelligence System Phonetic Code, commonly known as NYSIIS, is a phonetic algorithm devised in 1970 as part of the New York State Identification and Intelligence System (now a part of the New York State Division of Criminal Justice Services). It features an accuracy increase of 2.7% over the traditional Soundex algorithm.  

### refined_soundex(string)
Encodes a string into a Refined Soundex value. Soundex is an encoding used to relate similar names, but can also be used as a general purpose scheme to find a word with similar phonemes. 

### soundex(string)  

Encodes a string into a Soundex value. Soundex is an encoding used to relate similar names, but can also be used as a general purpose scheme to find word with similar phonemes.



