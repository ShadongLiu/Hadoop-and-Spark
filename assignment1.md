**Assignment 1**
================

## Question 1
Briefly describe in prose your solution, both the pairs and stripes implementation. For example: how many MapReduce jobs? What are the input records? What are the intermediate key-value pairs? What are the final output records? A paragraph for each implementation is about the expected length.

**Paris implementation:** 2 MapReduce jobs  
* The input to the first job is the whole data set. But the first mapper only reads   the first 40 words for each line and ignore the duplicates and at last emit a pair   (word 1). Meanwhile, for each map function, (a_line_counter 1) is also emitted to   keep track of the number of lines it went through.  
* The first reducer is to sum up values in the pair emitted by the first mapper and   emit a pair (word sum) for each word. Also, the output of the first MapReduce job is    stored in an intermediate file (called tmp_for_pairs) which will be used later.    
* The input to the second job is still the whole data set. But this time the second   mapper reads the first 40 words for each line (ignore duplicates as well) and the   output is like ((word1, word2)  1)  
* The second reducer sum up the values in the output emitted from the second mapper and at the same time, it will load the intermediate date stored in the disk before and capture the information which is needed. The(word sum) pair holds the times that each unique word occurred and the (a_line_counter sum) tells how many lines in total. These will help to calculate the PMI for each co-occurrence pair. So the final output looks like (word 1, word 2) (PMI, sum)   


**Stripes implementation:** 2 MapReduce jobs 
* Like the Pairs implementation, the input to the first job is the whole data set.   But the first mapper only reads the first 40 words for each line and ignore the   duplicates and at last emit a pair (word 1). Meanwhile, for each map function,   (a_line_counter 1) is also emitted to keep track of the number of lines it went through.  
* Likewise, the first reducer is to sum up values in the pair emitted by the first   mapper and emit a pair (word sum) for each unique word. Also, the output of the    first MapReduce job is stored in an intermediate file (called tmp_for_stripes) which   will be used later.    
* The second MapReduce job is something different. The input to the second job is still the whole data set, and the second mapper still reads the first 40 words for each line (ignore duplicates as well). But the output this time is like (word1 word2)  1)  
* The second reducer sum up the values in the output emitted from the second mapper and at the same time, it will load the intermediate date stored in the disk before and capture the information which is needed. The(word sum) pair holds the times that each unique word occurred and the (a_line_counter sum) tells how many lines in total. These will help to calculate the PMI for each co-occurrence pair. So the final output looks like (word 1, word 2) (PMI, sum)  



## Question 2




## Question 3




## Question 4

Number of distinct PMI pairs (use threshold 10)

77198  308792 2327792



## Question 5

Pair or pairs with the highest PMI (threshold 10)

(maine, anjou)	(3.6331422, 12)  
(anjou, maine)	(3.6331422, 12) 


Pair or pairs with the lowest PMI (threshold 10)

(thy, you)	(-1.5303967, 11)  
(you, thy)	(-1.5303967, 11)



## Question 6

Three words that have the highest PMI with "tears" and "death"? And their PMI values? (threshold 10)

('tears')  
(tears, shed)	(2.1117902, 15)  
(tears, salt)	(2.052812, 11)  
(tears, eyes)	(1.165167, 23)  

('death')  
(death, father's)	(1.120252, 21)  
(death, die)	(0.7541594, 18)  
(death, life)	(0.7381346, 31)  



## Question 7

Five words that have the highest PMI with "hockey" and the times they co-occurred:

(hockey, defenceman)	(2.4180875, 153)  
(hockey, winger)	(2.3700922, 188)  
(hockey, sledge)	(2.3521852, 93)  
(hockey, goaltender)	(2.2537389, 199)  
(hockey, ice)	(2.209348, 2160)  



## Question 8

Five words that have the highest PMI with "data" and the times they co-occurred:

(data, cooling)	(2.0979044, 74)  
(data, encryption)	(2.0443728, 53)  
(data, array)	(1.992631, 50)  
(data, storage)	(1.987839, 110)  
(data, database)	(1.8893092, 99)  



