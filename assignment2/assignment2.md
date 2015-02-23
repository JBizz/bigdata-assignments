#Assignment 2
##0
>My assignment 2 project is pretty simple. The mapper breaks the lines apart and emits a pair (PairOfStrings) for every neighbor of a word (on the same line). The mapper also emits a pair with the right value being an exclamation point (which will always sort to the top). A partitioner is used to ensure pairs are sent to reducers based only on the left value of the pair. A combiner adds values together, and the reducer for this first job only writes out a pair that contains an exclamation point. After the first job completes, the total mappers counter is retreived and loaded into the second job's configuration. The next job uses the exact same mapper, combiner, and partitioner. The main difference is the reducer, this reducer uses a setup method and loads in the mapper count, and the output from the first job. The reducer then runs the equation to output a pair and the PMI for that pair.
>
>My stripes implementation is very similar in build to the pairs implementation. The mapper and combiner are repeated, there is no need for a partitioner using stripes. The first job outputs counts for each word, and the second reducer reads them back in. The second reducer adds all hashmaps together, then for each word/value pair in the hashmap runs the same PMI equation, then outputs the key and hashmap.
>
##1&2
>Runtime measured using UMIACS cluster.
>
>Pairs: With Combiner-136.521
>Without Combiner-162.321
>
>Stripes: With Combiner-82.827
>Without Combiner-84.614
>
##3
>Dinstinct PMI Pairs: 1398067
>
##4
>Highest PMI: (the, of) -> 8.562602
>
>I would imagine that these have the highest PMI due to their almost lack of independence from each other in the English language. So first off, PMI assumes independence, so words which commonly appear next to each other will get a much higher PMI. Second, they are two of the most common words, and they also commonly appear together, therefore they get a very high PMI.
##5
>1) the -> 4.561616
>
>2) a -> 4.362514
>
>3) and -> 4.147965