Steps for execution.
**********************

1. Place the input file in the hdfs.

2. Download the jar file submitted in the assignment.

3. To execute use the following command

hadoop jar <path/Page_Rank.jar> Page_Rank <Input file path> <Output Path>

**************************************************************************
This program utilizes three jobs to execute the page rank algorithm.

The first job generates the link graph, the second is the iterator job and
the third job is the cleanup job.
**************************************************************************

Input and Output formats of the files processed by the jobs.
**************************************************************************

INITIAL JOB
------------
Input format of MAPPER  <title>....<text>...[[link1]][[link2]]....</text>...</title>

Output format of MAPPER --- KEY title#####links

Input format of Reducer --- KEY title#####link1$$$link2...

Output format of Reducer --- title#####rank link1$$$link2$$$...

ITERATOR JOB
-------------
Input format of MAPPER --- title#####rank link1$$$link2$$$...

Output format of MAPPER --- title	pagerank#####link1$$$link2$$$....

Input format of Reducer --- title	pagerank#####link1$$$link2$$$....

Output format of Reducer --- title#####pagerank	link1$$$link2....

CLEANUP JOB
-------------
Input format of MAPPER --- title#####pagerank	link1$$$link2....

Output format of MAPPER --- -pagerank	title

Input format of Reducer --- -pagerank	title

Output format of Reducer --- title	pagerank
