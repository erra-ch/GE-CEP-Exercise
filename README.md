1.       Write a data generator in Java that creates hourly readings for 10 households. The generated data should be timestamp sorted.

2.       Write a Flink query (in Java) that reads this data and that calculates the average over tumbling windows of 6 hours in event time. Use Flink CEP to find sequences with at least 3 consecutive growing averages (on a per-household basis)

3.       The parallelism degree of the query should be an input parameter.

Provide a repository with the code and a document describing the code. The document should also describe which parts of the exercise were most challenging.
