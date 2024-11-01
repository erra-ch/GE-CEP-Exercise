## Documentation

### Data Generation
The MeterDataGenerator uses the provided date and number of hours to create a list of MeterDataTuples.
For each household (hard-coded 10) it loops through and randomizes the consumption for each hour.
The Date is converted to EpochMillis since that is what Flink will use for Watermarks.

### Average Consumption Calculation
To calculate the average we make use of an aggregate function.
It simply sums the consumption and keeps track of how many data points (count) it consists of.
When the window closes, it return the sum divided by the count.

### Consecutive Increase Pattern
The pattern uses three filters.
The first one accepts all, since we want to compare every emitted value.
The second and third compares the consumption of the current event with the event stored in the previous filter.
By using the .next keyword between the filters we can ensure strict contiguity (it will only compare consecutive events).

### Enable Parallelism and Debugging
To enable the use of parallelism we need to make use of keyed streams, so Flink knows how to divide the computation.

From the start we can simply key by Household ID since we do not want to compare between households.

To keep the aggregator simple but also emit more than the average consumption, a ProcessWindowFunction was used.
Using the AverageConsumptionTuple we can keep the Household ID as well as the window it belongs to. This allows us to key by Household ID again.

This also means that the Pattern logic can be very simple, since we guarantee that the input is split up by household but also arrives in the correct order,
sorted by the window/timestamp.

Finally, by keeping metadata throughout the data flow we could debug it at every step. Moreover, the resulting output is very readable.

### Challenges
Writing tests made the development take a bit longer but we felt it would look a bit better. It also made us more sure of our approach since we could verify 
it worked as expected. Unfortunately, we did not manage to fix the test for the Pattern and decided it was not worth spending more time on.
The output looks as expected so we still have confidence that it is properly implemented.

The data generation and pattern were the easier parts. The Aggregation logic took some time before we found the ProcessWindowFunction, which looked to achieve what we wanted.

Finally, during development we ran with parallelism 1 and through IntelliJ. It was a bit challenging to make sure it was runnable in a more
standard way (with the Flink binary local cluster). There was also some head-scratching until we found out that we needed to explicitly set the number 
of TaskSlots.

