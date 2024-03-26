# FAQ on spark
 * Since Dataframe are immutable , wouldnt it add more data(redundancy) ?
   * ```
     The difference is essentially laziness. Each new DataFrame that is returned is not materialized in memory. 
     It just stores the base DataFrame and the function that should be applied to it. 
     It's essentially an execution plan for how to create some data, not the data itself.
     When it comes time to actually execute and save the result somewhere, 
     then all 1000 operations can be applied to each row in parallel, so you get 1 additional output DataFrame. 
     Spark condenses as many operations together as possible, 
     and does not materialize anything unnecessary or that hasn't been explicitly requested to be saved or cached. 
     ```
 * When to use repartion ?
   * ``` 
     If you know that you’re going to be filtering by a certain column often, 
     it can be worth repartitioning based on that column: 
     df.repartition(5, col("DEST_COUNTRY_NAME"))
     ```
   * How Repartition help in join performance ?
     * ``` 
       if you partition your data correctly prior to a join, 
       you can end up with much more efficient execution because even if a shuffle is planned, 
       if data from two different DataFrames is already located on the same machine, 
       Spark can avoid the shuffle. Experiment with some of your data and try partitioning beforehand to see 
       if you can notice the increase in speed when performing those joins
       ```
