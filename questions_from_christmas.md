### Help Christmas learn coding!

Here are the questions she left for you.

1) We calculated the maxSpenderId from our dataset

val maxSpenderId = df1.withColumn("t", col("Total").cast("integer"))
.drop("Total")
.withColumnRenamed("t", "Total")
.groupBy("CustomerID")
.sum("Total")
.withColumnRenamed("sum(Total)", "sum_total")
.orderBy(desc("sum_total"))
.select("CustomerID")
.as[String]
.collect
.head 

But the use of .head in the end of expression is not the best way to do it, if you do not have a result value this function will fail.

Can you find a better way to calculate maxSpenderId?

#### hint
after call of .collect the result type of your expression will be collection of type String. 
How can we select first element of a collection in a safe manner in Scala?

2) In the class we calculated the least popular Product line for the customer who spends the most. Can you calculate what is the most popular Product line for the customer who spends less than anyone else?

#### hint
After calculating the total amount spend by each customer in our ordering we need to make sure the smallest total spend will be on the top of the list.
*Can you try to apply the solution you found for the question 1?

3)  What is the most popular hour for our customers?

#### hint
Calculate the hour column, figure out how much our customers spend in total in the Superstore on every hour, and the hour with highest total spend would be the answer.
*we don't need to group by customers here, only by hours

4) Filter customers who are from NSW, spend less than 500$ with Credit card and are in the age box [25-35]

#### hint
we will perform following steps:
- filter the Credit card payments
- calculate total by customers
- filter those who spend less than 500 
- perform join with the reference data (our customer information from second csv file)
- filter by customers in NSW
- filter customers between 25 and 35 yo, including 25 and 35 values

*does the order of this steps matter? and if yes or no - why?

Good luck! The hints are recommendations, feel free to do it your way :-)
