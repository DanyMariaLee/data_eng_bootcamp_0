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

2) In the class we calculated the least popular Product line for the customer who spends the most. Can you calculate what is the most popular Product line for the customer who spends less than anyone else?

3)  What is the most popular hour for our customers?

4) Filter customers who are from NSW, spend less than 500$ with Credit card and are in the age box [25-35]
