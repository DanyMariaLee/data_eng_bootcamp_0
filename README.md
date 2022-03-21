# Congratulations, you are a junior Data Engineer at Superstore!

We begin from creating cluster using Azure Databricks
On the left hand side at the tab bar: `Create -> Cluster -> enter name, change "Terminate after" to 30 min -> Change Max Workers onto 4 -> choose tech spec of cluster as per screenshot -> click Create Cluster`

![Image](./pictures/create_cluster.png)

Now we can create a Notebook
On the left hand side at the tab bar: `Create -> Notebook -> enter name, choose Scala and your cluster -> click Create`

![Image](./pictures/create_notebook.png)

Download the data for your warehouse

open this link in your browser
https://raw.githubusercontent.com/DanyMariaLee/data/main/supermarket_sales%20-%20Sheet1.csv

(This is raw CSV data of sales stats that we will be using in this class)

Right click on window "save as..." and save it as input_data.csv on your computer. We will use it in a minute.
![Image](./pictures/save_as.png)

Now let's ingest data!

Left tab -> Workspaces -> choose your workspace
Click file -> Upload data

![Image](./pictures/upload_data.png)

then drag and drop our input_data.csv

![Image](./pictures/upload_data_2.png)

And here click "copy" to get the code snippet to read the file

![Image](./pictures/upload_data_3.png)

Look at the code snippet to read data - it contains email. Please note, in the following code everywhere you should replace my email with your own, because this is the address of the file created under your user account. You can't access mine, and no one can access yours.


Paste copied code into the console
![Image](./pictures/paste_code.png)

Before we run it let's look at the line of code and learn what it actually does

```scala
val df1 = spark.read.format("csv").load("dbfs:/FileStore/shared_uploads/besselfunction@mail.ru/input_data.csv")
```
- spark - is Spark Session
- read - reads the data
- format - you provide info about the format of the data you are reading (csv, parquet)
- load parameter has to contain full path to the data
- df1 is a DataFrame

A Spark DataFrame is an integrated data structure with an easy-to-use API for simplifying distributed big data processing. 

Now we going to look at the data. Call functions `show` on your dataframe

```scala
df1.show
```

![Image](./pictures/df1_show.png)

If you look carefully you'll notice, that the column names are messed up: _c0, _c1...

Let's fix it by adding options to our data reading function
```scala
.option("header", "true")
```
so the result code snippet will be
```scala
val df1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/besselfunction@mail.ru/input_data.csv")
df1.show
```

Let's execute it and see

[Image](./pictures/df1_show_2.png)

Now we can find out how many row of data do we have
```scala
df1.count
```

Before executing the code please comment out the df1.show line, we don't need it right now.

[this screenshot is too wide and will open in new window](./pictures/df1_count.png)

Now we can see there are transactions from different cities, can we find out in how many different cities we have our stores?

```scala
df1.select("City").distinct.show

+---------+
|     City|
+---------+
| Brisbane|
|   Sydney|
|Melbourne|
+---------+
```

To have a clear idea of what columns we have we can call function df1.printSchema
```
root
 |-- CustomerID: string (nullable = true)
 |-- Product line: string (nullable = true)
 |-- Unit price: string (nullable = true)
 |-- Quantity: string (nullable = true)
 |-- Tax 5%: string (nullable = true)
 |-- Total: string (nullable = true)
 |-- Date: string (nullable = true)
 |-- Time: string (nullable = true)
 |-- Payment: string (nullable = true)
 |-- cogs: string (nullable = true)
 |-- gross margin percentage: string (nullable = true)
 |-- gross income: string (nullable = true)
 |-- Rating: string (nullable = true)

df1: org.apache.spark.sql.DataFrame = [CustomerID: string, Product line: string ... 11 more fields]
```

Now that we can calculate distinct values in any column let's find out how many unique customers we have

```scala
df1.select("CustomerID").distinct.count

res15: Long = 99
```

What is total $ each customer spend with us?
```scala
df1.groupBy(“CustomerID").sum("Total").show

AnalysisException: "Total" is not a numeric column. Aggregation function can only be applied on a numeric column.
```

This is happening because our schema has type string on column Total. We can fix that and calculate total spend using two options:
```
// Use column we created with correct type - column t

df1.withColumn("t", col(“Total").cast("integer")).groupBy("CustomerID").sum("t").show

// OR drop old column and rename new one to Total

df1.withColumn("t", col("Total").cast("integer")).drop("Total").withColumnRenamed("t", "Total").groupBy("CustomerID").sum("Total").show
```

result will be the same
```
+-----------+------+
| CustomerID|sum(t)|
+-----------+------+
|756-01-7507|  3456|
|829-34-3910|  3344|
|299-46-1805|  4175|
|617-15-4209|  2888|
|865-92-6136|  2538|
|853-23-2453|  3079|
|225-32-0908|  4110|
|232-11-3025|  5203|
|120-06-4233|  3968|
|319-50-3348|  2807|
|370-41-7321|  2602|
|382-03-4532|  2615|
|252-56-2699|  4290|
|354-25-5821|  3769|
|227-03-5010|  2686|
|315-22-5665|  4324|
|393-65-2792|  2951|
|838-78-4295|  3779|
|669-54-1719|  1652|
|692-92-5582|  2482|
+-----------+------+
only showing top 20 rows
```
