# Congratulations, you are a junior Data Engineer at Superstore!

We begin from creating cluster using Azure Databricks
On the left hand side at the tab bar: `Create -> Cluster -> enter name, change "Terminate after" to 30 min -> Change Max Workers onto 4 -> click Create Cluster`
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
