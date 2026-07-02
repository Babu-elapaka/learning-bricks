# Databricks notebook source
from pyspark.sql.window import Window
from pyspark.sql import functions as F
#from pyspark.sql.functions import row_number,dense_rank,rank

sampleData = (("Olivia", 28, "Sales", 3000),
              ("Harry", 33, "Sales", 4600),
              ("Smith", 40, "Sales", 4100),
              ("Marry", 25, "Finance", 3000),
              ("Henry", 28, "Sales", 3000),
              ("Lars", 46, "Management", 3300),
              ("Jeny", 26, "Finance", 3900),
              ("Aya", 30, "Marketing", 3000),
              ("Omar", 29, "Marketing", 2000),
              ("Johnny", 39, "Sales", 4100)
              )

columns = ["Employee_Name", "Age", "Department", "Salary"]

df = spark.createDataFrame(data=sampleData, schema=columns)

windowPartitionAgg  = Window.partitionBy("Department")
df.withColumn("Avg", F.avg(F.col("Salary")).over(windowPartitionAgg))\
  .withColumn("Sum", F.sum(F.col("Salary")).over(windowPartitionAgg))\
  .withColumn("Min", F.min(F.col("Salary")).over(windowPartitionAgg))\
  .withColumn("Max", F.max(F.col("Salary")).over(windowPartitionAgg)).show()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql import functions as F
#from pyspark.sql.functions import row_number,dense_rank,rank

sampleData = (("Olivia", 28, "Sales", 3000),
              ("Harry", 33, "Sales", 4600),
              ("Smith", 40, "Sales", 4100),
              ("Marry", 25, "Finance", 3000),
              ("Henry", 28, "Sales", 3000),
              ("Lars", 46, "Management", 3300),
              ("Jeny", 26, "Finance", 3900),
              ("Aya", 30, "Marketing", 3000),
              ("Omar", 29, "Marketing", 2000),
              ("Johnny", 39, "Sales", 4100)
              )

columns = ["Employee_Name", "Age", "Department", "Salary"]

df = spark.createDataFrame(data=sampleData, schema=columns)

windowPartition = Window.partitionBy("Department").orderBy(F.col("salary").desc())
df = df.withColumn("prev_salary", F.lag("Salary").over(windowPartition))
df = df.withColumn("Next_salary", F.lead("Salary").over(windowPartition))
df.show()

# COMMAND ----------

spark.table("workspace.default.emp_dept_join").display()

# COMMAND ----------

spark.createDataFrame(data=sampleData, schema=columns)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql import functions as F
#from pyspark.sql.functions import row_number,dense_rank,rank

sampleData = (("Olivia", 28, "Sales", 3000),
              ("Harry", 33, "Sales", 4600),
              ("Smith", 40, "Sales", 4100),
              ("Marry", 25, "Finance", 3000),
              ("Henry", 28, "Sales", 3000),
              ("Lars", 46, "Management", 3300),
              ("Jeny", 26, "Finance", 3900),
              ("Aya", 30, "Marketing", 3000),
              ("Omar", 29, "Marketing", 2000),
              ("Johnny", 39, "Sales", 4100)
              )

columns = ["Employee_Name", "Age", "Department", "Salary"]

df = spark.createDataFrame(data=sampleData, schema=columns)

windowPartition = Window.partitionBy("Department").orderBy(F.col("salary").desc())
df1 = df.withColumn("sum_dept_salary", F.sum("Salary").over(windowPartition))
df.show()
df1.show()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql import functions as F
#from pyspark.sql.functions import row_number,dense_rank,rank

sampleData = (("Olivia", 28, "Sales", 3000),
              ("Harry", 33, "Sales", 4600),
              ("Smith", 40, "Sales", 4100),
              ("Marry", 25, "Finance", 3000),
              ("Henry", 28, "Sales", 3000),
              ("Lars", 46, "Management", 3300),
              ("Jeny", 26, "Finance", 3900),
              ("Aya", 30, "Marketing", 3000),
              ("Omar", 29, "Marketing", 2000),
              ("Johnny", 39, "Sales", 4100)
              )

columns = ["Employee_Name", "Age", "Department", "Salary"]

df = spark.createDataFrame(data=sampleData, schema=columns)

windowPartition = Window.partitionBy("Department").orderBy(F.col("salary").desc())
# df.printSchema()
df1 = df.withColumn("row_number", F.row_number().over(windowPartition)).withColumn("dense_rank", F.dense_rank().over(windowPartition)).withColumn("rank", F.rank().over(windowPartition))
df1.show()
df1.filter('row_number == 2').show()

# COMMAND ----------

# MAGIC %md
# MAGIC Read, Write, Join
# MAGIC * Uploading the orders.csv and sales.csv in to Volumes
# MAGIC * Read Orders.csv and primary key is ProductID
# MAGIC * Read products.csv and primary key is ProductID
# MAGIC * use Write to join these two in delta format

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

ordersdf=spark.read.format("csv").\
    option("header", "true").\
    option("inferSchema", "true").\
    load("/Volumes/mycatalog/default/myvolume/orders.csv")
#display(ordersdf)

for c in ordersdf.columns:
  ordersdf = ordersdf.withColumnRenamed(c,f"o_{c}")



# COMMAND ----------

#window functions

# COMMAND ----------

#optiomazation

# COMMAND ----------

ordersdf.where((col("o_ProductID") > 800) & (col("o_ProductID") < 900)).select("o_CustomerID","o_LineItem","o_LineItemTotal").display()

# COMMAND ----------

productsdf=spark.read.format("csv").\
    option("header", "true").\
    option("inferSchema", "true").\
    load("/Volumes/mycatalog/default/myvolume/products.csv")
#display(productsdf)

for c in productsdf.columns:
  productsdf = productsdf.withColumnRenamed(c,f"p_{c}")

# COMMAND ----------

df = productsdf.alias("pd").join(ordersdf.alias("od"),col("pd.p_ProductID")==col("od.o_ProductID").alias("orders_productid"),"inner").select("od.*","pd.*")

# COMMAND ----------

df.write.partitionBy("p_Category").format("delta").\
    mode("overwrite").\
    save("/Volumes/mycatalog/default/myvolume/porders3")

# COMMAND ----------

# DBTITLE 1,Cell 8
# Note: bucketBy() is not supported for Delta on serverless.
# Use partitionBy() instead:
df.write.partitionBy("p_Category").format("delta").\
    mode("overwrite").\
    save("/Volumes/mycatalog/default/myvolume/porders4")

# COMMAND ----------

df.write.format("delta").\
    mode("append").\
    save("/Volumes/mycatalog/default/myvolume/porders")

# COMMAND ----------

df.write.format("parquet").\
    mode("append").\
    save("/Volumes/mycatalog/default/myvolume/porders1")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC restore version '/Volumes/mycatalog/default/myvolume/porders1' as of 0

# COMMAND ----------

# MAGIC %sql
# MAGIC restore table delta.`/Volumes/mycatalog/default/myvolume/porders` version as of 1

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC describe history '/Volumes/mycatalog/default/myvolume/porders'

# COMMAND ----------

# MAGIC %sql 
# MAGIC optimize '/Volumes/mycatalog/default/myvolume/porders2'

# COMMAND ----------

# MAGIC %fs ls '/Volumes/mycatalog/default/myvolume/porders2'

# COMMAND ----------

# MAGIC %fs ls '/Volumes/mycatalog/default/myvolume/porders3'

# COMMAND ----------

# DBTITLE 1,Cell 12
spark.conf.set("delta.retentionDurationCheck.enabled" , False)

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize '/Volumes/mycatalog/default/myvolume/porders' 

# COMMAND ----------

# MAGIC %fs ls '/Volumes/mycatalog/default/myvolume/porders'

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/Volumes/mycatalog/default/myvolume/porders/_delta_log/'
