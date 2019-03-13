## Import the necessary Packages to work with Spark

from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import pandas as pd


# Find out what functions are available in spark
dir(f)

# To get more help with a module/function
help(f)
help(f.to_date)

# Though even better is to have the html docs at hand
# https://spark.apache.org/docs/latest/api/python/index.html

## Configure the spark session

spark = (
    SparkSession.builder.appName("my-spark-app")
    .config("spark.executor.memory", "5g")
    .config("spark.yarn.executor.memoryOverhead", "5g")
    .config("spark.executor.cores", 5)
    .config("spark.dynamicAllocation.enabled", "true")
    .config("spark.dynamicAllocation.maxExecutors", 3)
    .config("spark.shuffle.service.enabled", "true")
    .enableHiveSupport()
    .getOrCreate()
)

# Handy option for better output display in pandas dfs
pd.set_option("display.html.table_schema", True)

## Load the Data

# Use CSV in this session, though most big datasets will be read from HDFS (More on that later)

# To find data if its on HDFS
spark.sql("show databases").show(truncate=False)

# To find what tabels are in the database
spark.sql("use exit_checks")
spark.sql("show tables").show(truncate=False)

# Reading Data from SQL
sdf = spark.sql("SELECT * FROM exit_checks.ec_visit_history_v3")

# The returned results can look pretty ugly, so best way to explore them is to
# convert to a pandas DataFrame, which are then displayed as HTML tables
df = sdf.toPandas()
df

# Reading in Data From a CSV
rescue = spark.read.csv(
    "/dapsen/landing_zone/ls_training/rescue.csv", header=True, inferSchema=True
)

rescue.show(10, truncate=False).toPandas()

rescue.printSchema()


## Data Preprocessing

# Rename first column to RowId
rescue = rescue.withColumnRenamed("_c0", "RowId")
rescue.printSchema()

## Exercise 1 ##############
# Rename PumpTotalHours --> JobHours
# Rename PumpTotalHours --> JobHours

############################

### Convert Dates from String to Date format

# Here we make use of the additional functions in sql module we imported earlier,
# and operate on a single column using withColumn to select it.
rescue = rescue.withColumn(
    "DateTimeOfCall", f.to_date(rescue.DateTimeOfCall, "dd/MM/yyyy HH:mm")
)
rescue.printSchema()

# Find the number of Rows and Columns
n_rows = rescue.count()
n_columns = len(rescue.columns)

# We have an IncidentNumber column, lets check that this is unique.
n_unique = rescue.select("IncidentNumber").distinct().count()
n_rows == n_unique

# Filter data to just last 5 years
recent_rescue = rescue.filter(rescue.CalYear > 2012)
recent_rescue.show(10).toPandas()

## Exercise 2 ################

# Filter the recent data to find all the those with AnimalGroupParent equal to 'Fox'

##############################

# JobHours gives the number of hours the call out took, HourlyNominalCost gives the hourly cost
# Lets create a variable that calculates the total cost.

# withColumn can be used to either create a new column, or overwrite an existing one.
recent_rescue = recent_rescue.withColumn(
    "TotalCost", recent_rescue.JobHours * recent_rescue.HourlyNominalCost
)

# Lets subset the columns to just show the incident number, total cost and description
result = (
    recent_rescue.select("IncidentNumber", "TotalCost", "FinalDescription")
    .show()
    .toPandas()
)
result

# Lets investigate the highest total cost incidents
result = (
    recent_rescue.select("IncidentNumber", "TotalCost", "FinalDescription")
    .sort("TotalCost", ascending=False)
    .show()
    .toPandas()
)
result

# Seems that horses make up a lot of the more expensive calls, makes sense.

## A Note of sorting

# Confusingly, there are quite a few ways to do sorting. All the below do the same thing!
# df.sort(f.desc("age"))
# df.sort(df.age.desc())
# df.sort("age", ascending=False)
# df.orderBy(df.age.desc())

# And if you wanted to sort by multiple columns, you have a few options too.
# import pyspark.sql.functions as f
# df.sort(["age", "name"], ascending=False)
# df.orderBy(f.desc("age"), "name")
# df.orderBy(["age", "name"], ascending=[0, 1])

# Lets look at this more closely and find the average cost by AnimalGroupParent
cost_by_animal = recent_rescue.groupBy("AnimalGroupParent").agg(f.mean("TotalCost"))
cost_by_animal.printSchema()

# Lets sort by average cost and display the highest
cost_by_animal.sort("ave(TotalCost)", ascending=False).show(10).toPandas()

# Notice anything out the ordinary?

# Lets compare the number of Goats vs Horses. We can filter with multiple conditions
# using the pipe | to mean OR and & to mean AND.
goat_vs_horse = recent_rescue.filter(
    (recent_rescue.AnimalGroupParent == "Horse")
    | (recent_rescue.AnimalGroupParent == "Goat")
)
goat_vs_horse.show(10).toPandas()

# Count the number of each animal type
goat_vs_horse.groupBy("AnimalGroupParent").count().show()

# Just one expensive goat it seems!

# Lets see what that incident was.
result = (
    recent_rescue.filter(recent_rescue.AnimalGroupParent == "Goat")
    # .select('AnimalGroupParent', 'JobHours' 'FinalDescription')
    .show().toPandas()
)

# Exercise 3 ####################

# Get counts of incidents for the different animal types

# Sort the results in descending order and show the top 10

##################################

# Next lets explore the number of different animal groups
n_groups = recent_rescue.select(recent_rescue.AnimalGroupParent).distinct.count()
n_groups

# What are they?
animal_groups = recent_rescue.select(recent_rescue.AnimalGroupParent).distinct()
animal_groups.show(30).toPandas()


## Adding Processing Flags / Indicator Variables

# Lets create some extra columns to act as flags in the data to indicate rows of interest
# for downstream analysis. Typically these are set based on a particular grouping or
# calculation result we want to remember.

# For this example lets look at all the incidents that involved snakes in someones
# home (Dwelling).
recent_rescue = recent_rescue.withColumn(
    "SnakeFlag", f.when(recent_rescue.AnimalGroupParent == "Snake", 1).otherwise(0)
)

## Exercise 4 ########################

# Add an additional flag to indicate when PropertyType is 'Dwelling'.

# Subset the data to rows when both the snake and property flag is 1

## Joining Data

# Lets load in another data source to indicate the animals class, and join that onto the
# rescue data
filepath = ""
animal_type = spark.read.csv(filepath, header=True, inferSchema=True)
animal_type.printSchema()

recent_rescue.select("RowID", "AnimalGroupParent", "AnimalClass").show(10)

# As these columns names are slightly different, we can express this mapping in the
# on argument.
result = (
    rescue.join(
        animal_type,
        on=(rescue.AnimalGroupParent == animal_groups.AnimalGroup),
        how="left")
    .drop("AnimalGroup")
)

## Using SQL

# You can also swap between pyspark and sql during your workflow

# As we read this data from CSV (not from a Hive Table), we need to first register a
# temporary table to use the SQL interface. If you have read in data from an existing SQL 
# table, you don't need this step
rescue.registerTempTable('rescue_temp')

# You can then do SQL queries 
spark.sql(
    """SELECT AnimalGroupParent, count(AnimalGroupParent) FROM rescue_temp
        GROUP BY AnimalGroupParent
        ORDER BY count(AnimalGroupParent) DESC"""
).show(10)

# Exercise 5 ###################

# Using SQL, find the top 10 most expensive call outs by AnimalGroupParent aggregated 
# by the sum total of cost.

################################

