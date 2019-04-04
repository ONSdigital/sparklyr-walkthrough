#----------------------------
## Introduction 
#----------------------------

# This course aims to give you a high level overview of working with the DataFrame 
# API in PySpark. We will not cover all functionality but instead focus on getting 
# up and running and performing a few common operations on an example dataset. 
#
# Prerequisites:
#  * Python, SQL

### CDSW Environment

# * Work is performed in specifically created Sessions (linux container), and attached to 
#  the project. 
# * Different languages possible (Python, R Scala)
# * Basic file browser, basic editor + REPL
# * Run code in editor with Ctr+Enter for current line or 'Run all lines' button
#
# Additional Features:
# * Comments are rendered as Markdown 
# * Terminal Access provided to the linux container

### Setup

# 'Fork' this project and select your username as the destination, this gives you your 
# own copy of this matieral to work with for this session. Environment variables have also
# been setup for you. 

# Note, some additional setup is required when working in DAP, specifically:
#  * Authentication to the cluster (Account settings --> Hadoop authentication, enter windows credentials)
#
#  * Setting Environemnt variable to tell Pyspark to use Python 3:
#  * `PYSPARK_PYTHON` = `/usr/local/bin/python3`
#  
#  * Setting up the link to Artifactory to install Python packages:
# * `PIP_INDEX_URL` = `http://<USERNAME>:<PASSWORD>@art-p-01/artifactory/api/pypi/yr-python/simple` 
# * `PIP_TRUSTED_HOST` = `art-p-01`
# * Where `<USERNAME>` is your windows username and `<PASSWORD>` is your hashed password from artifactory
#    (see instructions, artifactory section; https://share.sp.ons.statistics.gov.uk/sites/odts/wiki/Wiki/Components%20Introduction.aspx)


### Import all necessary packages to work with Spark

from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import pandas as pd

# Functions are kept in 2 places in PySpark:
# * The `pyspark.sql.functions` module
# * Attached to the `DataFrame` or `Column` object itself

# To find out the functions available in the sql module
dir(f)

### Finding more Help

# To get more help with a module/function
help(f.to_date)

# Though even better is to have the html docs at hand
# https://spark.apache.org/docs/latest/api/python/index.html

#----------------------------
## Configure the Spark Session
#----------------------------

# For this Training setup: 
#  *  max executor cores = 2
#  *  max executor memory = 2g (but this included overheads)

spark = (
    SparkSession.builder.appName("my-spark-app")
    .config("spark.executor.memory", "1536m")
    .config("spark.executor.cores", 2)
    .config("spark.dynamicAllocation.enabled", 'true')
    .config('spark.dynamicAllocation.maxExecutors', 4)
    .config('spark.shuffle.service.enabled','true')
    .enableHiveSupport()
    .getOrCreate()
)

# Handy option for better output display in pandas dfs
pd.set_option("display.html.table_schema", True)

#----------------------------
## Load the Data
#----------------------------

# Use CSV in this session, though most big datasets will be read from HDFS (More on that later)

### From HDFS

# To find data if its on HDFS
spark.sql("show databases").show(truncate=False)

# To find what tables are in the database
spark.sql("use training")
spark.sql("show tables").show(truncate=False)

# Reading Data from SQL
sdf = spark.sql("SELECT * FROM department_budget")
sdf

# Note that the table is not yet displayed
# Spark is built on the concept of transformations and actions.
#   * Transformations are lazily evaluated expressions. This form the set of 
#     instructions that will be sent to the cluster.  
#   * Actions trigger the computation to be performed on the cluster and the 
#     results accumulated locally in this session.
#
# Multiple transformations can be combined,and only when an action is triggered 
# are these executed on the cluster, after which the results are returned. 

sdf.show(10)

# The default returned results can look pretty ugly when you get a lot of columns, 
# so best way to visualise is to explore them is to convert to a pandas DataFrame,
# which are then displayed as HTML tables
df = sdf.toPandas()
df

### Reading in Data From a CSV

rescue = spark.read.csv(
    "/tmp/training/animal-rescue.csv", header=True, inferSchema=True, 
)

# The .show(n=10, truncate=True) function is an action that displays a DataFrame

rescue.show(10, truncate=False)

# It can get real messy to display everything this way with wide data, recomendations are:
# 1.  Subset to fewer columns
# 2.  convert to pandas df
# 3.  copy to text file

# Option 1 
rescue.select('DateTimeOfCall', 'FinalDescription', 'AnimalGroupParent').show(truncate=False)

# Option 2
# Warning converting to pandas will bring back all the data, so first subset the rows 
# with limit
rescue_df = rescue.limit(10).toPandas()
rescue_df

# To just get the column names and data types
rescue.printSchema()

#----------------------------
## Data Preprocessing
#----------------------------

# First, there are a lot of columns related to precise geographic position
# which we will not use in this analysis, so lets drop them for now.
rescue = rescue.drop(
    'WardCode', 
    'BoroughCode', 
    'Easting_m', 
    'Northing_m', 
    'Easting_rounded', 
    'Northing_rounded'
)
rescue.printSchema()

# Rename column to a more descriptive name
rescue = rescue.withColumnRenamed("PumpCount", "EngineCount")
rescue = rescue.withColumnRenamed("FinalDescription", "Description")
rescue = rescue.withColumnRenamed("HourlyNotionalCost(£)", "HourlyCost")
rescue = rescue.withColumnRenamed("IncidentNotionalCost(£)", "TotalCost")
# Fix a typo 
rescue = rescue.withColumnRenamed("OriginofCall", "OriginOfCall")

rescue.printSchema()

## Exercise 1 ##########################################################################

#> Rename PumpHoursTotal --> JobHours
#>
#> Rename AnimalGroupParent --> AnimalGroup


########################################################################################

### Convert Dates from String to Date format

# Here we make use of the additional functions in sql module we imported earlier,
# and operate on a single column using withColumn to select it.
rescue = rescue.withColumn(
    "DateTimeOfCall", f.to_date(rescue.DateTimeOfCall, "dd/MM/yyyy")
)
rescue.printSchema()
rescue.limit(10).toPandas()

### Filter data to just last 7 years

recent_rescue = rescue.filter(rescue.CalYear > 2012)
# Or equivilantly
recent_rescue = rescue.filter('CalYear > 2012')

recent_rescue.limit(10).toPandas()


## Exercise 2 ############################################################################

#> Filter the recent data to find all the those with AnimalGroup equal to 'Fox'



##########################################################################################


#----------------------------
## Data Exploration 
#----------------------------

### Investigate Incident Number

# Find the number of Rows and Columns
n_rows = rescue.count()
n_columns = len(rescue.columns)

# We have an IncidentNumber column, lets check that this is unique.
n_unique = rescue.select("IncidentNumber").distinct().count()
n_rows == n_unique

### Exploring Animal Groups

# Next lets explore the number of different animal groups
n_groups = recent_rescue.select("AnimalGroup").distinct().count()
n_groups

# What are they?
animal_groups = recent_rescue.select("AnimalGroup").distinct()
animal_groups.limit(30).toPandas()


### Adding Columns and Sorting

# JobHours gives the total number of hours for engines attending the incident, 
# e.g. if 2 engines attended for an hour JobHours = 2
# 
# So to get an idea of the duration of the incident we have to divide JobHours 
# by the number of engines in attendance. 
#
# Lets add another column that calculates this

# withColumn can be used to either create a new column, or overwrite an existing one.
recent_rescue = recent_rescue.withColumn(
    "IncidentDuration", recent_rescue.JobHours / recent_rescue.EngineCount
)
recent_rescue.printSchema()

# Lets subset the columns to just show the incident number, duration, total cost and description
result = (
    recent_rescue.select("IncidentNumber", "TotalCost", "IncidentDuration", "Description")
)
result.show(truncate=False)

# Lets investigate the highest total cost incidents
result = (
    recent_rescue.select("IncidentNumber", "TotalCost", "IncidentDuration", "Description")
    .sort("TotalCost", ascending=False)
)
result.limit(10).toPandas()

# Seems that horses make up a lot of the more expensive calls, makes sense.


## Exercise 3 ################################################################################

#> Sort the incidents in terms of there duration, look at the top 10 and the bottom 10.



##############################################################################################

# So it looks like we may have a lot of Missing values to account for.

## Handeling Missing values

# Lets count the number of missing values in these Columns. They have a `isNull` and 
# `isNotNull` function, which can be used with `.filter()` 

recent_rescue.filter(recent_rescue.TotalCost.isNull()).count()

recent_rescue.filter(recent_rescue.IncidentDuration.isNull()).count()

# Looks like this effects just 23 rows, for now lets row these from the dataset. We 
# could combine the two above operations. Or use the `.na.drop()` function on DataFrames
recent_rescue = recent_rescue.na.drop(subset=["TotalCost", "IncidentDuration"])

# Now lets rerun our sorting from above.
bottom_10 = (
    recent_rescue.select("IncidentNumber", "TotalCost", "IncidentDuration", "Description")
    .sort("IncidentDuration", ascending=True)
    .limit(10)
)
bottom_10.toPandas()

# Much better.


#### Side Note on sorting and Spark API

# Confusingly, there are often several ways to interact with the Spark API to get the 
# same result. Part of this is by design to allow it to support SQL like phrasing of 
# commands, and some is due to the evolution of the API.
#
# Sorting is one place you may come across this. All the below do the same thing!
# 
# ```
#   import pyspark.sql.functions as f
#   df.sort(f.desc("age"))
#   df.sort(df.age.desc())
#   df.sort("age", ascending=False)
#   df.orderBy(df.age.desc())
# ```

# And if you wanted to sort by multiple columns, you have a few options too.
# 
# ```
#   import pyspark.sql.functions as f
#   df.sort(["age", "name"], ascending=False)
#   df.orderBy(f.desc("age"), "name")
#   df.orderBy(["age", "name"], ascending=[0, 1])
# ```
#
# Advice is to be consistent in useing one approach. Here I've used the syntax 
# thats most similar to pandas with `df.sort("age", ascending=False)`

## Adding Indicator Variables/Flags

# Lets create some extra columns to act as flags in the data to indicate rows of interest
# for downstream analysis. Typically these are set based on a particular grouping or
# calculation result we want to remember.

# For this example lets look at all the incidents that involved snakes in someones
# home (Dwelling).
recent_rescue = recent_rescue.withColumn(
    "SnakeFlag", f.when(recent_rescue.AnimalGroup == "Snake", 1).otherwise(0)
)

## Exercise 4 ####################################################################################

#> Add an additional flag to indicate when PropertyCategory is 'Dwelling'.

#> Subset the data to rows when both the snake and property flag is 1

##################################################################################################

#----------------------
## Analysing By Group 
#----------------------

# Lets look at this more closely and find the average cost by AnimalGroup
cost_by_animal = recent_rescue.groupBy("AnimalGroup").agg(f.mean("TotalCost"))
cost_by_animal.printSchema()

# Lets sort by average cost and display the highest
cost_by_animal.sort("avg(TotalCost)", ascending=False).limit(10).toPandas()

# Notice anything out the ordinary?

# Lets compare the number of Goats vs Horses. We can filter with multiple conditions
# using the pipe | to mean OR and & to mean AND.
goat_vs_horse = recent_rescue.filter(
    (recent_rescue.AnimalGroup == "Horse") | (recent_rescue.AnimalGroup == "Goat")
)
goat_vs_horse.limit(10).toPandas()

# Count the number of each animal type
goat_vs_horse.groupBy("AnimalGroup").count().show()

# Lets see what that incident was.
result = (
    recent_rescue.filter(recent_rescue.AnimalGroup == "Goat")
    .select('AnimalGroup', 'JobHours', 'Description')
    .limit(10).toPandas()
)

# Just one expensive goat it seems!

### Combining Multiple Operations with Method Chaining

# Note, the above was a fair bit of work involving multiple stages. Once we 
# are more clear with what we want, several of these steps can be combined by
# chaining them together. Code written in this way gets long fast, and so its 
# encouraged to lay it out verticaly with indentation, and use parentheses to
# get python to evaluate expressions over multiple lines. 

avg_cost_by_animal = (
    recent_rescue.filter(
        (recent_rescue.AnimalGroup == "Horse") | (recent_rescue.AnimalGroup == "Goat")
    )
    .groupBy("AnimalGroup")
    .agg(
        f.avg('TotalCost'))
    .sort("avg(TotalCost)", ascending=False)
    .toPandas()
)
avg_cost_by_animal

## Exercise 5 ################################################################################

#> Get total counts of incidents for the different animal types on the full dataset


#> Sort the results in descending order and show the top 10


##############################################################################################

### A Few other Tips and Tricks

# I've rewritten the above using a few additional functions to give it more 
# flexibly, like `.isin()` and making use of mutliple functions to`.agg()`, 
#
# Note also that the alias function is a way of specifying the name of the column
# in the output 

avg_cost_by_animal = (
    recent_rescue.filter(
        recent_rescue.AnimalGroup.isin(
            "Horse", 
            "Goat", 
            "Cat", 
            "Bird"
        ))
    .groupBy("AnimalGroup")
    .agg(
        f.min('TotalCost').alias('Min'), 
        f.avg('TotalCost').alias('Mean'), 
        f.max('TotalCost').alias('Max'), 
        f.count('TotalCost').alias('Count'))
    .sort("Mean", ascending=False)
    .toPandas()
)
avg_cost_by_animal


#------------------------
## Joining Data
#------------------------

# Lets load in another data source to indicate population based on postcode, and join that 
# onto the rescue data

filepath = "/tmp/training/population_by_postcode.csv"
population = spark.read.csv(filepath, header=True, inferSchema=True)
population.printSchema()
population.show()

# We have this for each postcode, so lets aggregate before joining
outward_code_pop = (
    population.select('OutwardCode', 'Total')
    .groupBy('OutwardCode')
    .agg(f.sum('Total').alias('Population'))
)
outward_code_pop.show()

# Now lets join this based on the Postcode Outward code

# As these columns names are slightly different, we can express this mapping in the
# on argument.
rescue_with_pop = (
    rescue.join(
        outward_code_pop,
        on=rescue.PostcodeDistrict == outward_code_pop.OutwardCode,
        how="left")
    .drop("OutwardCode")
)

rescue_with_pop.limit(10).toPandas()

#---------------------------
## Using SQL
#---------------------------

# You can also swap between pyspark and sql during your workflow

# As we read this data from CSV (not from a Hive Table), we need to first register a
# temporary table to use the SQL interface. If you have read in data from an existing SQL 
# table, you don't need this step
rescue.registerTempTable('rescue')

# You can then do SQL queries 
result = spark.sql(
    """SELECT AnimalGroup, count(AnimalGroup) FROM rescue
            GROUP BY AnimalGroup
            ORDER BY count(AnimalGroup) DESC"""
)
result.limit(10).toPandas()

## Exercise 6 ###############################################################################

# >Using SQL, find the top 10 most expensive call outs by AnimalGroup aggregated 
# >by the sum total of cost.



############################################################################################

#------------------------------
## Writing Data
#------------------------------

## To HDFS 
username='dte_chrism'
rescue_with_pop.write.parquet(f'/user/{username}/rescue_with_pop.parquet')

# Also methods for CSV and JSON

# Benefits of parquet is that type schema are captured
# Its also a column format which makes loading in subsets of columns a lot faster
# Is not designed to be updated in place (imutable), so may have to delete and 
# recreate files, which requires useing the terminal commands

# Example of using hdfs tool to delete a file
!hdfs dfs -rm -r path/to/file/to/delete


## To a SQL Table (HIVE)

rescue.registerTempTable('rescue_with_pop')
spark.sql('CREATE TABLE training.my_rescue_table AS SELECT * FROM rescue_with_pop')

# Delete table
spark.sql('DROP TABLE IF EXISTS training.my_rescue_table')


## Final Exercise Questions

# How much do cats Cost the London Fire Brigade each year on average? 
# What percentage of the total cost is this? 

# Which Postcode districts reported the most/least incidents?
# When normalised by population count, which Postcode districts report the most/least incidents?

#-----------------------
## Tips and Tricks
#-----------------------

### Editor

#* double click = selct word; tripple click = select whole line

#* Tab completion

#* Run larger sections with - Shift + PgUp / PgDn then Ctr+Enter

#* Clear the Console output with Ctr+L

### IPython

#* who / whos

#* reset

#* pwd / cd / ls / mv / cp

#-----------------------
## Further Resource
#-----------------------
#
# * Pluralsight Courses
#
# * PySpark Documentation
#
# * StackOverflow
#
# * Text Books
#    *  Spark the Definitive Guide: https://www.amazon.co.uk/Spark-Definitive-Guide-Bill-Chambers/dp/1491912219
#
# * Data Explorers Slack Channel