#----------------------------
## Introduction 
#----------------------------

# This course aims to give you hands on experience in working with spark from R. 
# We will not cover all functionality but instead focus on getting 
# up and running and performing a few common operations on an example dataset. 
#
### Format 
#
#   * Walkthrough typical data analysis
#   * Several short exercises to build familiarity, 
#   * Some more in-depth exercises to explore after the course.
#   * You will have access to the training environment for a short time following
#     the course.
# 
### Prerequisites:
#
#  * Basic R, 
#  * SQL and `dplyr` optional but beneficial

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
#  
#  * Setting up the link to Artifactory to install R packages:
#
# ``` 
# r <- getOption("repos") 
# r['CRAN'] <- "http://<USERNAME>:<PASSWORD>@art-p-01/artifactory/list/cran-org"
# options(repos = r)
#```
# * Where `<USERNAME>` is your windows username and `<PASSWORD>` is your hashed password from artifactory
#    (see instructions, artifactory section; https://share.sp.ons.statistics.gov.uk/sites/odts/wiki/Wiki/Components%20Introduction.aspx)


### Import all necessary packages to work with Spark
install.packages("sparklyr")


library(sparklyr)
library(dplyr)


### Finding Help
?spark_config

#----------------------------
## Configure the Spark Session
#----------------------------

# For this Training setup: 
#  *  max executor cores = 2
#  *  max executor memory = 2g (this included overheads)

my_config <- spark_config()
my_config$spark.dynamicAllocation.maxExecutors <- 4
my_config$spark.executor.cores <- 2
my_config$spark.executor.memory <- "1500m"

sc <- spark_connect(
  master = "yarn-client",
  app_name = "my-spark-app",
  config = my_config)

# Check config
spark_config(sc)

# Check connection is open
spark_connection_is_open(sc)

## Sparklyr allows you to write R code to work with data in a Spark cluster. 
# The dplyr interface means that you can write the same dplyr-style R code, whether you are working with data on your machine 
# or on a Spark cluster.

#----------------------------
## Load the Data
#----------------------------

# Use CSV in this session, though most big datasets will be read from HDFS (More on that later)

### From HDFS

# To find data if its on HDFS as a HIVE table
src_databases(sc) 

# To find what tables are in the database
tbl_change_db(sc, "training")
DBI::dbGetQuery(sc, "show tables in training")


# Reading Data from SQL into a spark dataframe
sdf <- spark_read_table(sc, "department_budget", header = TRUE)
class(sdf)


# Note that the table is not yet displayed
# Spark is built on the concept of transformations and actions.
#   * **Transformations** are lazily evaluated expressions. This form the set of 
#     instructions that will be sent to the cluster.  
#   * **Actions** trigger the computation to be performed on the cluster and the 
#     results accumulated locally in this session.
#
# Multiple transformations can be combined, and only when an action is triggered 
# are these executed on the cluster, after which the results are returned. 

head(sdf)


### Reading in Data From a CSV

#### Data set: Animal rescue incidents by the London Fire Brigade.

rescue <- spark_read_csv(sc, "/tmp/training/animal-rescue.csv", header=TRUE, infer_schema=TRUE)

# To get the column names and data types
glimpse(rescue)


# The `head()` function is an action that displays a DataFrame 
head(rescue)

# It can get real messy to display everything this way with wide data, recomendations are:
# 1.  Subset to fewer columns
# 2.  convert to data.frame and use `print()`


# Option 1 
rescue %>% 
  select('DateTimeOfCall', 'FinalDescription', 'AnimalGroupParent')

# Option 2
# Warning using `collect()` will bring back all the data into R, so first subset the rows 
# with limit
rescue_df <- rescue %>% head(10) %>% collect()
print(rescue_df)


#----------------------------
## Data Preprocessing
#----------------------------

# First, there are a lot of columns related to precise geographic position
# which we will not use in this analysis, so lets drop them for now.
rescue <- rescue %>% 
  select(
     - WardCode, 
     - BoroughCode, 
     - Easting_m, 
     - Northing_m, 
     - Easting_rounded, 
     - Northing_rounded
)

glimpse(rescue)


# Rename column to a more descriptive name
rescue <- rescue %>% 
  rename(EngineCount = PumpCount,
         Description = FinalDescription,
         HourlyCost = HourlyNotionalCostGBP,
         TotalCost = IncidentNotionalCostGBP)

# Fix a typo 
rescue <- rescue %>% 
  rename(OriginOfCall = OriginofCall)

glimpse(rescue)

## Exercise 1 ##########################################################################

#> Rename PumpHoursTotal --> JobHours
#>
#> Rename AnimalGroupParent --> AnimalGroup


########################################################################################

### Convert Dates from String to Date format

rescue %>% 
  mutate(DateTimeOfCall = to_date(DateTimeOfCall, "dd/MM/yyyy")) %>% 
  collect() %>% 
  print()


### Filter data to just last 7 years

recent_rescue <- rescue %>% filter(CalYear > 2012)

recent_rescue %>% 
  head(10) %>%
  collect() %>%
  print()

## Exercise 2 ############################################################################

#> Filter the recent data to find all the those with AnimalGroup equal to 'Fox'



##########################################################################################


#----------------------------
## Data Exploration 
#----------------------------

### Investigate `IncidentNumber`

# Find the number of Rows and Columns
n_rows <- rescue %>% sdf_nrow()
n_columns <- rescue %>% sdf_ncol()

# We have an IncidentNumber column, lets check that this is unique.
n_unique <- rescue %>%  
  distinct(IncidentNumber) %>% 
  sdf_nrow()
n_rows == n_unique

### Exploring `AnimalGroups`

# Next lets explore the number of different animal groups
n_groups <- rescue %>% 
  distinct(AnimalGroup) %>% 
  sdf_nrow()

n_groups

# What are they?
animal_groups <- rescue %>% distinct(AnimalGroup)
animal_groups

### Adding Columns and Sorting

# `JobHours` gives the total number of hours for engines attending the incident, 
# e.g. if 2 engines attended for an hour JobHours = 2
# 
# So to get an idea of the duration of the incident we have to divide JobHours 
# by the number of engines in attendance. 
#
# Lets add another column that calculates this

# withColumn can be used to either create a new column, or overwrite an existing one.
rescue <- rescue %>% 
  mutate(IncidentDuration = JobHours / EngineCount)

glimpse(rescue)

# Lets subset the columns to just show the incident number, duration, total cost and description
result <-  rescue %>% 
  select(IncidentNumber, TotalCost, IncidentDuration, Description)

result %>%
  collect() %>%
  print()

# Lets investigate the highest total cost incidents
result <-  rescue %>% 
  select(IncidentNumber, TotalCost, IncidentDuration, Description) %>%
  arrange(desc(TotalCost))

result %>% 
  head(10) %>% 
  collect() %>%
  print()

# Seems that horses make up a lot of the more expensive calls, makes sense.


## Exercise 3 ################################################################################

#> Sort the incidents in terms of there duration, look at the top 10 and the bottom 10.

#> Do you notice anything strange?




##############################################################################################

# So it looks like we may have a lot of missing values to account for (which is why there are 
# a lot of blanks).

## Handeling Missing values

# Lets count the number of missing values in these columns. 
# The `is.na` function can be used with `filter()` to find missing values

rescue %>% 
  filter(is.na(TotalCost)) %>% 
  sdf_nrow()
rescue %>% 
  filter(is.na(IncidentDuration)) %>% 
  sdf_nrow()


# Looks like this effects just 38 rows, for now lets remove these from the dataset. 
rescue <- rescue  %>% 
  filter(!is.na(TotalCost) | !is.na(IncidentDuration))

# Now lets rerun our sorting from above.
bottom_10 <- rescue %>% 
  select(IncidentNumber, TotalCost, IncidentDuration, Description) %>%
   arrange(IncidentDuration) %>%
   head(10)

bottom_10

# Much better.



## Adding Indicator Variables/Flags

# Lets create some extra columns to act as flags in the data to indicate rows of interest
# for downstream analysis. Typically these are set based on a particular grouping or
# calculation result we want to remember.

# For this example lets look at all the incidents that involved snakes in someones
# home (Dwelling).
rescue <- rescue %>%
  mutate(SnakeFlag = ifelse(AnimalGroup == "Snake", 1, 0))

# Note that we can filter with multiple conditions.
# Using the pipe `|` represents OR  
# Using `&` or a comma',' represents AND. 
recent_snakes <- rescue %>% 
  filter(CalYear > 2015 & SnakeFlag == 1)
recent_snakes

## Exercise 4 ####################################################################################

#> Add an additional flag to indicate when PropertyCategory is 'Dwelling'.

#> Subset the data to rows when both the 'snake' and 'property' flag is 1


##################################################################################################

#----------------------
## Analysing By Group 
#----------------------

# Lets look at this more closely and find the average cost by AnimalGroup
cost_by_animal <- recent_rescue %>% 
  group_by(AnimalGroup) %>% 
  summarise(MeanCost = mean(TotalCost))

glimpse(cost_by_animal)

# Lets sort by average cost and display the highest
cost_by_animal <- cost_by_animal %>% 
  arrange(desc(MeanCost)) %>% 
  head(10)

cost_by_animal

# Notice anything out the ordinary?

# Lets compare the number of Goats vs Horses. We can filter with multiple conditions
# using the pipe `|` to mean OR and `&` to mean AND.
goat_vs_horse <- rescue %>% 
  filter(AnimalGroup == "Horse" | AnimalGroup == "Goat")

goat_vs_horse %>% 
  head(10) %>% 
  collect() %>% 
  print()

# Count the number of each animal type

goat_vs_horse %>% 
  group_by(AnimalGroup) %>% 
  summarise(n())

# Lets see what that incident was.
result <- rescue %>%
  filter(AnimalGroup == "Goat") %>% 
  select(AnimalGroup, JobHours, Description) %>%
  head(10)

result

# Just one expensive goat it seems!

### Combining Multiple Operations with Method Chaining

# Note, the above was a fair bit of work involving multiple stages. Once we 
# are more clear with what we want, several of these steps can be combined by
# chaining them together. Code written in this way gets long fast, and so its 
# encouraged to lay it out verticaly with indentation, and use parentheses to
# get python to evaluate expressions over multiple lines. 

avg_cost_by_animal <-
    rescue %>% 
      filter(AnimalGroup == "Horse" | AnimalGroup == "Goat") %>%
      group_by(AnimalGroup) %>%
      summarise(AvgTotal = mean(TotalCost)) %>%
      arrange(desc(AvgTotal))

avg_cost_by_animal

## Exercise 5 ################################################################################

#> Get total counts of incidents for each different animal types



#> Sort the results in descending order and show the top 10

    


##############################################################################################

### A Few Tips and Tricks

# I've rewritten the above method chaining example using a few additional functions to give it more 
# flexibly, like `%in%` and making use of mutliple functions to`summarise()` by, 
#


avg_cost_by_animal <- rescue %>%
  filter(AnimalGroup %in%  c("Horse", "Goat", "Cat", "Bird")) %>% 
  group_by(AnimalGroup) %>%
  summarise(Min = min(TotalCost),
            Mean = mean(TotalCost),
            Max = max(TotalCost),
            Count = n()) %>%
  arrange(desc(Mean))

avg_cost_by_animal  %>% 
  collect() %>% 
  print()


#------------------------
## Joining Data
#------------------------

# Lets load in another data source to indicate population based on postcode, and join that 
# onto the rescue data

filepath  <- "/tmp/training/population_by_postcode.csv"
population <- spark_read_csv(sc, filepath, header=TRUE, infer_schema=TRUE)

glimpse(population)
population 

# We have this for each postcode, so lets aggregate before joining
outward_code_pop  <- population %>%
    select(OutwardCode, Total) %>%
    group_by(OutwardCode) %>%
    summarise(Population = sum(Total))

outward_code_pop 

# Now lets join this based on the Postcode Outward code

# As these columns names are slightly different, we can express this mapping in the
# on argument.
rescue_with_pop <- rescue %>% 
  left_join(outward_code_pop, by =c("PostcodeDistrict" = "OutwardCode"))


rescue_with_pop %>%
  head(10) %>% 
  collect() %>%
  print()

#---------------------------
## Using SQL
#---------------------------

# You can also swap between sparklyr and sql during your workflow by using the DBI package

# As we read this data from CSV (not from a Hive Table), we need to first register a
# temporary table to use the SQL interface. If you have read in data from an existing SQL 
# table, you don't need this step
library(DBI)

sdf_register(rescue, 'rescue')

# You can then do SQL queries 
result <- dbGetQuery(sc, 
    "SELECT AnimalGroup, count(AnimalGroup) FROM rescue
            GROUP BY AnimalGroup
            ORDER BY count(AnimalGroup) DESC"
)
result %>% head(10)

## Exercise 6 ###############################################################################

# >Using SQL, find the top 10 most expensive call outs aggregated by the total sumed cost for 
# >each AnimalGroup. 




############################################################################################

#------------------------------
## Writing Data
#------------------------------

## To HDFS 
spark_write_parquet(rescue_with_pop, '/tmp/rescue_with_pop.parquet')

# Note that if the file exists, it will not let you overwright it. You must first delete
# it with the hdfs tool. This can be run from the console with 
system("hdfs dfs -rm -r /tmp/rescue_with_pop.parquet")

# Also note that each user and workspace will have its own home directory which you can,
# save work to.
# ````R
# username <- 'your-username-on-hue'
# spark_write_parquet(rescue_with_pop, '/user/{username}/rescue_with_pop.parquet')
# ````

# Benefits of parquet is that type schema are captured
# Its also a column format which makes loading in subsets of columns a lot faster for 
# large datasets.

# However it is not designed to be updated in place (imutable), so may have to delete and 
# recreate files, which requires using the terminal commands. It is also harder to view 
# the data in HUE, as it first needs to be loaded into a table (beyond the scope of this
# session).

# There are also methods for writing CSV and JSON formats. 

## To a SQL Table (HIVE)

sdf_register(rescue_with_pop, 'rescue_with_pop')
dbGetQuery(sc, 'CREATE TABLE training.my_rescue_table AS SELECT * FROM rescue_with_pop')

# Delete table
dbGetQuery(sc, 'DROP TABLE IF EXISTS training.my_rescue_table')


## Final Exercise Questions

# 1. How much do cats cost the London Fire Brigade each year on average? 
# 2. What percentage of their total cost (across all years) is this?
# 3. Extend the above to work out the percentage cost for each animal type.
# 4. Which Postcode districts reported the most and least incidents?
# 5. When normalised by population count, which Postcode districts report the most and least incidents (incidents per person)?
# 6. Create outputs for the above questions and save them back to hdfs as a csv file
# in your users home directory. 

#-----------------------
## Tips and Tricks
#-----------------------

### Editor

#* double click = selct word; tripple click = select whole line

#* Tab completion

#* Run larger sections with - Shift + PgUp / PgDn then Ctr+Enter

#* Clear the Console output with Ctr+L


#-----------------------
## Further Resource
#-----------------------
#
# * Pluralsight Courses
#
# * sparklyr Documentation
#
# * sparklyr cheatsheet https://ugoproto.github.io/ugo_r_doc/sparklyr.pdf 
#
# * StackOverflow
#
# * Text Books
#    *  Spark the Definitive Guide: https://www.amazon.co.uk/Spark-Definitive-Guide-Bill-Chambers/dp/1491912219
#
# * Data Explorers Slack Channel