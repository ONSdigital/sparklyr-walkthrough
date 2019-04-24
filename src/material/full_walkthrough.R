#----------------------------
## Introduction 
#----------------------------

# This course aims to give you hands on experience in working with the DataFrame 
# API in PySpark. We will not cover all functionality but instead focus on getting 
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
#  * Basic Python, 
#  * SQL and `pandas` optional but beneficial

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
#  r <- getOption("repos")
#  r['CRAN'] <- "http://<USERNAME>:<PASSWORD>@art-p-01/artifactory/list/cran-org"
#  options(repos = r)
#
# * Where `<USERNAME>` is your windows username and `<PASSWORD>` is your hashed password from artifactory
#    (see instructions, artifactory section; https://share.sp.ons.statistics.gov.uk/sites/odts/wiki/Wiki/Components%20Introduction.aspx)


### Import all necessary packages to work with Spark

install.packages("sparklyr")

#
library(sparklyr)
library(dplyr)
library(DBI)

### Finding Help


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

#----------------------------
## Load the Data
#----------------------------

# Use CSV in this session, though most big datasets will be read from HDFS (More on that later)

### From HDFS

# To find data if its on HDFS as a HIVE table
src_databases(sc)

# To find what tables are in the database
tbl_change_db(sc, "training")

dbGetQuery(sc, "show tables in training")

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

rescue = spark_read_csv(sc, 
    "/tmp/training/animal-rescue.csv", 
    header=TRUE, infer_schema=TRUE, 
)

# To just get the column names and data types
sdf_schema(rescue)

# The `head()` function is an action that displays a DataFrame 
head(rescue)

# It can get real messy to display everything this way with wide data, recomendations are:
# 1.  Subset to fewer columns
# 2.  convert to data.frame
# 3.  copy to text file

# Option 1 
rescue %>% select('DateTimeOfCall', 'FinalDescription', 'AnimalGroupParent')

# Option 2
# Warning converting to pandas will bring back all the data, so first subset the rows 
# with limit
rescue_df <- rescue %>% head(10) %>% collect()
rescue_df

# Option 3
# Use .show(truncate=False) and highlight the output in the right hand side, then copy 
# and paste to a new file. 

#----------------------------
## Data Preprocessing
#----------------------------

# First, there are a lot of columns related to precise geographic position
# which we will not use in this analysis, so lets drop them for now.
rescue = rescue %>% select(
   - WardCode, 
   - BoroughCode, 
   - Easting_m, 
   - Northing_m, 
   - Easting_rounded, 
   - Northing_rounded
)
sdf_schema(rescue)


# Rename column to a more descriptive name
rescue <- rescue %>% 
  rename(EngineCount = PumpCount,
         Description = FinalDescription,
         HourlyCost = HourlyNotionalCostGBP,
         TotalCost =IncidentNotionalCostGBP)

# Fix a typo 
rescue <- rescue %>% rename(OriginOfCall = OriginofCall)

sdf_schema(rescue)

## Exercise 1 ##########################################################################

#> Rename PumpHoursTotal --> JobHours
#>
#> Rename AnimalGroupParent --> AnimalGroup

rescue = rescue %>% rename( JobHours= PumpHoursTotal)
rescue = rescue %>% rename(AnimalGroup = AnimalGroupParent)

########################################################################################

