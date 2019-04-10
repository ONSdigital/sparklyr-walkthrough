# Walkthrough of Data Manipulation in PySpark

**Duration: 2-3 hours**

## Overview

This course aims to introduce the CDSW environment and give an overview of working with 
PySpark within DAP. During the session we will focus on using Spark's DataFrame API to 
perform a number of common data manipulation tasks as we walk through the analysis of 
an example dataset. The course will include several short exercises to build familiarity, 
and also leave attendees with some more in-depth exercises to explore themselves following
the course. 

## Prerequisites

To get the most out of this course, participants should have at least a basic familiarity 
with the following:
  * Python 
  * SQL

## Learning Outcomes 

* Familiarity with the CDSW environment.
* How to access data on HDFS with Spark.
* Basic data manipulations with Pyspark's DataFrame API. Examples include: 
    * Reading and writing data.
    * Column creation / renaming / dropping.
    * Selecting by column and filtering rows.
    * Handling missing values
    * Group by operations and aggregations. 
    * Joining DataFrames
    * Using SQL with Spark

# Setup Instructions

## New Trainees

* Login to CDSW with the credentials sent out prior to the course. You should see a 
  project called 'PySpark Walkthrough' which contains this repository's content.

*  Within CDSW, 'fork' this project by clicking the grey button in the top right of CDSW on the project page, 
and choose your assigned username as the destination for the fork. This will create a copy of this 
project and the contained training material, and allow each participant to work at their own pace 
during and after this session. 

*  Trainees can then start their own workbench session by going to the newly copied project, and 
selecting `Open Workbench`, and choosing `Python 3`, with Engine profile of `0.5 vCPU / 2 GiB Memory`.

* All material covered is contained within the `material/` directory. 


## Additional Setup within DAP

Beyond this course, note that some additional setup is required when working in DAP, specifically:
  * Authentication to the cluster (Account settings --> Hadoop authentication, enter Windows credentials)
  * Setting Environment variable to tell Pyspark to use Python 3:
      * `PYSPARK_PYTHON` = `/usr/local/bin/python3`
  * Setting up the link to Artifactory to install Python packages:
    * `PIP_INDEX_URL` = `http://<USERNAME>:<PASSWORD>@art-p-01/artifactory/api/pypi/yr-python/simple` 
    * `PIP_TRUSTED_HOST` = `art-p-01`
    * Where `<USERNAME>` is your Windows username and `<PASSWORD>` is your hashed password from artifactory
     (see instructions in the artifactory section of the Data Explorers Support Pages)


## To Create this Course on the Training Environment

The following notes are to help fellow trainers recreate these scenarios on the training environment.

Repository layout:
* `material` holds the final training walkthroughs ready for teaching.
* `src` holds all resources for building the course material, including copies of the data used in training.    
* `build.py` performs all steps to prepare data and recreate material for the course under `/material`. 

The script can be run with:

```
    python3 build.py
```

### Data Sources

* [Animal Rescue Data](https://data.london.gov.uk/dataset/animal-rescue-incidents-attended-by-lfb)
* [Population by Postcode Data](https://www.nomisweb.co.uk/census/2011/postcode_headcounts_and_household_estimates) Table 1: All postcodes
