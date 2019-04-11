# Walkthrough of Data Manipulation in PySpark

**Duration: 2-3 hours**

## Overview

This course aims to introduce the CDSW environment and give an overview of working with 
PySpark within DAP. During the session we will focus on using Spark's DataFrame API to 
performs a number of common data manipulations tasks as we walkthough the analysis of 
an example dataset. The course will include several short exercises to build familiarity, 
and also leave attendees with some more indepth exercises to explore themselves following
the course. 

## Prerequisites

To get the most out of this course, participants should have at least a basic familiarity 
with the following:
  * Python 
  * SQL

## Learning Outcomes 

* Familiarity with the CDSW environment.
* How to access data on HDFS with Spark.
* Basic data manipulations with Pysparks DataFrame API. Examples include: 
    * Reading and writing data.
    * Column creation / renaming / droping.
    * Selecting by column and filtering rows.
    * Handeling Missing Values
    * Group by opperations and aggregations. 
    * Joining DataFrames
    * Using SQL with Spark

# Setup Instructions for Trainees

* Login to CDSW with the credentials sent out prior to the course. You should see a 
  project called 'PySpark Walkthrough' which contains this repositories content.

*  Within CDSW, 'fork' this project by clicking the grey button in the top right of CDSW on the project page, 
and choose your assigned username as the destination for the fork. This will create a copy of this 
project and the contained training material, and allow each participant work at their own pace 
during and after this session. 

*  Trainees can then start their own workbenck session by going to the newly copied project, and 
selecting `Open Workbench`, and choosing `Python 3`, with Engine profile of `0.5 CPU / 2 Gb Memory`.

* All material covered is contained within the `material/` directory. 


# Additional Setup within DAP

Beyond this course, note that some additional setup is required when working in DAP, specifically:
  * Authentication to the cluster (Account settings --> Hadoop authentication, enter windows credentials)
  * Setting Environemnt variable to tell Pyspark to use Python 3:
      * `PYSPARK_PYTHON` = `/usr/local/bin/python3`
  * Setting up the link to Artifactory to install Python packages:
    * `PIP_INDEX_URL` = `http://<USERNAME>:<PASSWORD>@art-p-01/artifactory/api/pypi/yr-python/simple` 
    * `PIP_TRUSTED_HOST` = `art-p-01`
    * Where `<USERNAME>` is your windows username and `<PASSWORD>` is your hashed password from artifactory
     (see instructions in the artifactory section of the Data Explorers Support Pages)


# Instructions for Trainers

Consult [CONTRIBUTING.md](CONTRIBUTING.md) page for details on how to setup this material on the DAP CATS 
training environment. 
