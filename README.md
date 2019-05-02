# Walkthrough of Data Manipulation in Sparklyr

**Duration: 3-4 hours**

## Overview

This course aims to introduce the CDSW environment and give an overview of working with 
Sparklyr within DAP. During the session we will focus on using Spark's DataFrame API to 
perform a number of common data manipulation tasks as we walk through the analysis of 
an example dataset. The course will include several short exercises to build familiarity, 
and also leave attendees with some more in-depth exercises to explore themselves following
the course. 

As Spark and distributed computing are themselves broad and complex topics, this 
course has been designed to be a more targeted deep dive to give participants an initial 
taste of working with CDSW and what the sparklyr library can do.  We aim to provide 
participants with enough understanding and experience to get going, and provide them 
with pointers on how to find help and further their own understanding going forwards after the course.  

## Prerequisites

We will be focusing on using R’s sparklyr library in the this course. And so to get the most out 
of it, participants should have at least a basic familiarity with R’s syntax. 

In addition we will touch briefly on the following areas, and while not essential, any prior 
experience would be beneficial.  
* SQL
*	The `dplyr` R library

## Learning Outcomes 

* Familiarity with the CDSW environment.
* How to access data on HDFS with Spark.
* Basic data manipulations with sparklyr's DataFrame API. Examples include: 
    * Reading and writing data.
    * Column creation / renaming / dropping.
    * Selecting by column and filtering rows.
    * Handling missing values
    * Group by operations and aggregations. 
    * Joining DataFrames
    * Using SQL with Spark

# Setup Instructions for Trainees

* Login to CDSW with the credentials sent out prior to the course. You should see a 
  project called 'Sparklyr Walkthrough' which contains this repository's content.

*  Within CDSW, 'fork' this project by clicking the grey button in the top right of CDSW on the project page, 
and choose your assigned username as the destination for the fork. This will create a copy of this 
project and the contained training material, and allow each participant to work at their own pace 
during and after this session. 

*  Trainees can then start their own workbench session by going to the newly copied project, and 
selecting `Open Workbench`, and choosing `R`, with Engine profile of `0.5 vCPU / 2 GiB Memory`.

* All material covered is contained within the `material/` directory. 


# Additional Setup within DAP

Beyond this course, note that some additional setup is required when working in DAP, specifically:
  * Authentication to the cluster (Account settings --> Hadoop authentication, enter Windows credentials)
  * Setting up the link to Artifactory to install R packages:
      * `r <- getOption("repos")`
      * `r['CRAN'] <- "http://<USERNAME>:<PASSWORD>@art-p-01/artifactory/list/cran-org"`
      * `options(repos = r)`
    * Where `<USERNAME>` is your Windows username and `<PASSWORD>` is your hashed password from artifactory
     (see instructions in the artifactory section of the Data Explorers Support Pages)


# Instructions for Trainers

Consult [CONTRIBUTING.md](CONTRIBUTING.md) page for details on how to setup this material on the DAP CATS 
training environment. 
