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

