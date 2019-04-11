# Trainer and Contributing Guide

The following notes are to help fellow trainers recreate these scenarios on the training environment, 
as well as build upon or extend the material itself.

## Repository Layout 

* `material` holds the final training walkthroughs ready for teaching.
* `src` holds all resources for building the course material, including copies of the data used in training.    
* `dodo.py` Automation script used by [`doit`](http://pydoit.org/) to prepare data and recreate material for the course under `/material`. 

## Dependencies

* [`doit`](http://pydoit.org/) library is used to automate the setup
* [`requests`](http://pydoit.org/) library for downloading datasets

## Quick Start Setup 

Install requirements via the termianl with:
```
    pip3 install -r src/requirements.txt
```

Then run the automated setup with:
```
    doit
```

The above will:

* Clean raw datasets
* Transfer datasets to HDFS on the Cloudera stack.
* Seperate exercise answers from the full walkthrough and save them seperately.

# Writing/Extending Material 

The material is designed to be taught within CDSW by walking though an example analysis. CDSW renders comments 
as Markdown in the active session output, and this is used to highlight the key points and create the narative
as the course progresses. 

## Exercises

These are included at regular intervals to check participants can follow along. An example of the format, 
along with an answer is shown below.  

```python
## Exercise 2 ############################################################################

#> Filter the recent data to find all the those with AnimalGroup equal to 'Fox'

recent_foxes = recent_rescue.filter(recent_rescue.AnimalGroup == 'Fox')
# Or equivilantly
recent_foxes = recent_rescue.filter('AnimalGroup == "Fox"')

recent_foxes.limit(10).toPandas()
recent_foxes.select('DateTimeOfCall', 'Description').show(truncate=False)

##########################################################################################

```

As the walkthough progressively builds up an analysis, there may be times where there's a 
dependency between the exercise and the code that comes before or after. Therefore to test 
everything runs as expected end to end, both exercises and answers are included in one script,
and this is stored in `src/material/full_walkthough.py`. 

For teaching however, we need a way to seperate out the answers to a seperate file so 
participants can have a go themselves. This is the purpose of the functions in 
`src/scripts/process.py` which strip out the exercise answers and save a new version of 
the files under `material/walkthrough.py`. This function also keeps a copy of just the 
exercises themselves with answers, and saves them to `material/exercise/answers.py`

In order for the above filtering process to work:
* The instrctions within the exercise itself need to be a prefixed with a markdown quote (lines starting with a `>`). that way they are
kept and not removed. 
* The first line of the exercise must start with `## Exercise` 
* The last line must be at least 3 or more `#` characters only.

An example of the above exercise after the filtering process is shown below:

```python
## Exercise 2 ############################################################################

#> Filter the recent data to find all the those with AnimalGroup equal to 'Fox'



##########################################################################################

```

Note that any blank lines are kept as part of this process. 



## Data Sources Used

Copies of the data are included in the repo, and links to original sources are below for reference:

* [Animal Rescue Data](https://data.london.gov.uk/dataset/animal-rescue-incidents-attended-by-lfb)
* [Population by Postcode Data](https://www.nomisweb.co.uk/census/2011/postcode_headcounts_and_household_estimates) Table 1: All postcodes
