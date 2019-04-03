## Exercise 1 ##########################################################################

#> Rename PumpHoursTotal --> JobHours
#>
#> Rename AnimalGroupParent --> AnimalGroup

rescue = rescue.withColumnRenamed("PumpHoursTotal", "JobHours")
rescue = rescue.withColumnRenamed("AnimalGroupParent", "AnimalGroup")

########################################################################################



## Exercise 2 ############################################################################

#> Filter the recent data to find all the those with AnimalGroup equal to 'Fox'

recent_foxes = recent_rescue.filter(recent_rescue.AnimalGroup == 'Fox')
# Or equivilantly
recent_foxes = recent_rescue.filter('AnimalGroup == "Fox"')

recent_foxes.limit(10).toPandas()
recent_foxes.select('DateTimeOfCall', 'Description').show(truncate=False)

##########################################################################################



## Exercise 3 ################################################################################

#> Sort the incidents in terms of there duration, look at the top 10 and the bottom 10.

top_10 = (
    recent_rescue.select("IncidentNumber", "TotalCost", "IncidentDuration", "Description")
    .sort("IncidentDuration", ascending=False)
    .limit(10)
)
top_10.toPandas()

bottom_10 = (
    recent_rescue.select("IncidentNumber", "TotalCost", "IncidentDuration", "Description")
    .sort("IncidentDuration", ascending=True)
    .limit(10)
)
bottom_10.toPandas()

##############################################################################################



## Exercise 4 ####################################################################################

#> Add an additional flag to indicate when PropertyCategory is 'Dwelling'.
recent_rescue = recent_rescue.withColumn(
    "DwellingFlag", f.when(recent_rescue.PropertyCategory == "Dwelling", 1).otherwise(0)
)

#> Subset the data to rows when both the snake and property flag is 1
recent_rescued_pet_snakes = recent_rescue.filter(
    (recent_rescue.SnakeFlag == 1) & (recent_rescue.DwellingFlag == 1) 
)
recent_rescued_pet_snakes.limit(20).toPandas()

##################################################################################################



## Exercise 5 ################################################################################

#> Get total counts of incidents for the different animal types on the full dataset

incident_counts = (
    rescue.groupBy('AnimalGroup')
    .agg(f.count('AnimalGroup').alias('count'))
    .toPandas()
)

#> Sort the results in descending order and show the top 10

incident_counts = (
    rescue.groupBy('AnimalGroup')
    .agg(f.count('AnimalGroup').alias('count'))
    .sort('count', ascending=False)
    .limit(10)
    .toPandas()
)

##############################################################################################



## Exercise 6 ###############################################################################

# >Using SQL, find the top 10 most expensive call outs by AnimalGroup aggregated 
# >by the sum total of cost.

result = spark.sql(
    """SELECT AnimalGroup, sum(TotalCost) FROM rescue
            GROUP BY AnimalGroup
            ORDER BY sum(AnimalGroup) DESC
            LIMIT 10"""
)
result.show(truncate=False)


############################################################################################



