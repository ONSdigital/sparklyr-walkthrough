## Exercise 1 ##########################################################################

#> Rename PumpHoursTotal --> JobHours
#>
#> Rename AnimalGroupParent --> AnimalGroup

rescue = rescue.withColumnRenamed("PumpHoursTotal", "JobHours")
rescue = rescue.withColumnRenamed("AnimalGroupParent", "AnimalGroup")

########################################################################################



## Exercise 2 ############################################################################

#> Filter the recent data to find all the those with AnimalGroup equal to 'Fox'

foxes = rescue.filter(rescue.AnimalGroup == 'Fox')
# Or equivilantly
foxes = rescue.filter('AnimalGroup == "Fox"')

foxes.limit(10).toPandas()
foxes.select('DateTimeOfCall', 'Description').show(truncate=False)

##########################################################################################



## Exercise 3 ################################################################################

#> Sort the incidents in terms of there duration, look at the top 10 and the bottom 10.

#> Do you notice anything strange?
top_10 = (
    rescue.select("IncidentNumber", "TotalCost", "IncidentDuration", "Description")
    .sort("IncidentDuration", ascending=False)
    .limit(10)
)
top_10.toPandas()

bottom_10 = (
    rescue.select("IncidentNumber", "TotalCost", "IncidentDuration", "Description")
    .sort("IncidentDuration", ascending=True)
    .limit(10)
)
bottom_10.toPandas()

##############################################################################################



## Exercise 4 ####################################################################################

#> Add an additional flag to indicate when PropertyCategory is 'Dwelling'.
rescue = rescue.withColumn(
    "DwellingFlag", f.when(rescue.PropertyCategory == "Dwelling", 1).otherwise(0)
)

#> Subset the data to rows when both the 'snake' and 'property' flag is 1
pet_snakes = rescue.filter(
    (rescue.SnakeFlag == 1) & (rescue.DwellingFlag == 1) 
)
pet_snakes.limit(20).toPandas()

##################################################################################################



## Exercise 5 ################################################################################

#> Get total counts of incidents for each different animal types

incident_counts = (
    rescue.groupBy('AnimalGroup')
    .agg(f.count('AnimalGroup'))
    .toPandas()
)

#> Sort the results in descending order and show the top 10

incident_counts = (
    rescue.groupBy('AnimalGroup')
    .agg(f.count('AnimalGroup'))
    .sort('count(AnimalGroup)', ascending=False)
    .limit(10)
    .toPandas()
)

##############################################################################################



## Exercise 6 ###############################################################################

# >Using SQL, find the top 10 most expensive call outs aggregated by the total sumed cost for 
# >each AnimalGroup. 

result = spark.sql(
    """SELECT AnimalGroup, sum(TotalCost) FROM rescue
            GROUP BY AnimalGroup
            ORDER BY sum(TotalCost) DESC
            LIMIT 10"""
)
result.show(truncate=False)


############################################################################################



