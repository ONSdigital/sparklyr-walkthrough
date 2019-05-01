## Exercise 1 ##########################################################################

#> Rename PumpHoursTotal --> JobHours
#>
#> Rename AnimalGroupParent --> AnimalGroup

rescue <- rescue %>% rename(JobHours = PumpHoursTotal)
rescue <- rescue %>% rename(AnimalGroup = AnimalGroupParent)

########################################################################################



## Exercise 2 ############################################################################

#> Filter the recent data to find all the those with AnimalGroup equal to 'Fox'

foxes <- rescue %>% filter(AnimalGroup == 'Fox')

foxes %>% 
  select(DateTimeOfCall, Description) %>%
  collect() %>%
  print()

##########################################################################################



## Exercise 3 ################################################################################

#> Sort the incidents in terms of there duration, look at the top 10 and the bottom 10.

#> Do you notice anything strange?
top_10 <- rescue %>% 
  select(IncidentNumber, TotalCost, IncidentDuration, Description) %>%
   arrange(desc(IncidentDuration)) %>%
   head(10)

top_10

bottom_10 <- rescue %>% 
  select(IncidentNumber, TotalCost, IncidentDuration, Description) %>%
   arrange(IncidentDuration) %>%
   head(10)

bottom_10

##############################################################################################



## Exercise 4 ####################################################################################

#> Add an additional flag to indicate when PropertyCategory is 'Dwelling'.
rescue <- rescue %>%
  mutate(DwellingFlag = ifelse(PropertyCategory == "Dwelling", 1, 0))

#> Subset the data to rows when both the 'snake' and 'property' flag is 1
pet_snakes <- rescue %>% 
  filter(SnakeFlag == 1 & DwellingFlag == 1)

pet_snakes %>% 
  collect() %>%
  print()

##################################################################################################



## Exercise 5 ################################################################################

#> Get total counts of incidents for each different animal types

incident_counts <- rescue %>% 
  group_by(AnimalGroup) %>%
  summarise(count = n())

incident_counts

#> Sort the results in descending order and show the top 10

incident_counts %>%
    arrange(desc(count)) %>%
    head(10)
    


##############################################################################################



## Exercise 6 ###############################################################################

# >Using SQL, find the top 10 most expensive call outs aggregated by the total sumed cost for 
# >each AnimalGroup. 

result <- dbGetQuery(sc, 
    "SELECT AnimalGroup, sum(TotalCost) FROM rescue
            GROUP BY AnimalGroup
            ORDER BY sum(TotalCost) DESC
            LIMIT 10"
)
result



############################################################################################



