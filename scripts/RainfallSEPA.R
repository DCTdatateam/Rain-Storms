library(tidyverse)
library(dplyr)

## Import needed csv - LA linked to stations and sheet filtered to 
## just 36 hour values
stations_la <- read_csv("data/LA_rainfall_stations.csv")
total_value_ids <- read_csv("data/rainfall_36_hour_totals.csv")

stations_full <- total_value_ids  |> 
  left_join(stations_la |> select (station_no, local_auth))


## Function to grab the most recent 36 hour total data for all stations
process_data <- function(id, name, la, station_no) {
timeseries_url <-paste0("https://timeseries.sepa.org.uk/KiWIS/KiWIS?",
                        "service=kisters&type=queryServices&datasource=0&request=getTimeseriesValues",
                        "&ts_id=", id,
                        "&period&returnfields=Timestamp,%20Value,%20Quality%20Code&format=csv")
total_data <- read_csv2(timeseries_url, skip = 2, col_types = cols(.default = "c"))

total_data <- total_data %>%
  mutate(Value = as.numeric(str_replace(Value, ",", "."))) %>%
  mutate(`Quality Code` = as.integer(`Quality Code`))

total_data <- total_data %>%
  mutate(name = name) %>% 
  mutate(la = la) %>% 
  mutate(station_no = station_no)
return(total_data)

}

## Loop it through all the stations
all_totals <- data.frame()
for (i in 1:nrow(stations_full)) {
  id <-   stations_full$ts_id[i]
  name <- stations_full$station_name[i]
  la <- stations_full$local_auth[i]
  station_no <- stations_full$station_no[i]
  
  station_data <- process_data(id, name, la, station_no)
  result <- process_data(id, name, la, station_no)
  
  # Handle the result (e.g., save to CSV, print, etc.)
  all_totals <- bind_rows(all_totals, station_data)
}
write.csv(all_totals, "data/rain/36hoursall.csv", row.names = FALSE)

############# add it to map information


map_totals <- all_totals  |> 
  left_join(stations_la |> select (station_no, station_longitude, station_latitude))

map_totals <- map_totals %>% 
  mutate(range = case_when(Value <= 10 ~ "<10mm",
                           Value > 10 & Value <= 20 ~ "10-20mm",
                           Value > 20 & Value <= 30 ~ "20-30mm",
                           Value > 30 & Value <= 40 ~ "30-40mm",
                           Value > 40 & Value <= 50 ~ "40-50mm",
                           Value > 50 & Value <= 60 ~ "50-60mm",
                           Value > 60 & Value <= 70 ~ "60-70mm",
                           Value > 70  ~ ">70mm"))
write.csv(map_totals, "data/rain/36hoursGeography.csv", row.names = FALSE)

###########################################################
####################### MAIN FILTER ############################
## Now we can filter to find the stations to visualise
## First a number filter
## This should be set to >70 during periods of high rainfall

most_rain <- all_totals %>% 
  filter(Value > 30)
most_rain_patch <- most_rain %>% 
  filter(la == "Aberdeen City" | la == "Aberdeenshire" | 
           la == "Highland" | la == "Na h-Eileanan an Iar" | 
           la == "Orkney Islands" |  la == "Shetland Islands" | 
           la == "Moray" | la ==  "Angus" | la == "Dundee City" | la == "Fife" |
           la == "Perth and Kinross" | la == "Stirling")

#######################MAIN FILTER END############################
##############################################################
## Add a message here if they are empty

timings <- read.csv("data/rainfall_timings.csv")
stations_no_list <- unique(most_rain_patch$station_no)

##All parameters for the stations########
timings_latest_stations <- timings %>% 
  filter(station_no %in% stations_no_list)


###################### DATA FOR CHARTS ###############################
###### HOURLY DATA for the past three days ###########################
################### Use &period=P3D or period=P36H if you want 36 hours ##############################

hourly_data <- timings_latest_stations %>% 
  filter(ts_shortname == "Hour.Total")

hourly_data_la <- hourly_data  |> 
  left_join(stations_la |> select (station_no, local_auth))


process_hourly_data <- function(id, name, la) {
  timeseries_url <-paste0("https://timeseries.sepa.org.uk/KiWIS/KiWIS?",
                          "service=kisters&type=queryServices&datasource=0&request=getTimeseriesValues",
                          "&ts_id=", id,
                          "&period=P3D&returnfields=Timestamp,%20Value,%20Quality%20Code&format=csv")
  hourly_df <- read_csv2(timeseries_url, skip = 2, col_types = cols(.default = "c"))
  
  hourly_df <- hourly_df %>%
    mutate(Value = as.numeric(str_replace(Value, ",", "."))) %>%
    mutate(`Quality Code` = as.integer(`Quality Code`))
  
  hourly_df <- hourly_df %>%
    mutate(name = name) %>% 
    mutate(la = la) 
  return(hourly_df)
  
}

## Loop it through all the stations
filtered_hourly_data <- data.frame()
for (i in 1:nrow(hourly_data_la)) {
  id <-   hourly_data_la$ts_id[i]
  name <- hourly_data_la$station_name[i]
  la <- hourly_data_la$local_auth[i]
  
  station_hour_data <- process_hourly_data(id, name, la)
  result <- process_hourly_data(id, name, la)
  
  filtered_hourly_data <- bind_rows(filtered_hourly_data, station_hour_data)
}
## Write the filtered hourly data for the needed stations
write.csv(filtered_hourly_data, "data/rain/36hours_filtered.csv", row.names = FALSE)

##### Data analysis by local authority ###############################
LA_averages <- all_totals %>% 
  group_by(la) %>% 
  summarise("Average rainfall in 36 hours" = mean(Value), 
            "Number of stations" = n())
write.csv(LA_averages, "data/rain/LA_average_36_totals.csv", row.names = FALSE)

######################################################################
####### MONTHLY TOTALS ###############################################
######### HMonth.Total and P3Y #######################################
# I have set the period for the calculation to P3Y or three years adjust
# the number to change it Y=Year, M=Month, D=Day

rainfall_months_all <- timings_latest_stations %>% 
  filter(ts_shortname == "HMonth.Total")


rainfall_months_all <- rainfall_months_all |> 
  left_join(stations_la |> select (station_no, local_auth))

# function 
process_monthly_data <- function(id, name, la) {
  timeseries_url <-paste0("https://timeseries.sepa.org.uk/KiWIS/KiWIS?",
                          "service=kisters&type=queryServices&datasource=0&request=getTimeseriesValues",
                          "&ts_id=", id,
                          "&period=P3Y&returnfields=Timestamp,%20Value,%20Quality%20Code&format=csv")
  monthly_df <- read_csv2(timeseries_url, skip = 2, col_types = cols(.default = "c"))
  
  monthly_df <- monthly_df %>%
    mutate(Value = as.numeric(str_replace(Value, ",", "."))) %>%
    mutate(`Quality Code` = as.integer(`Quality Code`))
  
  monthly_df <- monthly_df %>%
    mutate(name = name) %>% 
    mutate(la = la) 
  return(monthly_df)
  
}

## Loop it through all the stations
filtered_monthly_data <- data.frame()
for (i in 1:nrow(rainfall_months_all)) {
  id <-   rainfall_months_all$ts_id[i]
  name <- rainfall_months_all$station_name[i]
  la <- rainfall_months_all$local_auth[i]
  
  station_month_data <- process_monthly_data(id, name, la)
  result <- process_monthly_data(id, name, la)
  
  filtered_monthly_data <- bind_rows(filtered_monthly_data, station_month_data)
}

write.csv(filtered_monthly_data, "data/rain/filteredmonthly.csv", row.names = FALSE)
