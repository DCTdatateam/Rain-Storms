library(tidyverse)
library(dplyr)
library(lubridate)
library(stringr)
rm(list=ls())

## Upload the data you want pulled
## Assign local authorities to allow filtering 
stations_la <- read_csv("data/LA_waterlevel_stations.csv")

station_daily <- read_csv("data/waterlevels_daily.csv")

waterlevels_months <- read_csv("data/waterlevels_summary.csv")

waterlevels_timeseries <- read_csv("data/waterlevels_with_all_timeseries.csv")

stations_add_la <- waterlevels_timeseries  |> 
  left_join(stations_la |> select (station_no, local_auth))


stations_full <- stations_add_la %>% 
  pivot_wider(names_from = ts_shortname, values_from = ts_id) %>% 
  select(-site_no, -stationparameter_no, -from, -to)  %>% 
  group_by(station_name, station_no, local_auth) %>% 
  summarise(across(starts_with("X15m") | starts_with("LTV") | starts_with("HDay") | starts_with("15m") | starts_with("HMonth"), 
                   ~ na.omit(.)[1])) %>%
  ungroup() %>% 
  rename(current = "15m.Cmd")


#########################################################################
############### MONTH SUMMARIES #########################################
########################################################################
load_data <- function(name, la, m_mean, m_max, m_min, station_id) {
  

  max_url <- paste0("https://timeseries.sepa.org.uk/KiWIS/KiWIS?",
                    "service=kisters&type=queryServices&datasource=0&request=getTimeseriesValues",
                    "&ts_id=", m_max,
                    "&period=P1Y&returnfields=Timestamp,%20Value&format=csv")
  
  max_data <- read_csv2(max_url, skip = 2, col_types = cols(.default = "c")) %>%
    mutate(m_max = as.numeric(str_replace(Value, ",", "."))) %>%
    select('#Timestamp', m_max)
  

  mean_url <- paste0("https://timeseries.sepa.org.uk/KiWIS/KiWIS?",
                     "service=kisters&type=queryServices&datasource=0&request=getTimeseriesValues",
                     "&ts_id=", m_mean,
                     "&period=P1Y&returnfields=Timestamp,%20Value&format=csv")
  
  mean_data <- read_csv2(mean_url, skip = 2, col_types = cols(.default = "c")) %>%
    mutate(m_mean = as.numeric(str_replace(Value, ",", "."))) %>%
    select('#Timestamp', m_mean)
  

  min_url <- paste0("https://timeseries.sepa.org.uk/KiWIS/KiWIS?",
                    "service=kisters&type=queryServices&datasource=0&request=getTimeseriesValues",
                    "&ts_id=", m_min,
                    "&period=P1Y&returnfields=Timestamp,%20Value&format=csv")
  
  min_data <- read_csv2(min_url, skip = 2, col_types = cols(.default = "c")) %>%
    mutate(m_min = as.numeric(str_replace(Value, ",", "."))) %>%
    select('#Timestamp', m_min)
  

  merged_data <- max_data %>%
    left_join(mean_data, by = "#Timestamp") %>%
    left_join(min_data, by = "#Timestamp") %>%
    mutate(name = name, la = la, station_id = station_id)
  
  return(merged_data)
}

## Empty dataframe for the loop
monthly_values_all <- data.frame()

## The loop that merges the data together into one sheet
for (i in 1:nrow(stations_full)) {
  name <- stations_full$station_name[i]
  la <- stations_full$local_auth[i]
  m_mean <- stations_full$LTV.HMonth.Mean[i]
  m_max <- stations_full$LTV.HMonth.Max[i]
  m_min <- stations_full$LTV.HMonth.Min[i]
  station_id <- stations_full$station_no[i]
  
  station_data <- load_data(name, la, m_mean, m_max, m_min, station_id)
  
  # Append the station data to the df
  monthly_values_all  <- bind_rows(monthly_values_all , station_data)
}
######################################################################
#######################DAY MAX##########################################
######################################################################


max_level_function <- function(name, la, today_max, station_id) {
  
  today_max_url <-paste0("https://timeseries.sepa.org.uk/KiWIS/KiWIS?",
                          "service=kisters&type=queryServices&datasource=0&request=getTimeseriesValues",
                          "&ts_id=", today_max,
                          "&period&returnfields=Occurrance%20Timestamp,%20Value&format=csv")
  
  todaymax_data <- read_csv2(today_max_url, skip = 2, col_types = cols(.default = "c")) %>% 
    mutate(Value = as.numeric(str_replace(Value, ",", "."))) %>% 
    mutate(name = name, la = la, station_id = station_id) %>% 
    rename(daily_max = Value) 
    
  
  return(todaymax_data)

}

##empty dataframe for the loop
day_max <- data.frame()

##The loop that merges the data together into one sheet
for (i in 1:nrow(stations_full)) {
  name <- stations_full$station_name[i]
  la <- stations_full$local_auth[i]
  today_max <- stations_full$HDay.Max[i]
  station_id <- stations_full$station_no[i]
  
  gather_data <- max_level_function(name, la, today_max, station_id)
  
  # Handle the result (e.g., save to CSV, print, etc.)
  day_max <- bind_rows(day_max, gather_data)
  
}
######################################################################
#######################CHECK FOR HIGHS################################
######################################################################
#date format: 2023-10-01T09:00:00.000Z
monthly_values_all <- monthly_values_all %>% 
  rename(Time = `#Timestamp`)

monthly_values_all$Time <- ymd_hms(gsub("T", " ", gsub("Z", "", monthly_values_all$Time)))
current_date <- Sys.Date()
current_year <- year(current_date)
current_month <- month(current_date)

this_month_high_record <- monthly_values_all %>%
  filter(year(Time) == current_year & month(Time) == current_month)


## Now join the two sheets
this_month_high_record <- this_month_high_record %>%
  mutate(name = str_to_title(trimws(name)))

day_max <- day_max %>%
  mutate(name = str_to_title(trimws(name)))


checking_highs <- day_max %>% 
  left_join(this_month_high_record, by = "name") %>% 
  select(-`la.y`) %>%  
  rename(la = `la.x`) %>%   
  filter(la == "Aberdeen City" | la == "Aberdeenshire" | 
  la == "Highland" | la == "Na h-Eileanan an Iar" | 
  la == "Orkney Islands" |  la == "Shetland Islands" | 
  la == "Moray" | la ==  "Angus" | la == "Dundee City" | la == "Fife" |
  la == "Perth and Kinross" | la == "Stirling")

## There are a few which end up as NA for the mean info. There are so many stations
## we can remove them from consideration as another filter level

checking_highs <- checking_highs[!(is.na(checking_highs$m_max)),]

## Now check how daily maxes perform against the record max for that month
write.csv(checking_highs, "data/waterlevels/checking_highs.csv", row.names = FALSE)
######################################################################
########### SET COMPARISON - currently checking if it is closer to the max than mean)

checking_highs_compare <- checking_highs %>% 
  mutate(within_high_range = m_max - ((m_max - m_mean)/2)) %>% 
  rowwise() %>% 
  filter(daily_max > within_high_range)
write.csv(checking_highs_compare,"data/waterlevels/checking_highs_compare.csv", row.names = FALSE)

######################################################################
################### 15 Min data ######################################

# get timeseries ids
waterlevels_available_timings <- read.csv2("https://timeseries.sepa.org.uk/KiWIS/KiWIS?service=kisters&type=queryServices&datasource=0&request=getTimeseriesList&parametertype_name=S&returnfields=site_no,%20station_no,%20station_name,%20stationparameter_no,%20ts_shortname,%20ts_id,%20coverage&dateformat=yyyy-MM-dd%20HH:mm:ss&format=csv", dec = ".") %>% 
  filter(ts_shortname == "15m.Cmd") %>% 
  rename(station_id.x = station_no)


minute_waterlevels <- checking_highs_compare %>% 
  left_join(waterlevels_available_timings)


#Same function to loop through the timeseries ids 
minute_function <- function(name, la, minute_timeseries, station_id) {
  
  minute_url <-paste0("https://timeseries.sepa.org.uk/KiWIS/KiWIS?",
                         "service=kisters&type=queryServices&datasource=0&request=getTimeseriesValues",
                         "&ts_id=", minute_timeseries,
                         "&period=P36H&returnfields=Timestamp,%20Value&format=csv")
  
 minute_data <- read_csv2(minute_url, skip = 2, col_types = cols(.default = "c")) %>% 
    mutate(Value = as.numeric(str_replace(Value, ",", "."))) %>% 
    mutate(name = name, la = la, station_id = station_id) %>% 
    rename('15min value' = Value) 
  
  
  return(minute_data)
  
}

##empty dataframe for the loop
waterlevel_timeseries <- data.frame()

##The loop that merges the data together into one sheet
for (i in 1:nrow(minute_waterlevels)) {
  name <- minute_waterlevels$name [i]
  la <- minute_waterlevels$la [i]
  minute_timeseries <- minute_waterlevels$ts_id [i]
  station_id <- minute_waterlevels$station_id.x  [i]
  
  gather_df <- minute_function(name, la, minute_timeseries, station_id)
  
  # Handle the result (e.g., save to CSV, print, etc.)
  waterlevel_timeseries <- bind_rows(waterlevel_timeseries, gather_df)
  
}

write.csv(waterlevel_timeseries, "data/waterlevels/waterlevels_minutes.csv", row.names = FALSE)


######################################################################
##filter just for P&J 
# 
# daily_means <- stations_full
# 
# daily_means <- daily_means %>% 
#   filter(local_auth == "Aberdeen City" | local_auth == "Aberdeenshire" | 
#            local_auth == "Highland" | local_auth == "Na h-Eileanan an Iar" | 
#            local_auth == "Orkney Islands" |  local_auth == "Shetland Islands" | 
#            local_auth == "Moray")
# 
# ## filter just for Courier
# cour_daily_means <- daily_means %>% 
#   filter(local_auth ==  "Angus" | local_auth == "Dundee City" | local_auth == "Fife" |
#            local_auth == "Perth and Kinross" | local_auth == "Stirling")
# 
# ## Optional filter for stations
# # daily_means <- daily_means %>% 
# #   filter(station_name/station_no == "")
# 
######################################################################   
# #Use this code if you just want a spreadsheet for each timeseries  
#   # file_name <- paste0("waterlevels/",name, ".csv")
#   # write.csv(historic_data, file_name, row.names = FALSE)
#   
#   
