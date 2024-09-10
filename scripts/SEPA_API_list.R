library(tidyverse)
library(dplyr)


#Full list of stations for all measurements

stationlist <- read.csv2("https://timeseries.sepa.org.uk/KiWIS/KiWIS?service=kisters&type=queryServices&datasource=0&request=getStationList&format=csv", dec = ".")

#Parameter list

parameters <- read.csv2("https://timeseries.sepa.org.uk/KiWIS/KiWIS?service=kisters&type=queryServices&datasource=0&request=getParameterList&format=csv", dec = ".")


################################################
################SUMMARY#########################
###Summary of available figures from SEPA API###

##### WATER LEVELS #######
## parametertype_name=S

waterlevels_all <- read.csv2("https://timeseries.sepa.org.uk/KiWIS/KiWIS?service=kisters&type=queryServices&datasource=0&request=getStationList&parametertype_name=S&returnfields=station_name,%20station_no,%20catchment_name,%20river_name,%20station_latitude,%20station_longitude&format=csv", dec = ".")


##the edits to the link and filter should be done for each category
waterlevels_available_timings <- read.csv2("https://timeseries.sepa.org.uk/KiWIS/KiWIS?service=kisters&type=queryServices&datasource=0&request=getTimeseriesList&parametertype_name=S&returnfields=site_no,%20station_no,%20station_name,%20stationparameter_no,%20ts_shortname,%20ts_id,%20coverage&dateformat=yyyy-MM-dd%20HH:mm:ss&format=csv", dec = ".")


waterlevels_daily_mean <- waterlevels_available_timings %>% 
#  filter(ts_shortname == "HDay.Min" | ts_shortname == "HDay.Max" | ts_shortname ==  "HDay.Mean" )
  filter(ts_shortname == "HDay.Max")

waterlevels_monthly_summary <-  waterlevels_available_timings %>% 
  filter(ts_shortname == "LTV.HMonth.Max" | ts_shortname == "LTV.HMonth.Min" | ts_shortname ==  "LTV.HMonth.Mean" )

waterlevels_current <- waterlevels_available_timings %>% 
  filter(ts_shortname == "15m.Cmd")

waterlevels_joint <- waterlevels_available_timings %>% 
  filter(ts_shortname == "LTV.HMonth.Max" | ts_shortname == "LTV.HMonth.Min" | ts_shortname ==  "LTV.HMonth.Mean" | ts_shortname == "HDay.Max" | ts_shortname == "15m.Cmd" | ts_shortname == "HMonth.Max")
  
  
write.csv(waterlevels_all, "data/waterlevels_stations.csv", row.names = FALSE)
write.csv(waterlevels_daily_mean, "data/waterlevels_daily.csv", row.names = FALSE)
write.csv(waterlevels_monthly_summary, "data/waterlevels_summary.csv", row.names = FALSE)
write.csv(waterlevels_current, "data/waterlevels_current.csv", row.names = FALSE)
write.csv(waterlevels_joint, "data/waterlevels_with_all_timeseries.csv", row.names = FALSE)


#### RAINFALL #########
## parametertype_name=Precip

rainfall_all <- read.csv2("https://timeseries.sepa.org.uk/KiWIS/KiWIS?service=kisters&type=queryServices&datasource=0&request=getStationList&parametertype_name=Precip&returnfields=station_name,%20station_no,%20catchment_name,%20river_name,%20station_latitude,%20station_longitude&format=csv", dec = ".")

rainfall_available_timings <- read.csv2("https://timeseries.sepa.org.uk/KiWIS/KiWIS?service=kisters&type=queryServices&datasource=0&request=getTimeseriesList&parametertype_name=Precip&returnfields=site_no,%20station_no,%20station_name,%20stationparameter_no,%20ts_shortname,%20ts_id,%20coverage&dateformat=yyyy-MM-dd%20HH:mm:ss&format=csv", dec = ".")
#filter down to 36hour values
rainfall_recent_total <- rainfall_available_timings %>% 
  filter(ts_shortname == "Hour.36HourTotal")

rainfall_months <- rainfall_available_timings %>% 
  filter(ts_shortname =="HMonth.Total")

write.csv(rainfall_all, "data/rainfall_stations.csv", row.names = FALSE)
write.csv(rainfall_available_timings, "data/rainfall_timings.csv", row.names = FALSE)
write.csv(rainfall_recent_total, "data/rainfall_36_hour_totals.csv", row.names = FALSE)
write.csv(rainfall_months, "data/rainfall_monthly.csv", row.names = FALSE)

##### GROUNDWATER LEVEL #########
## parametertype_name=GWLVL

groundwater_all <- read.csv2("https://timeseries.sepa.org.uk/KiWIS/KiWIS?service=kisters&type=queryServices&datasource=0&request=getStationList&parametertype_name=GWLVL&returnfields=station_name,%20station_no,%20catchment_name,%20river_name,%20station_latitude,%20station_longitude&format=csv", dec = ".")

groundwater_available_timings <- read.csv2("https://timeseries.sepa.org.uk/KiWIS/KiWIS?service=kisters&type=queryServices&datasource=0&request=getTimeseriesList&parametertype_name=GWLVL&returnfields=site_no,%20station_no,%20station_name,%20stationparameter_no,%20ts_shortname,%20coverage&dateformat=yyyy-MM-dd%20HH:mm:ss&format=csv", dec = ".")

write.csv(groundwater_all, "data/gw_stations.csv", row.names = FALSE)

##### RIVER FLOW #########
## parametertype_name=Q

flow_all <- read.csv2("https://timeseries.sepa.org.uk/KiWIS/KiWIS?service=kisters&type=queryServices&datasource=0&request=getStationList&parametertype_name=Q&returnfields=station_name,%20station_no,%20catchment_name,%20river_name,%20station_latitude,%20station_longitude&format=csv", dec = ".")

flow_available_timings <- read.csv2("https://timeseries.sepa.org.uk/KiWIS/KiWIS?service=kisters&type=queryServices&datasource=0&request=getTimeseriesList&parametertype_name=Q&returnfields=site_no,%20station_no,%20stationparameter_no,%20ts_shortname,%20coverage&dateformat=yyyy-MM-dd%20HH:mm:ss&format=csv", dec = ".")

write.csv(flow_all, "data/flow_stations.csv", row.names = FALSE)



##### TIDAL LEVEL #########
## parametertype_name=TideLVL

tidal_all <- read.csv2("https://timeseries.sepa.org.uk/KiWIS/KiWIS?service=kisters&type=queryServices&datasource=0&request=getStationList&parametertype_name=TideLVL&returnfields=station_name,%20station_no,%20catchment_name,%20river_name,%20station_latitude,%20station_longitude&format=csv", dec = ".")

tidal_available_timings <- read.csv2("https://timeseries.sepa.org.uk/KiWIS/KiWIS?service=kisters&type=queryServices&datasource=0&request=getTimeseriesList&parametertype_name=TideLVL&returnfields=site_no,%20station_no,%20stationparameter_no,%20ts_shortname,%20coverage&dateformat=yyyy-MM-dd%20HH:mm:ss&format=csv", dec = ".")

write.csv(tidal_all, "data/tidal_stations.csv", row.names = FALSE)
