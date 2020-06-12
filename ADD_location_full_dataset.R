##### Load relevant packages #####
library(readr)
library(reshape2)
library(RJSONIO)
library(curl)
library(base64enc)
library(openair)
library(parallel)
library(doParallel)

##### Set the working directory DB ####
setwd("~/repositories/cona-arrowtown2019/mapping/")
data_path <- "./alldata/"
# Clear the error file
error_file <- "errors.txt"
if (file.exists(error_file)) 
  #Delete file if it exists
  file.remove(error_file)
## Read the location and times for each unit
odin_locations <- read_delim("./odin_locations.txt",
                             "\t", escape_double = FALSE, trim_ws = TRUE)
# Fix timestamps formats
odin_locations$startdate <- as.POSIXct(odin_locations$startdate,format = '%d/%m/%Y %H:%M', tz = 'UTC') + 12*3600
odin_locations$enddate <- as.POSIXct(odin_locations$enddate,format = '%d/%m/%Y %H:%M', tz = 'UTC') + 12*3600
# Make NA enddate equal to 1st January 2020
noend <- which(is.na(odin_locations$enddate))
odin_locations$enddate[noend] <- as.POSIXct("2020-01-01 00:00",format = '%Y-%m-%d %H:%M', tz = 'UTC')

## Get the timeseries data from the datafolder #####
files_all <- dir(data_path,pattern = "AVG")
# Extract ODINID from filenames
odin_ids <- substr(files_all,1,6)

print("Getting data")
# Need to go device by device for query stability
for (c_deviceid in odin_ids){
  # load file
  data.curr <- read_csv(paste0(data_path,c_deviceid,"_AVG.txt"))
  # Attach locations
  
  # Now incorporate the correct location information
  # serialn_long == serialn
  # find the right location lines
  c_location <- subset(odin_locations,serialn_long == data.curr$serialn[1])
  if (length(c_location$serialn_long) > 0){
    for (location_i in (1:length(c_location$serialn_long))){
      set_idx <- which(data.curr$date >= c_location$startdate[location_i] & data.curr$date <= c_location$enddate[location_i])
      data.curr$lat[set_idx] <- c_location$lat[location_i]
      data.curr$lon[set_idx] <- c_location$lon[location_i]
    }
    write_csv(data.curr,paste0(data_path,
                                          'data_location/',
                                          c_deviceid,
                                          '_AVG.txt'),append = FALSE)
    # Liberate some memory and stop leakage
  } else {
    print(paste("No location infor for:",data.curr$serialn[1]))
  }
}

