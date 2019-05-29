##### Load relevant packages #####
library(readr)
library(reshape2)
library(automap)
library(raster)
library(gstat)
library(sp)
library(rgdal)
library(ggmap)
library(gstat)
library(ncdf4)
library(RJSONIO)
library(curl)
library(base64enc)
library(zoo)
library(openair)
library(stringi)


##### Set the working directory DB ####
setwd("~/repositories/cona_live/mapping/")
##### Read the credentials file (ignored by GIT repository) ####
secret_hologram <- read_delim("./secret_hologram.txt", 
                              "\t", escape_double = FALSE, trim_ws = TRUE)

##### Get data ####

# Get the devices ID
base_url <- "https://dashboard.hologram.io/api/1/devices?"
tag <- "cona2018"
built_url <- paste0(base_url,
                    "orgid=",secret_hologram$orgid,"&",
                    "tagname=",tag,"&",
                    "apikey=",secret_hologram$apikey)
req1 <- curl_fetch_memory(built_url)
jreq1 <- fromJSON(rawToChar(req1$content))$data
nsites <- length(jreq1)
curr_data <- data.frame(deviceid = (1:nsites),ODIN = NA)
for (i in (1:nsites)){
  curr_data$deviceid[i] <- jreq1[[i]]$id
  curr_data$ODIN[i] <- jreq1[[i]]$name
}

# Get the latest measurements
base_url <- "https://dashboard.hologram.io/api/1/csr/rdm?"
curr_data$PM1 <- NA
curr_data$PM2.5 <- NA
curr_data$PM10 <- NA
curr_data$Temperature <- NA
curr_data$RH <- NA
curr_data$Timestamp <- as.POSIXct("2018-05-01 00:00:00",tz='UTC')
i <- 1
for (i in (1:nsites)){
  built_url <- paste0(base_url,
                      "deviceid=",curr_data$deviceid[i],"&",
                      "limit=1&",
                      "apikey=",secret_hologram$apikey)
  req2 <- curl_fetch_memory(built_url)
  jreq2 <- fromJSON(rawToChar(req2$content))$data
  xxx <- rawToChar(base64decode(fromJSON(jreq2[[1]]$data)$data))
  x_payload <- try(fromJSON(paste0(stri_split_fixed(xxx,",\"recordtime")[[1]][1],"}")),silent = TRUE)
  if (inherits(x_payload,"try-error")) {
    next
  }
  
  payload <- unlist(x_payload)
  if (length(payload)<5){
    next
  }
  
  curr_data$Timestamp[i] <- as.POSIXct(jreq2[[1]]$logged,format = "%Y-%m-%d %H:%M:%OS",tz="UTC")
  curr_data$PM1[i] <- as.numeric(payload[1])
  curr_data$PM2.5[i] <- as.numeric(payload[2])
  curr_data$PM10[i] <- as.numeric(payload[3])
  curr_data$Temperature[i] <- as.numeric(payload[7])
  curr_data$RH[i] <- as.numeric(payload[8])
}

curr_data$delay <- floor(difftime(Sys.time(),curr_data$Timestamp, units = 'secs'))
curr_data$mask <- 0
for (i in (1:nsites)){
  curr_data$mask[i] <- max(as.numeric(curr_data$delay[i] < 120),0.2)
}

# Get devices locations
proj4string <- "+proj=tmerc +lat_0=0.0 +lon_0=173.0 +k=0.9996 +x_0=1600000.0 +y_0=10000000.0 +datum=WGS84 +units=m"
odin_locations <- read_delim("./odin_locations.txt", 
                             "\t", escape_double = FALSE, trim_ws = TRUE)
curr_data$lat <- NA
curr_data$lon <- NA
for (i in (1:nsites)){
  loc_id <- which(substr(odin_locations$Serialn,7,11)==substr(curr_data$ODIN[i],6,9))
  p <- project(cbind(odin_locations$Easting[loc_id],odin_locations$Northing[loc_id]),proj = proj4string,inv = TRUE)
  curr_data$lon[i] <- p[1]
  curr_data$lat[i] <- p[2]
}

centre_lat <- mean(curr_data$lat)
centre_lon <- mean(curr_data$lon)

# Now get datafor last 12 hours and calculate average for mapping
# Cycle through each deviceID and calculate the 12hr average up to now
curr_data$PM1 <- NA
curr_data$PM2.5 <- NA
curr_data$PM10 <- NA
curr_data$Temperature <- NA
curr_data$RH <- NA
max_nmeas <- 60*12
ndev <- length(curr_data$deviceid)

# UTC time start
t_start <- as.numeric(as.POSIXct("2018/07/04 12:00:00",tz = "GMT-12"))
# UTC time start
t_end <- as.numeric(as.POSIXct("2018/07/06 07:00:00",tz = "GMT-12"))

base_url <- "https://dashboard.hologram.io/api/1/csr/rdm?"
for (i_dev in (1:ndev)){
  ndata <- 1
  nstep <- 1
  print("Getting data")
  while (ndata >= 1){
    if (nstep == 1){
      built_url <- paste0(base_url,
                          "deviceid=",curr_data$deviceid[i_dev],"&",
                          "timestart=",t_start,"&",
                          "timeend=",t_end,"&",
                          "limit=1000&",
                          "orgid=",secret_hologram$orgid,"&",
                          "apikey=",secret_hologram$apikey)
      req2 <- curl_fetch_memory(built_url)
      jreq2_tmp <- fromJSON(rawToChar(req2$content))$data
      jreq2 <- jreq2_tmp
    } else {
      built_url <- paste0(base_url,
                          "deviceid=",curr_data$deviceid[i_dev],"&",
                          "timestart=",t_start,"&",
                          "timeend=",t_end,"&",
                          "limit=1000&",
                          "startat=",startat,"&",
                          "orgid=",secret_hologram$orgid,"&",
                          "apikey=",secret_hologram$apikey)
      req2 <- curl_fetch_memory(built_url)
      jreq2_tmp <- fromJSON(rawToChar(req2$content))$data
      jreq2 <- append(jreq2,fromJSON(rawToChar(req2$content))$data)
    }
    
    ndata <- length(jreq2_tmp)
    if (ndata < 1){
      break
    }
    startat <- jreq2_tmp[[ndata]]$id
    nstep <- nstep + 1
  }
  
  #print(jreq2[[ndata]]$data)
  ndata <- length(jreq2)
  print("Got data")
  print(ndata)
  if (ndata < 1){
    # This device didn't have data for this period
    next
  }
  c_data <- data.frame(id = (1:ndata))
  c_data$PM1 <- NA
  c_data$PM2.5 <- NA
  c_data$PM10 <- NA
  c_data$PMc <- NA
  c_data$GAS1 <- NA
  c_data$Tgas1 <- NA
  c_data$GAS2 <- NA
  c_data$Temperature <- NA
  c_data$RH <- NA
  c_data$date <- as.POSIXct(jreq2[[ndata]]$logged,format = "%Y-%m-%d %H:%M:%OS",tz="UTC")
  c_data$timestamp <- c_data$date
  c_data$lat <- curr_data$lat[i_dev]
  c_data$lon <- curr_data$lon[i_dev]
  c_data$serialn <- curr_data$ODIN[i_dev]
  
  for (i in (1:ndata)){
    xxx <- rawToChar(base64decode(fromJSON(jreq2[[i]]$data)$data))
    x_payload <- try(fromJSON(xxx),silent = TRUE)
    if (inherits(x_payload,"try-error")) {
      next
    }
    
    payload <- unlist(x_payload)
    if (length(payload)<5){
      next
    }
    # {"PM1":4,"PM2.5":6,"PM10":6,"GAS1":-999,"Tgas1":0,"GAS2":204,"Temperature":7.35,"RH":80.85,"recordtime":"2018/07/11;00:21:01"}
    c_data$PM1[i] <- as.numeric(payload[1])
    c_data$PM2.5[i] <- as.numeric(payload[2])
    c_data$PM10[i] <- as.numeric(payload[3])
    c_data$PMc[i] <- as.numeric(payload[3]) - as.numeric(payload[2])
    c_data$GAS1[i] <- as.numeric(payload[4])
    c_data$Tgas1[i] <- as.numeric(payload[5])
    c_data$GAS2[i] <- as.numeric(payload[6])
    c_data$Temperature[i] <- as.numeric(payload[7])
    c_data$RH[i] <- as.numeric(payload[8])
    c_data$date[i] <- as.POSIXct(as.character(payload[9]),format = "%Y/%m/%d;%H:%M:%S",tz="UTC")
    c_data$timestamp[i] <- as.POSIXct(jreq2[[i]]$logged,format = "%Y-%m-%d %H:%M:%OS",tz="UTC")
  }

  has_data <- try(length(c_data$PM1),silent = TRUE)
  if (inherits(has_data,"try-error")) {
    next
  }
  print(min(c_data$timestamp))
  print(max(c_data$timestamp))
  
  if (i_dev == 1){
    all_data <- c_data
    all_data.10min <- timeAverage(c_data,avg.time = '10 min')
    all_data.10min$serialn <- curr_data$ODIN[i_dev]
  } else {
    all_data <- rbind(all_data,c_data)
    tmp10min <- timeAverage(c_data,avg.time = '10 min')
    tmp10min$serialn <- curr_data$ODIN[i_dev]
    all_data.10min <- rbind(all_data.10min,tmp10min)
  }
  curr_data$PM1[i_dev] <- mean(c_data$PM1,na.rm = TRUE)
  curr_data$PM2.5[i_dev] <- mean(c_data$PM2.5,na.rm = TRUE)
  curr_data$PM10[i_dev] <- mean(c_data$PM10,na.rm = TRUE)
  curr_data$Temperature[i_dev] <- mean(c_data$Temperature,na.rm = TRUE)
  curr_data$RH[i_dev] <- mean(c_data$RH,na.rm = TRUE)
  rm(c_data)
}
curr_data$Last_reading <- curr_data$Timestamp



  ggplot(data = all_data,aes(x=timestamp)) +
  geom_line(aes(y=PM2.5,colour=serialn))


# plot_data <- all_data
# select_SN <- "ODIN-0024 (90333)"
# plot_data <- subset(all_data,serialn==select_SN)
# ggplot(data = plot_data,aes(x=date)) +
#   geom_line(aes(y=rollmean(PM1,60,na.pad=TRUE),colour='PM1'))+
#   geom_line(aes(y=rollmean(PM2.5,60,na.pad=TRUE),colour='PM2.5'))+
#   geom_line(aes(y=rollmean(PM10,60,na.pad=TRUE),colour='PM10')) +
#   geom_line(aes(y=rollmean(Temperature * 5,60,na.pad=TRUE),colour='T * 10')) +
#   ggtitle(select_SN)


for (select_SN in unique(all_data$serialn)){
  save_data <- subset(all_data,serialn==select_SN)
  write_delim(save_data,paste0("~/data/CONA/2018/colo_1/",select_SN,".txt"),delim = "\t")
}
