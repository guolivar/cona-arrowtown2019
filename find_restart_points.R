# Sample data ... fetch data and play
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
library(stringi)
library(ggplot2)

# Read the secrets
secret_hologram <- read_delim("./secret_hologram.txt", 
                              "\t", escape_double = FALSE, trim_ws = TRUE)
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
devices <- data.frame(deviceid = (1:nsites),ODIN = NA)
for (i in (1:nsites)){
  devices$deviceid[i] <- jreq1[[i]]$id
  devices$ODIN[i] <- jreq1[[i]]$name
}

# Get devices locations
proj4string <- "+proj=tmerc +lat_0=0.0 +lon_0=173.0 +k=0.9996 +x_0=1600000.0 +y_0=10000000.0 +datum=WGS84 +units=m"
odin_locations <- read_delim("./odin_locations.txt", 
                             "\t", escape_double = FALSE, trim_ws = TRUE)
devices$lat <- NA
devices$lon <- NA
for (i in (1:nsites)){
  loc_id <- which(substr(odin_locations$Serialn,7,11)==substr(devices$ODIN[i],6,9))
  p <- project(cbind(odin_locations$Easting[loc_id],odin_locations$Northing[loc_id]),proj = proj4string,inv = TRUE)
  devices$lon[i] <- p[1]
  devices$lat[i] <- p[2]
}

# Get the last X measurements
nmeas <- 60*24*12
base_url <- "https://dashboard.hologram.io/api/1/csr/rdm?"
# timestart is "nmeas" minutes befre now
t_start <- floor(as.numeric(Sys.time()- nmeas * 60))
ndev <- length(devices$deviceid)
for (i_dev in (1:ndev)){
  print(devices$ODIN[i_dev])
    n_steps <- ceiling(nmeas/1000)
    for (step in (1:n_steps)){
      if (step == 1){
        built_url <- paste0(base_url,
                            "deviceid=",devices$deviceid[i_dev],"&",
                            "limit=",min(nmeas,1000),"&",
                            "timestart=",t_start,"&",
                            "orgid=",secret_hologram$orgid,"&",
                            "apikey=",secret_hologram$apikey)
        req2 <- curl_fetch_memory(built_url)
        jreq2 <- fromJSON(rawToChar(req2$content))$data
      } else {
        built_url <- paste0(base_url,
                            "deviceid=",devices$deviceid[i_dev],"&",
                            "limit=",min(nmeas-1000*step,1000),"&",
                            "timestart=",t_start,"&",
                            "startat=",startat,"&",
                            "orgid=",secret_hologram$orgid,"&",
                            "apikey=",secret_hologram$apikey)
        req2 <- curl_fetch_memory(built_url)
        jreq2 <- append(jreq2,fromJSON(rawToChar(req2$content))$data)
      }
      last <- length(jreq2)
      if (last<1){
        break
      }
      startat <- jreq2[[last]]$id
    }
    if (last < 1) {
      next
    }
    ndata <- length(jreq2)
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
    c_data$date <- as.POSIXct("2018-05-01 00:00:00",tz='UTC')
    c_data$timestamp <- c_data$date
    c_data$lat <- devices$lat[i_dev]
    c_data$lon <- devices$lon[i_dev]
    c_data$siteid <- devices$ODIN[i_dev]
  
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
  if (i_dev == 1){
    all_data <- c_data
  } else {
    all_data <- rbind(all_data,c_data)
  }
  rm(c_data)
}

# Identify (re)start times
start_data <- subset(all_data, date <= as.POSIXct('2018-01-01 00:00',tz = 'UTC'))[,c('siteid','date','timestamp','Temperature','RH')]

print(start_data)

ggplot(data = all_data,aes(x=timestamp)) + 
  geom_point(aes(y=PM2.5,col=siteid),position = "jitter")

ggplot(data = all_data.10min,aes(x=date)) +
  geom_line(aes(y=PM2.5,colour=serialn))


#for (odin_name in unique(all_data$siteid)){
#  xx <- subset(all_data,siteid == odin_name)
#  plot(xx$timestamp,xx$Temperature * 1000)
#  points(xx$timestamp,xx$date)
#}
