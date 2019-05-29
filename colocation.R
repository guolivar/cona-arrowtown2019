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

ndev <- length(curr_data$deviceid)

# UTC time start
t_start <- as.numeric(as.POSIXct("2018/07/04 19:00:00",tz = "GMT-12"))
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

  rm(c_data)
}
curr_data$Last_reading <- curr_data$Timestamp



ggplot(data = all_data.10min,aes(x=date)) +
  geom_line(aes(y=Temperature,colour=serialn))

# Reference ODIN (arbitrary choice)
base_SN <- "ODIN-0022 (90317)"
base_odin <- subset(all_data.10min,serialn==base_SN)
curr_data$pm1.int <- NA
curr_data$pm1.slp <- NA
curr_data$pm1.int_2.5 <- NA
curr_data$pm1.int_97.5 <- NA
curr_data$pm1.slp_2.5 <- NA
curr_data$pm1.slp_97.5 <- NA
curr_data$pm1.r2 <- NA

curr_data$pm2.5.int <- NA
curr_data$pm2.5.slp <- NA
curr_data$pm2.5.int_2.5 <- NA
curr_data$pm2.5.int_97.5 <- NA
curr_data$pm2.5.slp_2.5 <- NA
curr_data$pm2.5.slp_97.5 <- NA
curr_data$pm2.5.r2 <- NA

curr_data$pm10.int <- NA
curr_data$pm10.slp <- NA
curr_data$pm10.int_2.5 <- NA
curr_data$pm10.int_97.5 <- NA
curr_data$pm10.slp_2.5 <- NA
curr_data$pm10.slp_97.5 <- NA
curr_data$pm10.r2 <- NA


for (select_SN in unique(all_data.10min$serialn)){
  odin_idx <- which(curr_data$ODIN == select_SN)
  test_odin <- subset(all_data.10min,serialn==select_SN)
  joined_odin <- merge(base_odin,test_odin,by = 'date')
  # PM1
  lm.pm1 <- lm(data = joined_odin, PM1.x ~ PM1.y)
  lm.coefs <- coef(lm.pm1)
  lm.confint <- confint(lm.pm1)
  curr_data$pm1.int[odin_idx] <- lm.coefs[1]
  curr_data$pm1.slp[odin_idx] <- lm.coefs[2]
  curr_data$pm1.int_2.5[odin_idx] <- lm.confint[1]
  curr_data$pm1.int_97.5[odin_idx] <- lm.confint[3]
  curr_data$pm1.slp_2.5[odin_idx] <- lm.confint[2]
  curr_data$pm1.slp_97.5[odin_idx] <- lm.confint[4]
  curr_data$pm1.r2[odin_idx] <- cor(joined_odin$PM1.x,joined_odin$PM1.y,use = 'pairwise')^2
  #PM2.5
  lm.pm2.5 <- lm(data = joined_odin, PM2.5.x ~ PM2.5.y)
  lm.coefs <- coef(lm.pm2.5)
  lm.confint <- confint(lm.pm2.5)
  curr_data$pm2.5.int[odin_idx] <- lm.coefs[1]
  curr_data$pm2.5.slp[odin_idx] <- lm.coefs[2]
  curr_data$pm2.5.int_2.5[odin_idx] <- lm.confint[1]
  curr_data$pm2.5.int_97.5[odin_idx] <- lm.confint[3]
  curr_data$pm2.5.slp_2.5[odin_idx] <- lm.confint[2]
  curr_data$pm2.5.slp_97.5[odin_idx] <- lm.confint[4]
  curr_data$pm2.5.r2[odin_idx] <- cor(joined_odin$PM2.5.x,joined_odin$PM2.5.y,use = 'pairwise')^2
  
  #PM10
  lm.pm10 <- lm(data = joined_odin, PM10.x ~ PM10.y)
  lm.coefs <- coef(lm.pm10)
  lm.confint <- confint(lm.pm10)
  curr_data$pm10.int[odin_idx] <- lm.coefs[1]
  curr_data$pm10.slp[odin_idx] <- lm.coefs[2]
  curr_data$pm10.int_2.5[odin_idx] <- lm.confint[1]
  curr_data$pm10.int_97.5[odin_idx] <- lm.confint[3]
  curr_data$pm10.slp_2.5[odin_idx] <- lm.confint[2]
  curr_data$pm10.slp_97.5[odin_idx] <- lm.confint[4]
  curr_data$pm10.r2[odin_idx] <- cor(joined_odin$PM10.x,joined_odin$PM10.y,use = 'pairwise')^2
}

write_delim(curr_data,paste0("~/data/CONA/2018/colo_1/regression_data.txt"),delim = "\t")

# Correct from colocation data
# Get colo data
reg.data <- read.delim("~/data/CONA/2018/colo_1/regression_data.txt",sep = "\t")
for (serialn in unique(all_data.10min$serialn)){
  reg.id <- which(reg.data$ODIN == serialn)
  data.id <- which(all_data.10min$serialn == serialn)
  all_data.10min[data.id,c("PM1")] <- all_data.10min[data.id,c("PM1")] * reg.data$pm1.slp[reg.id] + reg.data$pm1.int[reg.id]
  all_data.10min[data.id,c("PM2.5")] <- all_data.10min[data.id,c("PM2.5")] * reg.data$pm2.5.slp[reg.id] + reg.data$pm2.5.int[reg.id]
  all_data.10min[data.id,c("PM10")] <- all_data.10min[data.id,c("PM10")] * reg.data$pm10.slp[reg.id] + reg.data$pm10.int[reg.id]
}
