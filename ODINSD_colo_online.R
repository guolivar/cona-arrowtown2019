#' title: Check ODIN Co-location
#' author: Gustavo Olivares
#' 

### Load relevant packages ####
library(librarian) # To more flexibly manage packages
shelf(readr,
      reshape2,
      RJSONIO,
      curl,
      base64enc,
      zoo,
      openair,
      stringi,
      viridis,
      dplyr)

##### Set the working directory DB ####
work_path <- path.expand("~/repositories/cona_live/odin_live_check/")
setwd(work_path)
data_path <- "./"
##### Read the credentials file (ignored by GIT repository) ####
secret_hologram <- read_delim("./secret_hologram.txt", 
                              " ", escape_double = FALSE, trim_ws = TRUE)

##### Get data ####

# Get the devices ID #####
base_url <- "https://dashboard.hologram.io/api/1/devices?"
tag <- "glen_eden_colo"
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

# Get the latest measurements #####
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


## Get the timeseries data #####

x_now <- Sys.time()
print(x_now)
t_start <- as.numeric(as.POSIXct("2018/09/28 12:00:00",tz = "GMT-12"))
# UTC time end ... now
t_end <- floor(as.numeric(x_now))
# Set the averaging interval
time_avg <- '15 min'

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
  wrong_dates <- which(c_data$date <= as.POSIXct("2010/01/01"))
  tmp_error_catching <- try(c_data$date[wrong_dates] <- c_data$timestamp[wrong_dates],
                            silent = TRUE)
  wrong_dates <- which(c_data$date <= as.POSIXct("2010/01/01"))
  tmp_error_catching <- try(c_data$date[wrong_dates] <- NA,
                            silent = TRUE)
  print(min(c_data$date))
  print(max(c_data$date))
  
  if (i_dev == 1){
    all_data <- c_data
    all_data.tavg <- timeAverage(c_data,avg.time = time_avg)
    all_data.tavg$serialn <- curr_data$ODIN[i_dev]
  } else {
    all_data <- rbind(all_data,c_data)
    tmp10min <- timeAverage(c_data,avg.time = time_avg)
    tmp10min$serialn <- curr_data$ODIN[i_dev]
    all_data.tavg <- rbind(all_data.tavg,tmp10min)
  }
  curr_data$PM1[i_dev] <- mean(c_data$PM1,na.rm = TRUE)
  curr_data$PM2.5[i_dev] <- mean(c_data$PM2.5,na.rm = TRUE)
  curr_data$PM10[i_dev] <- mean(c_data$PM10,na.rm = TRUE)
  curr_data$Temperature[i_dev] <- mean(c_data$Temperature,na.rm = TRUE)
  curr_data$RH[i_dev] <- mean(c_data$RH,na.rm = TRUE)
  rm(c_data)
}


readr::write_csv(all_data,paste0(data_path,
                                 'colo_all_data',
                                 format(min(all_data.tavg$date) + 12*3600,format = "%Y%m%d"),"_",
                                 format(max(all_data.tavg$date) + 12*3600,format = "%Y%m%d"),
                                 ".txt"),append = FALSE)
readr::write_csv(all_data.tavg,paste0(data_path,
                                 'colo_all_dataAVG',
                                 format(min(all_data.tavg$date) + 12*3600,format = "%Y%m%d"),"_",
                                 format(max(all_data.tavg$date) + 12*3600,format = "%Y%m%d"),
                                 ".txt"),append = FALSE)

# Plot time series ####
plot_tseries <- ggplot(data.frame(all_data.tavg),aes(x=date)) +
  geom_line(aes(y=PM2.5,colour=serialn))
ggsave(filename = paste0(data_path,
                         't_series_',
                         format(min(all_data.tavg$date) + 12*3600,format = "%Y%m%d"),"_",
                         format(max(all_data.tavg$date) + 12*3600,format = "%Y%m%d"),
                         ".png"),
       plot = plot_tseries,
       width = 12,
       height = 6,
       units = 'in')

# Summary stats ####
data.mean <- aggregate(all_data$PM2.5,by = list(all_data$serialn),FUN = mean)
data.counts <- aggregate(all_data$PM2.5,by = list(all_data$serialn),FUN = length)
data.sd <- aggregate(all_data$PM2.5,by = list(all_data$serialn),FUN = sd)

# Compress TXT files ####
system(paste0("tar -zcvf ",
              data_path,
              'colo_all_data',
              format(min(all_data.tavg$date) + 12*3600,format = "%Y%m%d"),"_",
              format(max(all_data.tavg$date) + 12*3600,format = "%Y%m%d"),
              ".tgz ",
              data_path,
              'colo_all_data',
              format(min(all_data.tavg$date) + 12*3600,format = "%Y%m%d"),"_",
              format(max(all_data.tavg$date) + 12*3600,format = "%Y%m%d"),
              ".txt"))

system(paste0("tar -zcvf ",
              data_path,
              'colo_all_dataAVG',
              format(min(all_data.tavg$date) + 12*3600,format = "%Y%m%d"),"_",
              format(max(all_data.tavg$date) + 12*3600,format = "%Y%m%d"),
              ".tgz ",
              data_path,
              'colo_all_dataAVG',
              format(min(all_data.tavg$date) + 12*3600,format = "%Y%m%d"),"_",
              format(max(all_data.tavg$date) + 12*3600,format = "%Y%m%d"),
              ".txt"))

## Upload data ####

RCurl::ftpUpload(paste0(data_path,
                        'colo_all_data',
                        format(min(all_data.tavg$date) + 12*3600,format = "%Y%m%d"),"_",
                        format(max(all_data.tavg$date) + 12*3600,format = "%Y%m%d"),
                        ".tgz"),
                 paste0("ftp://ftp.niwa.co.nz/incoming/GustavoOlivares/odin_alexandra/",
                        'colo_all_data_',
                        format(min(all_data.tavg$date) + 12*3600,format = "%Y%m%d"),"_",
                        format(max(all_data.tavg$date) + 12*3600,format = "%Y%m%d"),
                        ".tgz"))
RCurl::ftpUpload(paste0(data_path,
                        'colo_all_dataAVG',
                        format(min(all_data.tavg$date) + 12*3600,format = "%Y%m%d"),"_",
                        format(max(all_data.tavg$date) + 12*3600,format = "%Y%m%d"),
                        ".tgz"),
                 paste0("ftp://ftp.niwa.co.nz/incoming/GustavoOlivares/odin_alexandra/",
                        'colo_all_dataAVG_',
                        format(min(all_data.tavg$date) + 12*3600,format = "%Y%m%d"),"_",
                        format(max(all_data.tavg$date) + 12*3600,format = "%Y%m%d"),
                        ".tgz"))

## Remove files ####
system(paste0("rm -rf ",
              data_path,
              "idw/*"))
system(paste0("rm -rf ",
              data_path,
              "idw2/*"))
system(paste0('rm -f ',
              data_path,
              'all_data',
              format(min(all_data.tavg$date) + 12*3600,format = "%Y%m%d"),"_",
              format(max(all_data.tavg$date) + 12*3600,format = "%Y%m%d"),
              ".txt"))
system(paste0('rm -f ',
              data_path,
              'all_dataAVG',
              format(min(all_data.tavg$date) + 12*3600,format = "%Y%m%d"),"_",
              format(max(all_data.tavg$date) + 12*3600,format = "%Y%m%d"),
              ".txt"))
