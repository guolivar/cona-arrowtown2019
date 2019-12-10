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
data_path <- "./"
##### Read the credentials file (ignored by GIT repository) ####
# Read the secrets
secret_hologram <- read_delim("./secret_hologram.txt", 
                              " ", escape_double = FALSE, trim_ws = TRUE)
# Get the tag list
base_url <- "https://dashboard.hologram.io/api/1/devices/tags?"
built_url <- paste0(base_url,
                    "orgid=",secret_hologram$orgid,"&",
                    "apikey=",secret_hologram$apikey)
req1 <- curl_fetch_memory(built_url)
jreq1 <- fromJSON(rawToChar(req1$content))$data$tags
ntags <- length(jreq1)
all_tags <- data.frame(id = (1:ntags),name = NA,topic = NA)

for (i in (1:ntags)){
  all_tags$id[i] <- jreq1[[i]]$id
  all_tags$name[i] <- jreq1[[i]]$name
  all_tags$topic[i] <- paste0("_TAG_",jreq1[[i]]$id,"_")
}
wanted_tags_human <- c("arrowtown2019")
tags <- subset(all_tags,name %in% wanted_tags_human)
wanted_tags <-paste(tags$topic,collapse = ",")
print(wanted_tags)

# Fetch the ODIN names
base_url <- "https://dashboard.hologram.io/api/1/devices?"
built_url <- paste0(base_url,
                    "limit=500&",
                    "orgid=",secret_hologram$orgid,"&",
                    "apikey=",secret_hologram$apikey)
req1 <- curl_fetch_memory(built_url)
jreq1 <- fromJSON(rawToChar(req1$content))$data
ndevices <- length(jreq1)
all_devices <- data.frame(id = (1:ndevices),name = NA)

for (i in (1:ndevices)){
  all_devices$id[i] <- jreq1[[i]]$id
  all_devices$name[i] <- jreq1[[i]]$name
}

## Get the timeseries data #####
# Final date to fetch data for
x_now <- as.POSIXct("2019-12-01 18:00:00",tz='UTC')
print(x_now)
t_start <- as.POSIXct("2019-05-25 18:00:00",tz='UTC')
# UTC time end ... now
t_end <- floor(as.numeric(x_now))
# Set the averaging interval
time_avg <- '1 min'
# This is for the averagin
x_start <- as.numeric(t_start)

print("Getting data")
# Need to go device by device for query stability
for (c_deviceid in all_devices$id){
  base_url <- "https://dashboard.hologram.io/api/1/csr/rdm?"
  print("First 1000 fetch")
  built_url <- paste0(base_url,
                      "deviceid=",c_deviceid,"&",
                      "topicnames=",wanted_tags,"&",
                      "timestart=",t_start,"&",
                      "timeend=",t_end,"&",
                      "limit=1000&",
                      "orgid=",secret_hologram$orgid,"&",
                      "apikey=",secret_hologram$apikey)
  req2 <- curl_fetch_memory(built_url)
  if (fromJSON(rawToChar(req2$content))$size==0){
    next
  }
  jreq2_tmp <- fromJSON(rawToChar(req2$content))$data
  x_jreq2 <- jreq2_tmp
  
  base_url <- "https://dashboard.hologram.io"
  
  while (fromJSON(rawToChar(req2$content))$continues){
    print("Next 1000 fetch")
    built_url <- paste0(base_url,
                        fromJSON(rawToChar(req2$content))$links[3])
    req2 <- curl_fetch_memory(built_url)
    jreq2_tmp <- fromJSON(rawToChar(req2$content))$data
    x_jreq2 <- append(x_jreq2,fromJSON(rawToChar(req2$content))$data)
  }
  print(ndata <- length(x_jreq2))
  
  if (exists("jreq2")){
    jreq2 <- append(jreq2,x_jreq2)
  } else {
    jreq2 <- x_jreq2
  }
  print("Got data")
}
print(ndata <- length(jreq2))

# We'll do this in parallel because it takes A LONG time with a few 100k records
#setup parallel backend to use many processors
cores <- detectCores()
cl <- makeCluster(4) #not to overload your computer
registerDoParallel(cl)

all_data <- foreach(i=1:ndata,
                    .packages=c("base64enc","RJSONIO"),
                    .combine=rbind,
                    .errorhandling = 'remove') %dopar%
                    {
                      c_data <- data.frame(id = 1)
                      c_data$PM1 <- NA
                      c_data$PM2.5 <- NA
                      c_data$PM10 <- NA
                      c_data$PMc <- NA
                      c_data$GAS1 <- NA
                      c_data$Tgas1 <- NA
                      c_data$GAS2 <- NA
                      c_data$Temperature <- NA
                      c_data$RH <- NA
                      c_data$date <- NA
                      c_data$timestamp <- NA
                      c_data$deviceid <- NA
                      c_data$tags <- NA
                      xxx <- rawToChar(base64decode(fromJSON(jreq2[[i]]$data)$data))
                      x_payload <- fromJSON(xxx)
                      payload <- unlist(x_payload)
                      # {"PM1":4,"PM2.5":6,"PM10":6,"GAS1":-999,"Tgas1":0,"GAS2":204,"Temperature":7.35,"RH":80.85,"recordtime":"2018/07/11;00:21:01"}
                      c_data$PM1 <- as.numeric(payload[1])
                      c_data$PM2.5 <- as.numeric(payload[2])
                      c_data$PM10 <- as.numeric(payload[3])
                      c_data$PMc <- as.numeric(payload[3]) - as.numeric(payload[2])
                      c_data$GAS1 <- as.numeric(payload[4])
                      c_data$Tgas1 <- as.numeric(payload[5])
                      c_data$GAS2 <- as.numeric(payload[6])
                      c_data$Temperature <- as.numeric(payload[7])
                      c_data$RH <- as.numeric(payload[8])
                      c_data$date <- as.POSIXct(as.character(payload[9]),format = "%Y/%m/%d;%H:%M:%S",tz="UTC")
                      c_data$timestamp <- as.POSIXct(jreq2[[i]]$logged,format = "%Y-%m-%d %H:%M:%OS",tz="UTC")
                      c_data$deviceid <- jreq2[[i]]$deviceid
                      c_data$tags <- paste((jreq2[[i]]$tags),collapse = ",")
                      c_data
                    }

stopCluster(cl)

all_data$serialn <- NA
device_ids <- unique(all_data$deviceid)
for (i in device_ids){
  all_data$serialn[all_data$deviceid==i] <- subset(all_devices,id==i)$name
}

# Remove index
all_data$id <- NULL
print(min(all_data$timestamp))
print(max(all_data$timestamp))
names(all_data)

# Fix wrong dates
# Clock not setup ... wrong date ... replace with server logging date
wrong_dates <- which(is.na(all_data$date) | (all_data$date <= as.POSIXct("2018/01/01")) | all_data$date > as.POSIXct(Sys.time()))
tmp_error_catching <- try(all_data$date[wrong_dates] <- all_data$timestamp[wrong_dates],
                          silent = TRUE)
# Clock in device ahead of server logging time ... wrong date ... replace with server logging date
wrong_dates <- which((all_data$date - all_data$timestamp) > 0)
tmp_error_catching <- try(all_data$date[wrong_dates] <- all_data$timestamp[wrong_dates],
                          silent = TRUE)
# No timestamp and no clock ... wrong date ... catchall step, replace with NA
wrong_dates <- which(all_data$date <= as.POSIXct("2010/01/01"))
tmp_error_catching <- try(all_data$date[wrong_dates] <- NA,
                          silent = TRUE)

# Calculate averaged time series
cl <- makeCluster(4) #not to overload your computer
registerDoParallel(cl)

all_data.tavg <- foreach(i=1:length(device_ids),
                         .packages=c("openair"),
                         .combine=rbind,
                         .errorhandling = 'remove') %dopar%
                         {
                           device_now <- subset(all_devices,id==device_ids[i])
                           some_data <- subset(all_data, serialn == device_now$name)
                           avg_data <- timeAverage(some_data,
                                                   avg.time = time_avg,
                                                   start.date = strftime(t_start, format = "%Y-%m-%d %H:00:00"))
                           avg_data$serialn <- subset(all_devices,id==device_ids[i])$name
                           avg_data$lat <- NA
                           avg_data$lon <- NA
                           ### Get LAT LON from curr_data
                           if (is.numeric(subset(curr_data, ODIN == device_now$name)$lat)){
                             avg_data$lat <- subset(curr_data, ODIN == device_now$name)$lat
                             avg_data$lon <- subset(curr_data, ODIN == device_now$name)$lon
                           }
                           avg_data
                         }

stopCluster(cl)

readr::write_csv(all_data,paste0(data_path,
                                 'all_data',
                                 format(min(all_data.tavg$date) + 12*3600,format = "%Y%m%d"),"_",
                                 format(max(all_data.tavg$date) + 12*3600,format = "%Y%m%d"),
                                 ".txt"),append = FALSE)
readr::write_csv(all_data.tavg,paste0(data_path,
                                      'all_dataAVG',
                                      format(min(all_data.tavg$date) + 12*3600,format = "%Y%m%d"),"_",
                                      format(max(all_data.tavg$date) + 12*3600,format = "%Y%m%d"),
                                      ".txt"),append = FALSE)

# Compress TXT files ####
print("Compress text files")
system(paste0("tar -zcvf ",
              data_path,
              'all_data',
              format(min(all_data.tavg$date) + 12*3600,format = "%Y%m%d"),"_",
              format(max(all_data.tavg$date) + 12*3600,format = "%Y%m%d"),
              ".tgz ",
              data_path,
              'all_data',
              format(min(all_data.tavg$date) + 12*3600,format = "%Y%m%d"),"_",
              format(max(all_data.tavg$date) + 12*3600,format = "%Y%m%d"),
              ".txt"))

system(paste0("tar -zcvf ",
              data_path,
              'all_dataAVG',
              format(min(all_data.tavg$date) + 12*3600,format = "%Y%m%d"),"_",
              format(max(all_data.tavg$date) + 12*3600,format = "%Y%m%d"),
              ".tgz ",
              data_path,
              'all_dataAVG',
              format(min(all_data.tavg$date) + 12*3600,format = "%Y%m%d"),"_",
              format(max(all_data.tavg$date) + 12*3600,format = "%Y%m%d"),
              ".txt"))

## Upload data ####

print("Upload data")
RCurl::ftpUpload(paste0(data_path,
                        'all_data',
                        format(min(all_data.tavg$date) + 12*3600,format = "%Y%m%d"),"_",
                        format(max(all_data.tavg$date) + 12*3600,format = "%Y%m%d"),
                        ".tgz"),
                 paste0("ftp://ftp.niwa.co.nz/incoming/GustavoOlivares/odin_arrowtown/",
                        'all_data_',
                        format(min(all_data.tavg$date) + 12*3600,format = "%Y%m%d"),"_",
                        format(max(all_data.tavg$date) + 12*3600,format = "%Y%m%d"),
                        ".tgz"))
RCurl::ftpUpload(paste0(data_path,
                        'all_dataAVG',
                        format(min(all_data.tavg$date) + 12*3600,format = "%Y%m%d"),"_",
                        format(max(all_data.tavg$date) + 12*3600,format = "%Y%m%d"),
                        ".tgz"),
                 paste0("ftp://ftp.niwa.co.nz/incoming/GustavoOlivares/odin_arrowtown/",
                        'all_dataAVG_',
                        format(min(all_data.tavg$date) + 12*3600,format = "%Y%m%d"),"_",
                        format(max(all_data.tavg$date) + 12*3600,format = "%Y%m%d"),
                        ".tgz"))

system('mv *.tgz data_compressed/')
