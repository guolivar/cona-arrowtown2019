# Check the last message for all ODIN devices
##### Load relevant packages #####
library(readr)
library(reshape2)

library(RJSONIO)
library(curl)
library(base64enc)

##### Set the working directory DB ####
setwd("~/repositories/cona-arrowtown2019/mapping/")
data_path <- "./"
##### Read the credentials file (ignored by GIT repository) ####
# Read the secrets
secret_hologram <- read_delim("./secret_hologram.txt", 
                              " ", escape_double = FALSE, trim_ws = TRUE)
# Fetch the ODIN names
base_url <- "https://dashboard.hologram.io/api/1/devices?"
built_url <- paste0(base_url,
                    "limit=500&",
                    "orgid=",secret_hologram$orgid,"&",
                    "apikey=",secret_hologram$apikey,"&",
                    "tagname=arrowtown2020")
req1 <- curl_fetch_memory(built_url)
jreq1 <- fromJSON(rawToChar(req1$content))$data
ndevices <- length(jreq1)
all_devices <- data.frame(id = (1:ndevices),name = NA)

for (i in (1:ndevices)){
  all_devices$id[i] <- jreq1[[i]]$id
  all_devices$name[i] <- jreq1[[i]]$name
}

## Get the last data #####

print("Getting data")
# Need to go device by device for query stability
indx <- 1
for (c_deviceid in all_devices$id){
  base_url <- "https://dashboard.hologram.io/api/1/csr/rdm?"
  built_url <- paste0(base_url,
                      "deviceid=",c_deviceid,"&",
                      "limit=1&",
                      "orgid=",secret_hologram$orgid,"&",
                      "apikey=",secret_hologram$apikey)
  req2 <- curl_fetch_memory(built_url)
  Sys.sleep(0.5)
  print(indx)
  indx <- indx + 1
  if (!fromJSON(rawToChar(req2$content))$success){
    print(0)
    next
  }
  x_jreq2 <- fromJSON(rawToChar(req2$content))$data
  if (exists("jreq2")){
    jreq2 <- append(jreq2,x_jreq2)
  } else {
    jreq2 <- x_jreq2
  }
}
print(ndata <- length(jreq2))


all_data <- data.frame(id = (1:ndata))
all_data$PM1 <- NA
all_data$PM2.5 <- NA
all_data$PM10 <- NA
all_data$PMc <- NA
all_data$GAS1 <- NA
all_data$Tgas1 <- NA
all_data$GAS2 <- NA
all_data$Temperature <- NA
all_data$RH <- NA
all_data$date <- NA
all_data$timestamp <- NA
all_data$deviceid <- NA
all_data$tags <- NA
for (i in (1:ndata)) {
  xxx <- rawToChar(base64decode(fromJSON(jreq2[[i]]$data)$data))
  x_payload <- fromJSON(xxx)
  payload <- unlist(x_payload)
  # {"PM1":4,"PM2.5":6,"PM10":6,"GAS1":-999,"Tgas1":0,"GAS2":204,"Temperature":7.35,"RH":80.85,"recordtime":"2018/07/11;00:21:01"}
  all_data$PM1[i] <- as.numeric(payload[1])
  all_data$PM2.5[i] <- as.numeric(payload[2])
  all_data$PM10[i] <- as.numeric(payload[3])
  all_data$PMc[i] <- as.numeric(payload[3]) - as.numeric(payload[2])
  all_data$GAS1[i] <- as.numeric(payload[4])
  all_data$Tgas1[i] <- as.numeric(payload[5])
  all_data$GAS2[i] <- as.numeric(payload[6])
  all_data$Temperature[i] <- as.numeric(payload[7])
  all_data$RH[i] <- as.numeric(payload[8])
  all_data$date[i] <- as.POSIXct(as.character(payload[9]),format = "%Y/%m/%d;%H:%M:%S",tz="UTC")
  all_data$timestamp[i] <- as.POSIXct(jreq2[[i]]$logged,format = "%Y-%m-%d %H:%M:%OS",tz="UTC")
  all_data$deviceid[i] <- jreq2[[i]]$deviceid
  all_data$tags[i] <- paste((jreq2[[i]]$tags),collapse = ",")
}

all_data$serialn <- NA
device_ids <- unique(all_data$deviceid)
for (i in device_ids){
  all_data$serialn[all_data$deviceid==i] <- subset(all_devices,id==i)$name
}

# Remove index
all_data$id <- NULL

names(all_data)
# Add FW version variable
all_data$fw_version <- NA
for ( i in (1:length(all_data$tags))){
  if (stringr::str_detect(all_data$tags[i],"2002")){
    all_data$fw_version[i] <- "2002"
  } else if (stringr::str_detect(all_data$tags[i],"2000")){
    all_data$fw_version[i] <- "2000"
  }
}
# NEED TO CHECK MANUALLY THOSE WITH ODD MESSAGES!


sort(all_data$serialn[stringr::str_detect(all_data$tags,"2002")])
# sort(all_data$serialn[stringr::str_detect(all_data$tags,wanted_tags)])
# 
# sum(stringr::str_detect(all_data$tags,"2002")&stringr::str_detect(all_data$tags,wanted_tags))



