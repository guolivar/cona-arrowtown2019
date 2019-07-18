# Fetch data from 1 site
##### Load relevant packages #####
library(librarian) # To more flexibly manage packages
shelf(readr,
      reshape2,
      automap,
      raster,
      gstat,
      sp,
      rgdal,
      ggmap,
      ggplot2,
      scales,
      gstat,
      RNetCDF,
      RJSONIO,
      curl,
      base64enc,
      zoo,
      openair,
      stringi,
      viridis,
      dplyr,
      RColorBrewer,
      purrr,
      magick)

# Read the secrets
secret_hologram <- read_delim("./secret_hologram.txt", 
                              " ", escape_double = FALSE, trim_ws = TRUE)
# Get the devices ID #####
base_url <- "https://dashboard.hologram.io/api/1/devices?"
tag <- "odin"
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

# Choose ODIN
i_dev <- grep(x=curr_data$ODIN,pattern = '0184')

## Get the timeseries data #####
# UTC time start
x_now <- Sys.time()
print(x_now)
# UTC time start
t_start <- as.numeric(as.POSIXct("2018/08/02 12:00:00",tz = "GMT-12"))

# UTC time end ... now
t_end <- floor(as.numeric(x_now))
# Set the averaging interval
time_avg <- '15 min'

ndata <- 1
nstep <- 1
print("Getting data")
base_url <- "https://dashboard.hologram.io/api/1/csr/rdm?"

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
  
  print(ndata <- length(jreq2_tmp))
  if (ndata < 1){
    break
  }
  startat <- jreq2_tmp[[ndata]]$id
  nstep <- nstep + 1
  print(jreq2_tmp[[ndata]]$logged)
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
wrong_dates <- which(is.na(c_data$date) | (c_data$date <= as.POSIXct("2010/01/01")) | c_data$date > as.POSIXct(Sys.time()))
tmp_error_catching <- try(c_data$date[wrong_dates] <- c_data$timestamp[wrong_dates],
                          silent = TRUE)
wrong_dates <- which(c_data$date <= as.POSIXct("2010/01/01"))
tmp_error_catching <- try(c_data$date[wrong_dates] <- NA,
                          silent = TRUE)
print(min(c_data$date))
print(max(c_data$date))

all_data <- c_data
all_data.tavg <- timeAverage(c_data,avg.time = time_avg)
all_data.tavg$serialn <- curr_data$ODIN[i_dev]

rm(c_data)

# Plot data ####
avg_plot <- '10 min'
timePlot(all_data,pollutant = c('PM1','PM2.5','PM10'),main = curr_data$ODIN[i_dev],avg.time = avg_plot,group = TRUE,ylim=c(0,100))
#scatterPlot(all_data,x='PM2.5',y='PM10',avg.time = avg_plot,linear = TRUE)
#scatterPlot(all_data,x='PM1',y='PM2.5',avg.time = avg_plot,linear = TRUE)
#scatterPlot(all_data,x='PM1',y='PM10',avg.time = avg_plot,linear = TRUE)

#plot(all_data$date[1:(i-2)],all_data$date[2:(i-1)] - all_data$date[1:(i-2)],main = curr_data$ODIN[i_dev],ylim = c(-65,-55))
  