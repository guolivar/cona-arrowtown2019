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

##### Set the working directory DB ####
setwd("~/repositories/cona_live/mapping/")
data_path <- "./"
##### Read the credentials file (ignored by GIT repository) ####
secret_hologram <- read_delim("./secret_hologram.txt", 
                              " ", escape_double = FALSE, trim_ws = TRUE)

##### Get data ####

# Get the devices ID #####
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

# Get devices locations
proj4string <- "+proj=tmerc +lat_0=0.0 +lon_0=173.0 +k=0.9996 +x_0=1600000.0 +y_0=10000000.0 +datum=WGS84 +units=m"
odin_locations <- read_delim(paste0(data_path,"odin_locations_stage2.txt"), 
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

curr_data$PM1 <- NA
curr_data$PM2.5 <- NA
curr_data$PM10 <- NA
curr_data$Temperature <- NA
curr_data$RH <- NA
max_nmeas <- 60*12
ndev <- length(curr_data$deviceid)

## Prepare the map to plot animations #####

# Get the basemap
ca <- get_map(
  c(lon=centre_lon,lat=centre_lat),
  zoom=15,crop=T,
  scale="auto",color="bw",source="google",
  maptype="terrain") # can change to terrain

## Get the timeseries data #####
# UTC time start ... 24 hours ago
x_now <- Sys.time()
print(x_now)
t_start <- as.numeric(as.POSIXct("2018/09/05 12:00:00",tz = "GMT-12"))
# UTC time end ... now
t_end <- as.numeric(as.POSIXct("2018/09/12 12:00:00",tz = "GMT-12"))
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
  wrong_dates <- which(c_data$date <= as.POSIXct("2015/01/01"))
  tmp_error_catching <- try(c_data$date[wrong_dates] <- c_data$timestamp[wrong_dates],
                            silent = TRUE)
  wrong_dates <- which(c_data$date <= as.POSIXct("2015/01/01"))
  tmp_error_catching <- try(c_data$date[wrong_dates] <- NA,
                            silent = TRUE)
  print(min(c_data$date))
  print(max(c_data$date))
  
  if (i_dev == 1){
    all_data <- c_data
    all_data.tavg <- timeAverage(c_data,avg.time = time_avg)
    all_data.tavg$serialn <- curr_data$ODIN[i_dev]
    all_data.tavg$lat <- curr_data$lat[i_dev]
    all_data.tavg$lon <- curr_data$lon[i_dev]
  } else {
    all_data <- rbind(all_data,c_data)
    tmp10min <- timeAverage(c_data,avg.time = time_avg)
    tmp10min$serialn <- curr_data$ODIN[i_dev]
    tmp10min$lat <- curr_data$lat[i_dev]
    tmp10min$lon <- curr_data$lon[i_dev]
    all_data.tavg <- rbind(all_data.tavg,tmp10min)
  }
  curr_data$PM1[i_dev] <- mean(c_data$PM1,na.rm = TRUE)
  curr_data$PM2.5[i_dev] <- mean(c_data$PM2.5,na.rm = TRUE)
  curr_data$PM10[i_dev] <- mean(c_data$PM10,na.rm = TRUE)
  curr_data$Temperature[i_dev] <- mean(c_data$Temperature,na.rm = TRUE)
  curr_data$RH[i_dev] <- mean(c_data$RH,na.rm = TRUE)
  rm(c_data)
}

# Correct from colocation data #####
# Get colo data
# reg.data <- read.delim(paste0(data_path,"regression_data.txt"),sep = "\t")
# for (serialn in unique(all_data.tavg$serialn)){
#   reg.id <- which(reg.data$ODIN == serialn)
#   data.id <- which(all_data.tavg$serialn == serialn)
#   all_data.tavg[data.id,c("PM1")] <- all_data.tavg[data.id,c("PM1")] * reg.data$pm1.slp[reg.id] + reg.data$pm1.int[reg.id]
#   all_data.tavg[data.id,c("PM2.5")] <- all_data.tavg[data.id,c("PM2.5")] * reg.data$pm2.5.slp[reg.id] + reg.data$pm2.5.int[reg.id]
#   all_data.tavg[data.id,c("PM10")] <- all_data.tavg[data.id,c("PM10")] * reg.data$pm10.slp[reg.id] + reg.data$pm10.int[reg.id]
#   data.id <- which(all_data$serialn == serialn)
#   all_data[data.id,c("PM1")] <- all_data[data.id,c("PM1")] * reg.data$pm1.slp[reg.id] + reg.data$pm1.int[reg.id]
#   all_data[data.id,c("PM2.5")] <- all_data[data.id,c("PM2.5")] * reg.data$pm2.5.slp[reg.id] + reg.data$pm2.5.int[reg.id]
#   all_data[data.id,c("PM10")] <- all_data[data.id,c("PM10")] * reg.data$pm10.slp[reg.id] + reg.data$pm10.int[reg.id]
# }

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

coordinates(curr_data) <- ~ lon + lat
proj4string(curr_data) <- CRS('+init=epsg:4326')
coordinates(all_data.tavg) <- ~ lon + lat
proj4string(all_data.tavg) <- CRS('+init=epsg:4326')
# Re-project to NZTM #####
all_data.tavg <- spTransform(all_data.tavg,CRS('+init=epsg:2193'))

print("Starting the kriging")

#Setting the  prediction grid properties #####
cellsize <- 100 #pixel size in projection units (NZTM, i.e. metres)
min_x <- all_data.tavg@bbox[1,1] - cellsize - 1000 #minimun x coordinate 1km south
min_y <- all_data.tavg@bbox[2,1] - cellsize - 1000 #minimun y coordinate 1km west
max_x <- all_data.tavg@bbox[1,2] + cellsize + 1000 #mximum x coordinate 1km north
max_y <- all_data.tavg@bbox[2,2] + cellsize + 1000 #maximum y coordinate 1km east

x_length <- max_x - min_x #easting amplitude
y_length <- max_y - min_y #northing amplitude

ncol <- round(x_length/cellsize,0) #number of columns in grid
nrow <- round(y_length/cellsize,0) #number of rows in grid

grid <- GridTopology(cellcentre.offset=c(min_x,min_y),cellsize=c(cellsize,cellsize),cells.dim=c(ncol,nrow))

#Convert GridTopolgy object to SpatialPixelsDataFrame object. #####
grid <- SpatialPixelsDataFrame(grid,
                               data=data.frame(id=1:prod(ncol,nrow)),
                               proj4string=CRS('+init=epsg:2193'))


# Get rid of NA containing rows
all_data.tavg <- subset(all_data.tavg,!is.na(PM2.5))
all_dates <- sort(unique(all_data.tavg$date))
valid_dates <- FALSE * (1:length(all_dates))
# limits for colorscales #####
cmin <- min(all_data.tavg$PM2.5)
cmax <- max(all_data.tavg$PM2.5) * 0.5
## Interpolate and plot #####
ndates <- length(all_dates)
breaks <- as.numeric(quantile((1:ndates),c(0,0.5,1), type = 1))
nbreaks <- length(breaks)
i <- 0
for (d_slice in (1:ndates)){
  c_data <- subset(all_data.tavg,subset = (date==all_dates[d_slice]))
  
  if (length(unique(c_data$serialn))<2){
    next
  }
  valid_dates[d_slice] <- TRUE
  #  surf.krig <- autoKrige(pm2.5 ~ 1,data=c_data,new_data = grid, input_data=c_data)
  #  surf.krig$krige_output$timestamp <-d_slice
  #  proj4string(surf.krig$krige_output) <- CRS('+init=epsg:2193')
  
  surf.idw <- idw(PM2.5 ~ 1,newdata = grid, locations = c_data, idp = 1,na.action = na.omit)
  surf.idw$timestamp <-d_slice
  proj4string(surf.idw) <- CRS('+init=epsg:2193')
  
  surf.idw2 <- idw(PM2.5 ~ 1,newdata = grid, locations = c_data, idp = 2)
  surf.idw2$timestamp <-d_slice
  proj4string(surf.idw2) <- CRS('+init=epsg:2193')
  
  if (i==0){
    #    x_data <- surf.krig$krige_output@data
    #    x_bbox <- surf.krig$krige_output@bbox
    #    x_coords <- surf.krig$krige_output@coords
    #    x_coords.nrs <- c(1,2)
    #    to_rast.krig <- surf.krig$krige_output
    #    r0.krig <- rasterFromXYZ(cbind(to_rast.krig@coords,to_rast.krig@data$var1.pred))
    #    crs(r0.krig) <- '+init=epsg:2193'
    #    raster_cat.krig <- r0.krig
    
    to_rast.idw <- surf.idw
    r0.idw <- rasterFromXYZ(cbind(surf.idw@coords,surf.idw$var1.pred))
    crs(r0.idw) <- '+init=epsg:2193'
    raster_cat.idw<- r0.idw
    
    to_rast.idw2 <- surf.idw2
    r0.idw2 <- rasterFromXYZ(cbind(surf.idw2@coords,surf.idw2$var1.pred))
    crs(r0.idw2) <- '+init=epsg:2193'
    raster_cat.idw2<- r0.idw2
    i <- 1
  }
  else {
    #    x_data <- rbind(x_data,surf.krig$krige_output@data)
    #    x_coords <- rbind(x_coords,surf.krig$krige_output@coords)
    #    to_rast.krig <- surf.krig$krige_output
    #    r0.krig <- rasterFromXYZ(cbind(to_rast.krig@coords,to_rast.krig@data$var1.pred))
    #    crs(r0.krig) <- '+init=epsg:2193'
    #    raster_cat.krig <- addLayer(raster_cat.krig,r0.krig)
    
    to_rast.idw <- surf.idw
    r0.idw <- rasterFromXYZ(cbind(surf.idw@coords,surf.idw$var1.pred))
    names(r0.idw) <- as.character(all_dates[d_slice])
    crs(r0.idw) <- '+init=epsg:2193'
    raster_cat.idw<- addLayer(raster_cat.idw,r0.idw)
    
    to_rast.idw2 <- surf.idw2
    r0.idw2 <- rasterFromXYZ(cbind(surf.idw2@coords,surf.idw2$var1.pred))
    names(r0.idw2) <- as.character(all_dates[d_slice])
    crs(r0.idw2) <- '+init=epsg:2193'
    raster_cat.idw2<- addLayer(raster_cat.idw2,r0.idw2)
  }
  rtp <- rasterToPolygons(projectRaster(r0.idw,crs = "+proj=longlat +datum=WGS84"))
  rtp2 <- rasterToPolygons(projectRaster(r0.idw2,crs = "+proj=longlat +datum=WGS84"))
  points <- data.frame(spTransform(c_data,CRS('+init=epsg:4326')))
  
  # Build the animation
  map_out <- ggmap(ca) + geom_polygon(data = rtp,aes(x = long, y = lat, group = group, 
                                                     fill = rep(rtp[[1]], each = 5)), 
                                      size = 0, 
                                      alpha = 0.85) +
    scale_fill_gradient(low="white", high="red",limits=c(0, cmax), name = "PM2.5", oob=squish) +
    geom_point(data=points,aes(x=lon,y=lat),colour = "black") +
    ggtitle(paste(as.character(all_dates[d_slice]+12*3600),"NZST"))
  ggsave(filename=paste0(data_path,'idw/',format(all_dates[d_slice]+12*3600,format = "%Y-%m-%d %H:%M"),'.png'), plot=map_out, width=6, height=6, units = "in")
  
  map_out <- ggmap(ca) + geom_polygon(data = rtp2,aes(x = long, y = lat, group = group, 
                                                      fill = rep(rtp[[1]], each = 5)), 
                                      size = 0, 
                                      alpha = 0.8) +
    scale_fill_gradient(low="white", high="red",limits=c(0, cmax), name = "PM2.5", oob=squish) +
    geom_point(data=points,aes(x=lon,y=lat),colour = "black") +
    ggtitle(paste(as.character(all_dates[d_slice]+12*3600),"NZST"))
  ggsave(filename=paste0(data_path,'idw2/',format(all_dates[d_slice]+12*3600,format = "%Y-%m-%d %H:%M"),'.png'),
         plot=map_out,
         width=6,
         height=6,
         units = "in")
  
}
save('raster_cat.idw',file = paste0(data_path,'raster_cat.idw.RData'))
save('raster_cat.idw2',file = paste0(data_path,'raster_cat.idw2.Rdata'))

print("Done with interpolating ...")

raster_cat_idw_LL <- projectRaster(raster_cat.idw,crs = "+proj=longlat +datum=WGS84")
raster_cat_idw2_LL <- projectRaster(raster_cat.idw2,crs = "+proj=longlat +datum=WGS84")
save(list = c('raster_cat_idw_LL','raster_cat_idw2_LL'),file = paste0(data_path,"raster_odin_LL_IDW.RData"))

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

# Write NetCDF files ####
# IDW
lat_dim <- unique(coordinates(raster_cat_idw_LL)[,2])
lon_dim <- unique(coordinates(raster_cat_idw_LL)[,1])
tim_dim <- all_dates[valid_dates==1]
nc.idw <- create.nc("odin_idw.nc")
# Dimensions specifications
dim.def.nc(nc.idw, "time", unlim=TRUE)
dim.def.nc(nc.idw, "latitude",length(lat_dim))
dim.def.nc(nc.idw, "longitude",length(lon_dim))
# Variable specifications
var.def.nc(nc.idw,"time","NC_INT","time")
att.put.nc(nc.idw,"time","units","NC_CHAR","seconds since 1970-01-01 00:00:0.0")
att.put.nc(nc.idw,"time","long_name","NC_CHAR","time")

var.def.nc(nc.idw,"latitude","NC_FLOAT","latitude")
att.put.nc(nc.idw,"latitude","units","NC_CHAR","degrees_north")
att.put.nc(nc.idw,"latitude","long_name","NC_CHAR","latitude")
att.put.nc(nc.idw,"latitude","standard_name","NC_CHAR","latitude")

var.def.nc(nc.idw,"longitude","NC_FLOAT","longitude")
att.put.nc(nc.idw,"longitude","units","NC_CHAR","degrees_east")
att.put.nc(nc.idw,"longitude","long_name","NC_CHAR","longitude")
att.put.nc(nc.idw,"longitude","standard_name","NC_CHAR","longitude")

var.def.nc(nc.idw,"pm2p5","NC_FLOAT",c("longitude","latitude","time"))
att.put.nc(nc.idw,"pm2p5","units","NC_CHAR","ug m**-3")
att.put.nc(nc.idw,"pm2p5","long_name","NC_CHAR","Mass concentration of PM2.5 ambient aerosol particles in air")
att.put.nc(nc.idw,"pm2p5","standard_name","NC_CHAR","mass_concentration_of_pm2p5_ambient_aerosol_particles_in_air")
att.put.nc(nc.idw,"pm2p5","cell_methods","NC_CHAR","time: mean (interval: 15 minutes)")
att.put.nc(nc.idw,"pm2p5","missing_value","NC_FLOAT",-999.9)

# Global attributes
att.put.nc(nc.idw,"NC_GLOBAL","title","NC_CHAR","PM2.5 interpolated surface (Inverse Square Distance)")
att.put.nc(nc.idw,"NC_GLOBAL","Conventions","NC_CHAR","CF-1.6")
att.put.nc(nc.idw,"NC_GLOBAL","Institution","NC_CHAR","NIWA (National Institute of Water and Atmospheric Research, Auckland, New Zealand)")
att.put.nc(nc.idw,"NC_GLOBAL","project_id","NC_CHAR","CONA - 2018")
att.put.nc(nc.idw,"NC_GLOBAL","history","NC_CHAR",paste0(format(max(all_data.tavg$date),format = "%Y%m%d"),
                                                         " Data generated and formatted"))
att.put.nc(nc.idw,"NC_GLOBAL","comment","NC_CHAR","Data for visualisation only")

# Load data
var.put.nc(nc.idw,"latitude",lat_dim)
var.put.nc(nc.idw,"longitude",lon_dim)
var.put.nc(nc.idw,"time",as.numeric(tim_dim))
rast_data <- getValues(raster_cat_idw_LL)[,(1:length(tim_dim))]
dim(rast_data) <- c(length(lon_dim),
                    length(lat_dim),
                    length(tim_dim))
var.put.nc(nc.idw,"pm2p5",rast_data)

# Close the file and save
close.nc(nc.idw)

# IDW2
lat_dim <- unique(coordinates(raster_cat_idw2_LL)[,2])
lon_dim <- unique(coordinates(raster_cat_idw2_LL)[,1])
tim_dim <- all_dates[valid_dates==1]
nc.idw2 <- create.nc("odin_idw2.nc")
# Dimensions specifications
dim.def.nc(nc.idw2, "time", unlim=TRUE)
dim.def.nc(nc.idw2, "latitude",length(lat_dim))
dim.def.nc(nc.idw2, "longitude",length(lon_dim))
# Variable specifications
var.def.nc(nc.idw2,"time","NC_INT","time")
att.put.nc(nc.idw2,"time","units","NC_CHAR","seconds since 1970-01-01 00:00:0.0")
att.put.nc(nc.idw2,"time","long_name","NC_CHAR","time")

var.def.nc(nc.idw2,"latitude","NC_FLOAT","latitude")
att.put.nc(nc.idw2,"latitude","units","NC_CHAR","degrees_north")
att.put.nc(nc.idw2,"latitude","long_name","NC_CHAR","latitude")
att.put.nc(nc.idw2,"latitude","standard_name","NC_CHAR","latitude")

var.def.nc(nc.idw2,"longitude","NC_FLOAT","longitude")
att.put.nc(nc.idw2,"longitude","units","NC_CHAR","degrees_east")
att.put.nc(nc.idw2,"longitude","long_name","NC_CHAR","longitude")
att.put.nc(nc.idw2,"longitude","standard_name","NC_CHAR","longitude")

var.def.nc(nc.idw2,"pm2p5","NC_FLOAT",c("longitude","latitude","time"))
att.put.nc(nc.idw2,"pm2p5","units","NC_CHAR","ug m**-3")
att.put.nc(nc.idw2,"pm2p5","long_name","NC_CHAR","Mass concentration of PM2.5 ambient aerosol particles in air")
att.put.nc(nc.idw2,"pm2p5","standard_name","NC_CHAR","mass_concentration_of_pm2p5_ambient_aerosol_particles_in_air")
att.put.nc(nc.idw2,"pm2p5","cell_methods","NC_CHAR","time: mean (interval: 15 minutes)")
att.put.nc(nc.idw2,"pm2p5","missing_value","NC_FLOAT",-999.9)

# Global attributes
att.put.nc(nc.idw2,"NC_GLOBAL","title","NC_CHAR","PM2.5 interpolated surface (Inverse Square Distance)")
att.put.nc(nc.idw2,"NC_GLOBAL","Conventions","NC_CHAR","CF-1.6")
att.put.nc(nc.idw2,"NC_GLOBAL","Institution","NC_CHAR","NIWA (National Institute of Water and Atmospheric Research, Auckland, New Zealand)")
att.put.nc(nc.idw2,"NC_GLOBAL","project_id","NC_CHAR","CONA - 2018")
att.put.nc(nc.idw2,"NC_GLOBAL","history","NC_CHAR",paste0(format(max(all_data.tavg$date),format = "%Y%m%d"),
                                                          " Data generated and formatted"))
att.put.nc(nc.idw2,"NC_GLOBAL","comment","NC_CHAR","Data for visualisation only")

# Load data
var.put.nc(nc.idw2,"latitude",lat_dim)
var.put.nc(nc.idw2,"longitude",lon_dim)
var.put.nc(nc.idw2,"time",as.numeric(tim_dim))
rast_data2 <- getValues(raster_cat_idw2_LL)[,(1:length(tim_dim))]
dim(rast_data2) <- c(length(lon_dim),
                     length(lat_dim),
                     length(tim_dim))
var.put.nc(nc.idw2,"pm2p5",rast_data2)

# Close the file and save
close.nc(nc.idw2)

## Create MP4 video ####
system(paste0("ffmpeg -f image2 -r 6 -pattern_type glob -i '",
              data_path,
              "idw/",
              "*.png' ",
              data_path,
              "idw_",
              format(min(all_data.tavg$date) + 12*3600,format = "%Y%m%d"),"_",
              format(max(all_data.tavg$date) + 12*3600,format = "%Y%m%d"),
              ".mp4"))

system(paste0("ffmpeg -f image2 -r 6 -pattern_type glob -i '",
              data_path,
              "idw2/",
              "*.png' ",
              data_path,
              "idw2_",
              format(min(all_data.tavg$date) + 12*3600,format = "%Y%m%d"),"_",
              format(max(all_data.tavg$date) + 12*3600,format = "%Y%m%d"),
              ".mp4"))



# Upload to youtube ####
system(paste0("youtube-upload --title=\"Alexandra ",
              format(min(all_data.tavg$date) + 12*3600,format = "%Y%m%d %H:%M"),
              " to ",
              format(max(all_data.tavg$date) + 12*3600,format = "%Y%m%d %H:%M"),
              "\" --client-secrets=client_secrets.json ",
              data_path,
              "idw2_",
              format(min(all_data.tavg$date) + 12*3600,format = "%Y%m%d"),"_",
              format(max(all_data.tavg$date) + 12*3600,format = "%Y%m%d"),
              ".mp4 --playlist=\"Alexandra 2018 - ODIN\""))

# Compress TXT files ####
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

RCurl::ftpUpload(paste0(data_path,"odin_idw.nc"),
                 "ftp://ftp.niwa.co.nz/incoming/GustavoOlivares/odin_alexandra/odin_idw_2anim.nc")
RCurl::ftpUpload(paste0(data_path,"odin_idw2.nc"),
                 "ftp://ftp.niwa.co.nz/incoming/GustavoOlivares/odin_alexandra/odin_idw2_2anim.nc")

RCurl::ftpUpload(paste0(data_path,
                        'all_data',
                        format(min(all_data.tavg$date) + 12*3600,format = "%Y%m%d"),"_",
                        format(max(all_data.tavg$date) + 12*3600,format = "%Y%m%d"),
                        ".tgz"),
                 paste0("ftp://ftp.niwa.co.nz/incoming/GustavoOlivares/odin_alexandra/",
                        'all_data_',
                        format(min(all_data.tavg$date) + 12*3600,format = "%Y%m%d"),"_",
                        format(max(all_data.tavg$date) + 12*3600,format = "%Y%m%d"),
                        ".tgz"))
RCurl::ftpUpload(paste0(data_path,
                        'all_dataAVG',
                        format(min(all_data.tavg$date) + 12*3600,format = "%Y%m%d"),"_",
                        format(max(all_data.tavg$date) + 12*3600,format = "%Y%m%d"),
                        ".tgz"),
                 paste0("ftp://ftp.niwa.co.nz/incoming/GustavoOlivares/odin_alexandra/",
                        'all_dataAVG_',
                        format(min(all_data.tavg$date) + 12*3600,format = "%Y%m%d"),"_",
                        format(max(all_data.tavg$date) + 12*3600,format = "%Y%m%d"),
                        ".tgz"))

## Remove files ####
# system(paste0("rm -rf ",
#               data_path,
#               "idw/*"))
# system(paste0("rm -rf ",
#               data_path,
#               "idw2/*"))
# system(paste0('rm -f ',
#               data_path,
#               'all_data',
#               format(min(all_data.tavg$date) + 12*3600,format = "%Y%m%d"),"_",
#               format(max(all_data.tavg$date) + 12*3600,format = "%Y%m%d"),
#               ".txt"))
# system(paste0('rm -f ',
#               data_path,
#               'all_dataAVG',
#               format(min(all_data.tavg$date) + 12*3600,format = "%Y%m%d"),"_",
#               format(max(all_data.tavg$date) + 12*3600,format = "%Y%m%d"),
#               ".txt"))
