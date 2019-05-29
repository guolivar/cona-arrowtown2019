## The data is here ... do the geo stuff



#Setting the  prediction grid properties
cellsize <- 50 #pixel size in projection units (NZTM, i.e. metres)
min_x <- all_data.10min@bbox[1,1] - cellsize#minimun x coordinate
min_y <- all_data.10min@bbox[2,1] - cellsize #minimun y coordinate
max_x <- all_data.10min@bbox[1,2] + cellsize #mximum x coordinate
max_y <- all_data.10min@bbox[2,2] + cellsize #maximum y coordinate

x_length <- max_x - min_x #easting amplitude
y_length <- max_y - min_y #northing amplitude

ncol <- round(x_length/cellsize,0) #number of columns in grid
nrow <- round(y_length/cellsize,0) #number of rows in grid

grid <- GridTopology(cellcentre.offset=c(min_x,min_y),cellsize=c(cellsize,cellsize),cells.dim=c(ncol,nrow))

#Convert GridTopolgy object to SpatialPixelsDataFrame object.
grid <- SpatialPixelsDataFrame(grid,
                               data=data.frame(id=1:prod(ncol,nrow)),
                               proj4string=CRS('+init=epsg:2193'))

all_dates <- sort(unique(all_data.10min$date))
ndates <- length(all_dates)
breaks <- as.numeric(quantile((1:ndates),c(0,0.2,0.4,0.6,0.8,1), type = 1))
nbreaks <- length(breaks)
fidx <- 1

for (j in (1:(nbreaks-1))){
  i <- 0
  if (j == 1){
    j1 <- 1
    j2 <- breaks[j+1]
  } else {
    j1 <- breaks[j]
    j2 <- breaks[j+1]
  }
  d_slice <- j1
  for (d_slice in (j1:j2)){
    print(all_dates[d_slice])
    c_data <- subset(all_data.10min,subset = (date==all_dates[d_slice]))
    
    surf.idw <- try(idw(PM2.5 ~ 1,newdata = grid, locations = c_data, idp = 1),silent = TRUE)
    if (length(surf.idw)<4){
      print("idw failed")
      next
    }
    surf.idw$timestamp <-d_slice
    proj4string(surf.idw) <- CRS('+init=epsg:2193')
    
    surf.idw2 <- idw(PM2.5 ~ 1,newdata = grid, locations = c_data, idp = 2)
    surf.idw2$timestamp <-d_slice
    proj4string(surf.idw2) <- CRS('+init=epsg:2193')
    
    if (i==0){
      
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
      
      to_rast.idw <- surf.idw
      r0.idw <- rasterFromXYZ(cbind(surf.idw@coords,surf.idw$var1.pred))
      crs(r0.idw) <- '+init=epsg:2193'
      raster_cat.idw<- addLayer(raster_cat.idw,r0.idw)
      
      to_rast.idw2 <- surf.idw2
      r0.idw2 <- rasterFromXYZ(cbind(surf.idw2@coords,surf.idw2$var1.pred))
      crs(r0.idw2) <- '+init=epsg:2193'
      raster_cat.idw2<- addLayer(raster_cat.idw2,r0.idw2)
    }
    print(all_dates[d_slice])
  }
  if (exists("raster_cat.idw")){
    save('raster_cat.idw',file = paste0('/data/data_gustavo/cona/raster_cat.idw.',fidx,'.RData'))
    save('raster_cat.idw2',file = paste0('/data/data_gustavo/cona/raster_cat.idw2.',fidx,'.Rdata'))
    rm('raster_cat.idw')
    rm('raster_cat.idw2')
  }
  fidx <- fidx + 1
}
print(fidx)
#fidx <- 6
for (i in (1:(fidx-1))){
  load(paste0('/data/data_gustavo/cona/raster_cat.idw.',i,'.RData'))
  load(paste0('/data/data_gustavo/cona/raster_cat.idw2.',i,'.Rdata'))
  if (i == 1){
    raster_stack.idw <- raster_cat.idw
    raster_stack.idw2 <- raster_cat.idw2
  } else {
    raster_stack.idw <- addLayer(raster_stack.idw,raster_cat.idw)
    raster_stack.idw2 <- addLayer(raster_stack.idw2,raster_cat.idw2)
  }
}


raster_cat_idw_LL <- projectRaster(raster_stack.idw,crs = "+proj=longlat +datum=WGS84")
raster_cat_idw2_LL <- projectRaster(raster_stack.idw2,crs = "+proj=longlat +datum=WGS84")
save(list = c('raster_cat_idw_LL','raster_cat_idw2_LL'),file = '/data/data_gustavo/cona/raster_odin_IDW_CONA2018.RData')

writeRaster(raster_cat_idw_LL, filename="./odin_CONA2018_idw.nc", overwrite=TRUE)
writeRaster(raster_cat_idw2_LL, filename="./odin_CONA2018_idw2.nc", overwrite=TRUE)

animation::saveGIF(animate(raster_cat_idw2_LL, pause=0.25, c(0,100),1),movie.name = "raster_test")

