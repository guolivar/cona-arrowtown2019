#
# This is the server logic of a Shiny web application. You can run the 
# application by clicking 'Run App' above.
#
# Find out more about building applications with Shiny here:
# 
#    http://shiny.rstudio.com/
#

library(readr)
library(shiny)
library(RJSONIO)
library(curl)
library(base64enc)
# Define server logic required to draw a histogram

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
curr_data <- data.frame(deviceid = (1:nsites))
for (i in (1:nsites)){
  curr_data$deviceid[i] <- jreq1[[i]]$id
}

# Get the latest measurements
base_url <- "https://dashboard.hologram.io/api/1/csr/rdm?"
curr_data$PM1 <- -1
curr_data$PM2.5 <- -1
curr_data$PM10 <- -1
curr_data$Temperature <- -99
curr_data$RH <- -1
curr_data$Timestamp <- as.POSIXct("2018-05-01 00:00:00",tz='UTC')
i <- 1
for (i in (1:nsites)){
  built_url <- paste0(base_url,
                      "deviceid=",curr_data$deviceid[i],"&",
                      "limit=1&",
                      "orgid=",secret_hologram$orgid,"&",
                      "apikey=",secret_hologram$apikey)
  req2 <- curl_fetch_memory(built_url)
  jreq2 <- fromJSON(rawToChar(req2$content))$data
  payload <- fromJSON(rawToChar(base64decode(fromJSON(jreq2[[1]]$data)$data)))
  curr_data$Timestamp[i] <- as.POSIXct(jreq2[[1]]$logged,tz='UTC')
  curr_data$PM1[i] <- payload[1]
  curr_data$PM2.5[i] <- payload[2]
  curr_data$PM10[i] <- payload[3]
  curr_data$Temperature[i] <- payload[7]
  curr_data$RH[i] <- payload[8]
}

c_plot <- ggplot(data = curr_data,aes(x=deviceid))+
  geom_bar(aes(y=PM1),stat = StatIdentity) +
  geom_text(aes(y=PM1,label=Timestamp),hjust=0, vjust=0) +
  ylim(0,max(curr_data$PM1))


shinyServer(function(input, output) {
  
  output$distPlot <- renderPlot({
    
    ggplot(data = curr_data,aes(x=deviceid))+
      geom_bar(aes(y=PM1),stat = StatIdentity) +
      geom_text(aes(y=PM1,label=Timestamp),hjust=0, vjust=0) +
      ylim(0,max(curr_data$PM1))
    
  })
  
})
