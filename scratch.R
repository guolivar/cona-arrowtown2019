#scratch

built_url <- paste0(base_url,
                    "deviceid=165080&",
                    "limit=10&",
                    "timestart=1526249648&",
                    "timeend=1526250848&",
                    "orgid=",secret_hologram$orgid,"&",
                    "apikey=",secret_hologram$apikey)
req2 <- curl_fetch_memory(built_url)
jreq2 <- fromJSON(rawToChar(req2$content))$data
payload <- fromJSON(rawToChar(base64decode(fromJSON(jreq2[[1]]$data)$data)))
jreq2[[1]]$logged
