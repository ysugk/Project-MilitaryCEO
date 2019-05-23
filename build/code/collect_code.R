library(tidyverse)
library(rvest)
library(glue)
library(lubridate)

# Demand Institution Code
base_url <- "http://openapi.d2b.go.kr/openapi/service/CodeInqireService/"
operation <- "getOrntCodeList?"
option <- "numOfRows=10000"

url <- glue(base_url, operation, option)
json <- jsonlite::fromJSON(url)

dmndinst <- json$response$body$items %>%
  first()

write_csv(dmndinst, "build/output/dmndinst.csv")

# Contract Data
base_url <- "http://openapi.d2b.go.kr/openapi/service/CntrctInfoService/"
operation <- "getFcltyCntrctInfoList?"

date_seq <- seq.Date(from = ymd(20000101),
                     to = ymd(20181231),
                     by = "1 year")

date_begin <- date_seq[1:18] %>% str_remove_all("-")
date_end <- (date_seq[2:19] - ddays(1)) %>% str_remove_all("-")

cntrctDateBegin <- "cntrctDateBegin=20000101"
cntrctDateEnd <- "cntrctDateEnd=20001231"
numOfRows <- "numOfRows=10000"

option <- glue(cntrctDateBegin, cntrctDateEnd, numOfRows, .sep = "&")

get_totalCount <- function(cntrctDateBegin, cntrctDateEnd) {
  json <- glue(base_url, operation, cntrctDateBegin, "&", cntrctDateEnd) %>%
    jsonlite::fromJSON()
    
  json$response$body$totalCount
}

totalCount <- get_totalCount(cntrctDateBegin, cntrctDateEnd)

url <- glue(base_url, operation, option)
json <- jsonlite::fromJSON(url)

# Bid Information
json <- "http://openapi.d2b.go.kr/openapi/service/BidResultInfoService/getFcltyOthbcVltrnNtatResultMnufList" %>%
  jsonlite::fromJSON()
