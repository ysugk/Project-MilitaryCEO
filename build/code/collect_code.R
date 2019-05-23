library(tidyverse)
library(rvest)
library(glue)
library(lubridate)
library(fs, lib.loc = "~/r-packages/")
library(vroom, lib.loc = "~/r-packages/")

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
## Facility
base_url <- "http://openapi.d2b.go.kr/openapi/service/CntrctInfoService/"
operation <- "getFcltyCntrctInfoList?"

date_seq <- seq.Date(from = ymd(20000101),
                     to = ymd(20190101),
                     by = "1 year")

begin_seq <- date_seq[2:length(date_seq) - 1] %>% str_remove_all("-")
end_seq <- (date_seq[2:length(date_seq)] - ddays(1)) %>% str_remove_all("-")

get_totalCount <- function(cntrctDateBegin, cntrctDateEnd) {
  json <- glue(base_url, operation, cntrctDateBegin, "&", cntrctDateEnd) %>%
    jsonlite::fromJSON()
    
  json$response$body$totalCount
}

bypage_crawl <- function (p, cntrctDateBegin, cntrctDateEnd) {
  Sys.sleep(1)
  
  pageNo <- glue("pageNo=", p)
  numOfRows <- "numOfRows=10000"
  
  option <- glue(cntrctDateBegin, cntrctDateEnd, numOfRows, pageNo, .sep = "&")
  url <- glue(base_url, operation, option)
  json <- jsonlite::fromJSON(url)
  
  json$response$body$items %>%
    first() %>%
    mutate_all(as.character)
}

byperiod_crawl <- function(begin, end) {
  print(glue("Processing ", begin, "--", end))
  
  cntrctDateBegin <- glue("cntrctDateBegin=", begin)
  cntrctDateEnd <- glue("cntrctDateEnd=", end)
  
  totalCount <- get_totalCount(cntrctDateBegin, cntrctDateEnd)
  totalPage <- ceiling(totalCount/10000)
  
  df <- map_dfr(1:totalPage, bypage_crawl, cntrctDateBegin, cntrctDateEnd)
  write_csv(df, glue("build/temp/FacilityContractInfo/", begin, "_", end, ".csv"))
}

walk2(begin_seq, end_seq, byperiod_crawl)
df <- vroom(dir_ls("build/temp/FacilityContractInfo/"))
vroom_write(df, "build/output/facility_cntrct.csv")

## Out-nation
operation <- "getOutnatnCntrctInfoList?"

byperiod_crawl <- function(begin, end) {
  print(glue("Processing ", begin, "--", end))
  
  cntrctDateBegin <- glue("cntrctDateBegin=", begin)
  cntrctDateEnd <- glue("cntrctDateEnd=", end)
  
  totalCount <- get_totalCount(cntrctDateBegin, cntrctDateEnd)
  totalPage <- ceiling(totalCount/10000)
  
  df <- map_dfr(1:totalPage, bypage_crawl, cntrctDateBegin, cntrctDateEnd)
  write_csv(df, glue("build/temp/OutNationContractInfo/", begin, "_", end, ".csv"))
}

walk2(begin_seq, end_seq, byperiod_crawl)
df <- vroom(dir_ls("build/temp/OutNationContractInfo/"))
vroom_write(df, "build/output/outnation_cntrct.csv")

## Domestic
operation <- "getDmstcCntrctInfoList?"

byperiod_crawl <- function(begin, end) {
  print(glue("Processing ", begin, "--", end))
  
  cntrctDateBegin <- glue("cntrctDateBegin=", begin)
  cntrctDateEnd <- glue("cntrctDateEnd=", end)
  
  totalCount <- get_totalCount(cntrctDateBegin, cntrctDateEnd)
  totalPage <- ceiling(totalCount/10000)
  
  df <- map_dfr(1:totalPage, bypage_crawl, cntrctDateBegin, cntrctDateEnd)
  write_csv(df, glue("build/temp/DomesticContractInfo/", begin, "_", end, ".csv"))
}

walk2(begin_seq[10:19], end_seq[10:19], byperiod_crawl)
df <- vroom(dir_ls("build/temp/DomesticContractInfo/"))
vroom_write(df, "build/output/domestic_cntrct.csv")

# Bid Information

