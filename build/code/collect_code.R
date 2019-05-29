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

walk2(begin_seq, end_seq, byperiod_crawl)
df <- vroom(dir_ls("build/temp/DomesticContractInfo/"))
vroom_write(df, "build/output/domestic_cntrct.csv")

# Bid Information
## Facility Nego
base_url <- "http://openapi.d2b.go.kr/openapi/service/BidResultInfoService/"
operation <- "getFcltyOthbcVltrnNtatResultList?"

date_seq <- seq.Date(from = ymd(20060101),
                     to = ymd(20190101),
                     by = "1 year")

begin_seq <- date_seq[2:length(date_seq) - 1] %>% str_remove_all("-")
end_seq <- (date_seq[2:length(date_seq)] - ddays(1)) %>% str_remove_all("-")

get_totalCount <- function(ntatComptDateBegin, ntatComptDateEnd) {
  json <- glue(base_url, operation, ntatComptDateBegin, "&", ntatComptDateEnd) %>%
    jsonlite::fromJSON()
  
  json$response$body$totalCount
}

bypage_crawl <- function(p, ntatComptDateBegin, ntatComptDateEnd) {
  Sys.sleep(1)
  
  pageNo <- glue("pageNo=", p)
  numOfRows <- "numOfRows=10000"
  
  option <- glue(ntatComptDateBegin, ntatComptDateEnd, numOfRows, pageNo, .sep = "&")
  url <- glue(base_url, operation, option)
  json <- jsonlite::fromJSON(url)
  
  json$response$body$items %>%
    first() %>%
    mutate_all(as.character)
}

byperiod_crawl <- function(begin, end) {
  print(glue("Processing ", begin, "--", end))
  
  ntatComptDateBegin <- glue("ntatComptDateBegin=", begin)
  ntatComptDateEnd <- glue("ntatComptDateEnd=", end)
  
  totalCount <- get_totalCount(ntatComptDateBegin, ntatComptDateEnd)
  totalPage <- ceiling(totalCount/10000)
  
  df <- map_dfr(1:totalPage, bypage_crawl, ntatComptDateBegin, ntatComptDateEnd)
  write_csv(df, glue("build/temp/FacilityNegoResult/", begin, "_", end, ".csv"))
}

walk2(begin_seq, end_seq, byperiod_crawl)
df <- vroom(dir_ls("build/temp/FacilityNegoResult/"))
vroom_write(df, "build/output/facility_negoresult.csv")

## Facility Nego Detail
facility_negoresult <- read_delim("build/output/facility_negoresult.csv", delim = "\t")
operation <- "getFcltyOthbcVltrnNtatResultDetail?"

orntCode <- paste0("orntCode=", facility_negoresult$orntCode)
cntrwkNo <- paste0("cntrwkNo=", facility_negoresult$cntrwkNo)
ntatPlanDate <- paste0("ntatPlanDate=", facility_negoresult$ntatPlanDate)
pblancOdr <- paste0("pblancOdr=", facility_negoresult$pblancOdr)
option <- paste(orntCode, cntrwkNo, ntatPlanDate, pblancOdr, sep = "&")

url_seq <- paste0(base_url, operation, option)

byurl_crawl <- function(i) {
  
  print(glue("Processing ", url_seq[i]))
  Sys.sleep(0.1)
  url <- url_seq[i]
  
  json <- jsonlite::fromJSON(url)
  
  if (json$response$body == "") {
    return(NULL)
  }
  
  df <- json$response$body$item %>%
    bind_cols()
  
  write_csv(df, glue("build/temp/FacilityNegoDetail/", i, ".csv"))
}

walk(1:length(url_seq), byurl_crawl)
df <- dir_ls("build/temp/FacilityNegoDetail") %>%
  map_dfr(read_csv)

## Facility Competition
base_url <- "http://openapi.d2b.go.kr/openapi/service/BidResultInfoService/"
operation <- "getFcltyCmpetBidResultList?"

date_seq <- seq.Date(from = ymd(20060101),
                     to = ymd(20190101),
                     by = "1 year")

begin_seq <- date_seq[2:length(date_seq) - 1] %>% str_remove_all("-")
end_seq <- (date_seq[2:length(date_seq)] - ddays(1)) %>% str_remove_all("-")

get_totalCount <- function(opengDateBegin, opengDateEnd) {
  json <- glue(base_url, operation, opengDateBegin, "&", opengDateEnd) %>%
    jsonlite::fromJSON()
  
  json$response$body$totalCount
}

bypage_crawl <- function(p, opengDateBegin, opengDateEnd) {
  Sys.sleep(1)
  
  pageNo <- glue("pageNo=", p)
  numOfRows <- "numOfRows=10000"
  
  option <- glue(opengDateBegin, opengDateEnd, numOfRows, pageNo, .sep = "&")
  url <- glue(base_url, operation, option)
  json <- jsonlite::fromJSON(url)
  
  json$response$body$items %>%
    first() %>%
    mutate_all(as.character)
}

byperiod_crawl <- function(begin, end) {
  print(glue("Processing ", begin, "--", end))
  
  opengDateBegin <- glue("opengDateBegin=", begin)
  opengDateEnd <- glue("opengDateEnd=", end)
  
  totalCount <- get_totalCount(opengDateBegin, opengDateEnd)
  totalPage <- ceiling(totalCount/10000)
  
  df <- map_dfr(1:totalPage, bypage_crawl, opengDateBegin, opengDateEnd)
  write_csv(df, glue("build/temp/FacilityCompetitionResult/", begin, "_", end, ".csv"))
}

walk2(begin_seq, end_seq, byperiod_crawl)
df <- vroom::vroom(fs::dir_ls("build/temp/FacilityCompetitionResult/"))
vroom::vroom_write(df, "build/output/facility_cmptresult.csv")

## Facility Competition Detail
facility_cmptresult <- read_delim("build/output/facility_cmptresult.csv", delim = "\t")
operation <- "getFcltyCmpetBidResultDetail?"

cntrwkNo <- paste0("cntrwkNo=", facility_cmptresult$cntrwkNo)
opengDate <- paste0("opengDate=", facility_cmptresult$opengDate)
orntCode <- paste0("orntCode=", facility_cmptresult$orntCode)
pblancNo <- paste0("pblancNo=", facility_cmptresult$pblancNo)
pblancOdr <- paste0("pblancOdr=", facility_cmptresult$pblancOdr)
option <- paste(cntrwkNo, opengDate, orntCode, pblancNo, pblancOdr,  sep = "&")

url_seq <- paste0(base_url, operation, option)

byurl_crawl <- function(i) {
  
  print(glue("Processing ", url_seq[i]))
  Sys.sleep(0.1)
  url <- url_seq[i]
  
  json <- jsonlite::fromJSON(url)
  
  if (json$response$body == "") {
    return(NULL)
  }
  
  df <- json$response$body$item %>%
    bind_cols()
  
  write_csv(df, glue("build/temp/FacilityCompetitionDetail/", i, ".csv"))
}

walk(1:length(url_seq), byurl_crawl)
df <- fs::dir_ls("build/temp/FacilityCompetitionDetail") %>%
  map_dfr(read_csv, col_types = rep("c", 21) %>% paste0(collapse = ""))
write_excel_csv(df, "build/output/facility_cmptdetail.csv")

## Facility Competition Participants
facility_cmptresult <- read_delim("build/output/facility_cmptresult.csv", delim = "\t")
operation <- "getFcltyCmpetBidResultMnufList?"

orntCode <- paste0("orntCode=", facility_cmptresult$orntCode)
cntrwkNo <- paste0("cntrwkNo=", facility_cmptresult$cntrwkNo)
ntatPlanDate <- paste0("ntatPlanDate=", facility_cmptresult$opengDate)
option <- paste(cntrwkNo, orntCode, ntatPlanDate, sep = "&")

url_seq <- paste0(base_url, operation, option)

byurl_crawl <- function(i) {
  
  print(glue("Processing ", url_seq[i]))
  Sys.sleep(0.1)
  url <- url_seq[i]
  
  json <- jsonlite::fromJSON(url)
  
  if (json$response$body == "") {
    return(NULL)
  }
  
  df <- json$response$body$item %>%
    bind_cols()
  
  write_csv(df, glue("build/temp/FacilityCompetitionParticipant/", i, ".csv"))
}

str_extract(path_seq[1000], "\\d+(?=.csv)")

walk(1:length(url_seq), byurl_crawl)
path_seq <- fs::dir_ls("build/temp/FacilityCompetitionParticipant")
df <- map_dfr(path_seq, function(path){
  read_csv(path,
           col_types = rep("c", 13) %>% paste0(collapse = "")) %>%
    mutate(num = str_extract(path, "\\d+(?=.csv)"))
    })

df %>%
  distinct(bznsRgnb, mfkrName) %>%
  drop_na(bznsRgnb) %>%
  write_excel_csv("build/output/facility_cmptprtcpt_code.csv")

facility_cmptresult %>%
  rowid_to_column() %>%
  mutate(rowid = as.character(rowid)) %>%
  right_join(df, by = c("rowid" = "num")) %>%
  write_excel_csv("build/output/facility_cmptprtcpt.csv")
