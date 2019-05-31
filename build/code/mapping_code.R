library(tidyverse)
library(readxl)

firmID <- read_excel("build/input/DataGuide/DG_firmID_20190211.xlsx", skip = 5, col_types = rep("text", 4)) %>%
  set_names("id", "name", "bizno", "corpno") %>%
  mutate(bizno = str_remove_all(bizno, "-"))

cmptprtcpt_code <- read_csv("build/output/facility_cmptprtcpt_code.csv", col_types = "cc")

kospiCEO <- read_excel("build/input/TS2000/임원_KOSPI.xlsx", col_types = rep("text", 18))
kosdaqCEO <- read_excel("build/input/TS2000/임원_KOSDAQ.xlsx", col_types = rep("text", 18))
CEO_tbl <- bind_rows(kospiCEO, kosdaqCEO)

military_CEO <- CEO_tbl %>%
  # filter(str_detect(직명, "대표이사")) %>%
  filter_at(.vars = vars(contains("약력"), contains("경력")), 
            .vars_predicate = any_vars(str_detect(., "(육군|공군|해군)"))) %>%
  mutate(id = paste0("A", 거래소코드))

df <- cmptprtcpt_code %>%
  inner_join(firmID, by = c("bznsRgnb" = "bizno"))

df %>%
  arrange(bznsRgnb, id) %>%
  filter(str_detect(id, "^A")) %>%
  semi_join(military_CEO, by = "id") %>%
  View()
  distinct(bznsRgnb, .keep_all = TRUE) %>%
  write_excel_csv("build/output/bizno_id_map.csv")

