library(tidyverse)
library(readxl)

firmID <- read_excel("build/input/DataGuide/DG_firmID_20190211.xlsx", skip = 5, col_types = rep("text", 4)) %>%
  set_names("id", "name", "bizno", "corpno") %>%
  mutate(bizno = str_remove_all(bizno, "-"))

cleaned_firmID <- firmID %>%
  arrange(bizno, id) %>%
  distinct(bizno, .keep_all = TRUE) %>%

cmptprtcpt_code <- read_csv("build/output/facility_cmptprtcpt_code.csv", col_types = "cc")

df <- cmptprtcpt_code %>%
  inner_join(firmID, by = c("bznsRgnb" = "bizno"))

df %>%
  arrange(bznsRgnb, id) %>%
  distinct(bznsRgnb, .keep_all = TRUE) %>%
  View()
  write_excel_csv("build/output/bizno_id_map.csv")
