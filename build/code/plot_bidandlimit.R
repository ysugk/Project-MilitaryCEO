library(tidyverse)
library(ggthemes)
library(sparklyr)

sc <- spark_connect("local")

detail <- spark_read_csv(sc, name = "detail_tbl", "build/output/facility_cmptdetail.csv")
prtcpt <- spark_read_csv(sc, name = "prtcpt_tbl", "build/output/facility_cmptprtcpt.csv")
bizno_id_map <- read_csv("build/output/bizno_id_map.csv", col_types = "ccccc")

tidy_prtcpt <- select(prtcpt, -bidMth, -bidResult, -cntrctMth, -cntrwkNm, -ornt, -pblancSe, -sucbidrDecsnMth) %>%
  right_join(filter(detail,
                    bidResult == "낙찰",
                    between(bsisPreparPc*(1 + asessRtUplmt/100)/1.1, 5e9, 15e9)),
             by = c("opengDt", "cntrwkNo", "pblancNo", "pblancOdr")) %>%
  collect()

detail %>%
  filter(bidResult == "낙찰",
        between(bsisPreparPc*(1 + asessRtUplmt/100)/1.1, 5e9, 15e9)) %>%
  sdf_dim()

detail %>%
  filter(bidResult == "낙찰",
         between(bsisPreparPc*(1 + asessRtUplmt/100)/1.1, 5e9, 15e9)) %>%
  collect() %>%
  inner_join(bizno_id_map, by = c("scsbidEntrpsBsnmRegistNo" = "bznsRgnb", "scsbidEntrpsNm" = "mfkrName")) %>%
  count(id, name, sort = TRUE) %>%
  View()

limit_plot <- detail %>%
  collect() %>%
  drop_na(scsbidLwltRt) %>%
  mutate(scsbidLwltRt = scsbidLwltRt/100) %>%
  ggplot() +
  geom_point(aes(x = bsisPreparPc*(1 + asessRtUplmt/100)/1.1, y = scsbidLwltRt), shape = 4) +
  geom_vline(xintercept = 10e9, color = colorblind_pal()(8)[6], linetype = "dashed") +
  
  scale_x_continuous(name = "Estimated Cost (billion KRW)",
                     labels = function(x) x/1e9,
                     breaks = seq(7e9, 13e9, 1e9)) +
  scale_y_continuous(name = "Cutoff (%)",
                     labels = scales::percent,
                     breaks = seq(0.7, 1, 0.05)) +
  coord_cartesian(xlim = c(7e9, 13e9), ylim = c(0.7, 1)) +
  
  theme_few(base_family = "sans")

bid_plot <- tidy_prtcpt %>%
  mutate(running_var = bsisPreparPc*(1 + asessRtUplmt/100)/1.1,
         bidnRate = bidnRate/100,
         bucket = cut(running_var,
                      breaks = seq(5e9, 15e9, 0.5e9),
                      labels = seq(5.5e9, 15e9, 0.5e9) - 0.25e9,
                      include.lowest = TRUE) %>%
           as.character() %>%
           as.numeric()) %>%
  ggplot() +
  geom_hline(yintercept = 10e9, color = colorblind_pal()(8)[6], linetype = "dashed") +
  geom_density_ridges2(aes(x = bidnRate, y = bucket, group = bucket)) +
  
  scale_x_continuous(name = "Bid Price (%)",
                     labels = scales::percent,
                     breaks = seq(0.7, 1, 0.05)) +
  scale_y_continuous("Estimated Cost (billion KRW)",
                     breaks = seq(7e9, 13e9, 1e9),
                     labels = function(x) x/1e9) +
  
  coord_flip(xlim = c(0.7, 1), ylim = c(7e9, 13e9)) +
  
  theme_few(base_family = "sans")

win_plot <- tidy_prtcpt %>%
  filter(bidxNote == "낙찰") %>%
  mutate(bidnRate = bidnRate/100) %>%
  ggplot() +
  geom_point(aes(x = bsisPreparPc*(1 + asessRtUplmt/100)/1.1, y = bidnRate), shape = 17) +
  geom_vline(xintercept = 10e9, color = colorblind_pal()(8)[6], linetype = "dashed") +
  
  scale_x_continuous(name = "Estimated Cost (billion KRW)",
                     labels = function(x) x/1e9,
                     breaks = seq(7e9, 13e9, 1e9)) +
  scale_y_continuous(name = "Winning Price (%)",
                     labels = scales::percent,
                     breaks = seq(0.7, 1, 0.05)) +
  coord_cartesian(xlim = c(7e9, 13e9), ylim = c(0.7, 1)) +
  
  
  guides(color = FALSE) +
  theme_few(base_family = "sans")

multiple_plot <- gridExtra::grid.arrange(limit_plot, bid_plot, win_plot, nrow = 3)

ggsave("limit.pdf", plot = limit_plot, device = "pdf", units = "in",
       width = 8, height = 4, path = "paper-slides/figure")

ggsave("bid.pdf", plot = bid_plot, device = "pdf", units = "in",
       width = 8, height = 4, path = "paper-slides/figure")

ggsave("win.pdf", plot = win_plot, device = "pdf", units = "in",
       width = 8, height = 4, path = "paper-slides/figure")

ggsave("limit_bid_win.pdf", plot = multiple_plot, device = "pdf", units = "in",
       width = 8, height = 12, path = "paper-slides/figure")
