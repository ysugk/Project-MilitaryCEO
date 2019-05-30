library(tidyverse)
library(ggthemes)

detail <- read_csv("build/output/facility_cmptdetail.csv")
prtcpt <- read_csv("build/output/facility_cmptprtcpt.csv")

prtcpt <- detail %>%
  filter(bidResult == "낙찰") %>%
  left_join(select(prtcpt, -bidMth, -bidResult, -cntrctMth, -cntrwkNm, -ornt, -pblancSe, -sucbidrDecsnMth),
            by = c("opengDt", "cntrwkNo", "pblancNo", "pblancOdr"))

limit_plot <- detail %>%
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

bid_plot <- prtcpt %>%
  drop_na(bidnRate, bidxNote) %>%
  mutate(below = if_else(bidxNote == "낙찰하한가미만", "Yes", "No"),
         bidnRate = bidnRate/100) %>%
  ggplot() +
  geom_point(aes(x = bsisPreparPc*(1 + asessRtUplmt/100)/1.1, y = bidnRate, color = below),
             alpha = 0.5) +
  
  geom_vline(xintercept = 10e9, color = colorblind_pal()(8)[6], linetype = "dashed") +
  
  scale_x_continuous(name = "Estimated Cost (billion KRW)",
                     labels = function(x) x/1e9,
                     breaks = seq(7e9, 13e9, 1e9)) +
  scale_y_continuous(name = "Bid Price (%)",
                     labels = scales::percent,
                     breaks = seq(0.7, 1, 0.05)) +
  scale_color_colorblind(name = "Below Cutoff") +
  coord_cartesian(xlim = c(7e9, 13e9), ylim = c(0.7, 1)) +

  
  guides(color = FALSE) +
  theme_few(base_family = "sans")

win_plot <- prtcpt %>%
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




