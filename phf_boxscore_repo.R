library(fastRhockey)
library(tidyverse)
library(lubridate)

game_data <- read.csv("phf_meta_data.csv")

box_games <- game_data %>%
  mutate(date = date(datetime)) %>%
  dplyr::select(game_id, date) %>%
  dplyr::mutate(
    order = row_number()
  )

b <- list()

a <- Sys.time()

for (k in 1:nrow(box_games)) {
  
  use <- box_games %>%
    dplyr::filter(order == k)
  
  g <- use$game_id
  dr <- use$date
  
  box_data <- load_phf_boxscore(game_id = g) %>%
    mutate(date = dr)
  
  b[[k]] <- box_data
  
  print(paste0("Game ID: ", g, "; Game #: ", k))
  
}

(Sys.time() - a)

box_repo <- dplyr::bind_rows(b)

write.csv(box_repo, file = "boxscore.csv", row.names = FALSE)
