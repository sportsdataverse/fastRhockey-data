library(fastRhockey)
library(tidyverse)

game_data <- read.csv("phf_meta_data.csv")

pbp_games <- game_data %>%
  dplyr::filter(has_play_by_play == TRUE) %>%
  dplyr::select(game_id) %>%
  dplyr::mutate(
    order = row_number()
  )

p <- list()

a <- Sys.time()

for (k in 1:nrow(pbp_games)) {
  
  use <- pbp_games %>%
    dplyr::filter(order == k)
  
  g <- use$game_id
  
  pbp_data <- load_phf_pbp(game_id = g)
  
  p[[k]] <- pbp_data
  
  print(paste0("Game ID: ", g, "; Game #: ", k))
  
}

(Sys.time() - a)

pbp_repo <- dplyr::bind_rows(p)

write.csv(pbp_repo, file = "play_by_play.csv", row.names = FALSE)

test <- load_pbp(game_id = 368721)
