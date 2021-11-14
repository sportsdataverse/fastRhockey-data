library(powerplay)
library(dplyr)
library(tidyr)
library(readr)
library(purrr)
library(arrow)
library(glue)


# Play-by-Play Data Pull --------------------------------------------------
season_vector = 2012

version = packageVersion("powerplay")

### scrape season schedule

season_schedules <- purrr::map_dfr(season_vector, function(x){
  df <- powerplay::nhl_schedule(season=x)
  return(df)
})

pbp_list <- as.integer(gsub(".json","",list.files(path = glue::glue('nhl/json/'))))
season_schedules <- season_schedules %>% 
  dplyr::filter(.data$date <= Sys.Date(),
                !(.data$game_id %in% pbp_list))
purrr::map(season_schedules$game_id, function(x){
  game <- powerplay::nhl_game_feed(game_id = x)
  jsonlite::write_json(game, path = glue::glue("nhl/json/{x}.json"))
})