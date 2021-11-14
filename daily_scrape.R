library(powerplay)
library(dplyr)
library(tidyr)
library(readr)
library(purrr)
library(arrow)
library(glue)


# Play-by-Play Data Pull --------------------------------------------------
season_vector = 2006:2022

version = packageVersion("powerplay")

### scrape season schedule
### 
season_schedules <- purrr::map_dfr(season_vector, function(x){

  sched <- powerplay::nhl_schedule(season=x)
  ifelse(!dir.exists(file.path("nhl/schedules")), dir.create(file.path("nhl/schedules")), FALSE)
  ifelse(!dir.exists(file.path("nhl/schedules/csv")), dir.create(file.path("nhl/schedules/csv")), FALSE)
  ifelse(!dir.exists(file.path("nhl/schedules/qs")), dir.create(file.path("nhl/schedules/qs")), FALSE)
  ifelse(!dir.exists(file.path("nhl/schedules/rds")), dir.create(file.path("nhl/schedules/rds")), FALSE)
  ifelse(!dir.exists(file.path("nhl/schedules/parquet")), dir.create(file.path("nhl/schedules/parquet")), FALSE)
  data.table::fwrite(sched,paste0("nhl/schedules/csv/nhl_schedule_",x,".csv"))
  qs::qsave(sched,glue::glue('nhl/schedules/qs/nhl_schedule_{x}.qs'))
  saveRDS(sched, glue::glue('nhl/schedules/rds/nhl_schedule_{x}.rds'))
  arrow::write_parquet(sched, glue::glue('nhl/schedules/parquet/nhl_schedule_{x}.parquet'))
  
  return(sched)
})

pbp_list <- as.integer(gsub(".json","",list.files(path = glue::glue('nhl/json/'))))
season_schedules <- season_schedules %>% 
  dplyr::filter(.data$date <= Sys.Date(),
                !(.data$game_id %in% pbp_list))
purrr::map(season_schedules$game_id, function(x){
  game <- powerplay::nhl_game_feed(game_id = x)
  jsonlite::write_json(game, path = glue::glue("nhl/json/{x}.json"))
})