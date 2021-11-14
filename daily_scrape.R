library(powerplay)
library(dplyr)
library(tidyr)
library(readr)
library(furrr)
library(purrr)
library(future)
library(progressr)
library(arrow)
library(glue)
library(qs)


# Play-by-Play Data Pull --------------------------------------------------
season_vector <- 2013:2017
rebuild <- TRUE
version = packageVersion("powerplay")

### 1a) scrape season schedule
### 1b) save to disk
season_schedules <- purrr::map_dfr(season_vector, function(x){

  sched <- powerplay::nhl_schedule(season=x) %>% 
    dplyr::tibble()
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

### 1c) filter schedule to unscraped games
pbp_list <- as.integer(gsub(".json","",list.files(path = glue::glue('nhl/json/'))))

season_schedules <- season_schedules %>% 
  dplyr::filter(.data$status_status_code == 7)

if(rebuild == FALSE){
  pbp_list <- as.integer(gsub(".json","",list.files(path = glue::glue('nhl/json/'))))
  season_schedules <- season_schedules %>% 
    dplyr::filter(!(.data$game_id %in% pbp_list))
}
### 2a) scrape game json
### 2b) save json to disk
cli::cli_process_start("Starting scrape of {length(season_schedules$game_id)} games...")

future::plan("multisession")
scrape_games <- furrr::future_map(season_schedules$game_id, function(x){
  game <- powerplay::nhl_game_feed(game_id = x)
  jsonlite::write_json(game, path = glue::glue("nhl/json/{x}.json"))
})
cli::cli_process_done(msg_done = "Finished scrape of {length(season_schedules$game_id)} games!")
### 3a) Build play-by-play dataset
season_pbp_compile <- purrr::map(season_vector,function(x){
  sched <- data.table::fread(paste0("nhl/schedules/csv/nhl_schedule_",x,".csv"))
  sched <- sched %>% 
    dplyr::filter(.data$status_status_code == 7)
  future::plan("multisession")
  season_pbp <- furrr::future_map_dfr(sched$game_id,function(y){
    game <- jsonlite::fromJSON(glue::glue("nhl/json/{y}.json"))
    pbp <- game$all_plays
    return(pbp)
  })
  ifelse(!dir.exists(file.path("nhl/pbp")), dir.create(file.path("nhl/pbp")), FALSE)
  ifelse(!dir.exists(file.path("nhl/pbp/csv")), dir.create(file.path("nhl/pbp/csv")), FALSE)
  if(nrow(season_pbp)>1){
    data.table::fwrite(season_pbp, file=paste0("nhl/pbp/csv/play_by_play_",x,".csv.gz"))
    
    ifelse(!dir.exists(file.path("nhl/pbp/qs")), dir.create(file.path("nhl/pbp/qs")), FALSE)
    qs::qsave(season_pbp,glue::glue("nhl/pbp/qs/play_by_play_{x}.qs"))
    
    ifelse(!dir.exists(file.path("nhl/pbp/rds")), dir.create(file.path("nhl/pbp/rds")), FALSE)
    saveRDS(season_pbp,glue::glue("nhl/pbp/rds/play_by_play_{x}.rds"))
    
    ifelse(!dir.exists(file.path("nhl/pbp/parquet")), dir.create(file.path("nhl/pbp/parquet")), FALSE)
    arrow::write_parquet(season_pbp, glue::glue("nhl/pbp/parquet/play_by_play_{x}.parquet"))
  }
})

### 3b) Build team boxscore dataset
season_team_box_compile <- purrr::map(season_vector,function(x){
  sched <- data.table::fread(paste0("nhl/schedules/csv/nhl_schedule_",x,".csv"))
  sched <- sched %>% 
    dplyr::filter(.data$status_status_code == 7)
  future::plan("multisession")
  season_team_box <- furrr::future_map_dfr(sched$game_id,function(y){
    game <- jsonlite::fromJSON(glue::glue("nhl/json/{y}.json"))
    team_box <- game$team_box
    return(team_box)
  })
  if(nrow(season_team_box)>1){
    ifelse(!dir.exists(file.path("nhl/team_box")), dir.create(file.path("nhl/team_box")), FALSE)
    ifelse(!dir.exists(file.path("nhl/team_box/csv")), dir.create(file.path("nhl/team_box/csv")), FALSE)
    data.table::fwrite(season_team_box, file=paste0("nhl/team_box/csv/team_box_",x,".csv.gz"))
    
    ifelse(!dir.exists(file.path("nhl/team_box/qs")), dir.create(file.path("nhl/team_box/qs")), FALSE)
    qs::qsave(season_team_box,glue::glue("nhl/team_box/qs/team_box_{x}.qs"))
    
    ifelse(!dir.exists(file.path("nhl/team_box/rds")), dir.create(file.path("nhl/team_box/rds")), FALSE)
    saveRDS(season_team_box,glue::glue("nhl/team_box/rds/team_box_{x}.rds"))
    
    ifelse(!dir.exists(file.path("nhl/team_box/parquet")), dir.create(file.path("nhl/team_box/parquet")), FALSE)
    arrow::write_parquet(season_team_box, glue::glue("nhl/team_box/parquet/team_box_{x}.parquet"))
  }
})

### 3c) Build player boxscore dataset
season_player_box_compile <- purrr::map(season_vector,function(x){
  sched <- data.table::fread(paste0("nhl/schedules/csv/nhl_schedule_",x,".csv"))
  sched <- sched %>% 
    dplyr::filter(.data$status_status_code == 7)
  future::plan("multisession")
  season_player_box <- furrr::future_map_dfr(sched$game_id,function(y){
    game <- jsonlite::fromJSON(glue::glue("nhl/json/{y}.json"))
    player_box <- game$players_box
    return(player_box)
  })
  if(nrow(season_player_box)>1){
    ifelse(!dir.exists(file.path("nhl/player_box")), dir.create(file.path("nhl/player_box")), FALSE)
    ifelse(!dir.exists(file.path("nhl/player_box/csv")), dir.create(file.path("nhl/player_box/csv")), FALSE)
    data.table::fwrite(season_player_box, file=paste0("nhl/player_box/csv/player_box_",x,".csv.gz"))
    
    ifelse(!dir.exists(file.path("nhl/player_box/qs")), dir.create(file.path("nhl/player_box/qs")), FALSE)
    qs::qsave(season_player_box,glue::glue("nhl/player_box/qs/player_box_{x}.qs"))
    
    ifelse(!dir.exists(file.path("nhl/player_box/rds")), dir.create(file.path("nhl/player_box/rds")), FALSE)
    saveRDS(season_player_box,glue::glue("nhl/player_box/rds/player_box_{x}.rds"))
    
    ifelse(!dir.exists(file.path("nhl/player_box/parquet")), dir.create(file.path("nhl/player_box/parquet")), FALSE)
    arrow::write_parquet(season_player_box, glue::glue("nhl/player_box/parquet/player_box_{x}.parquet"))
  }
})