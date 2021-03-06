.libPaths("C:\\Users\\saiem\\Documents\\R\\win-library\\4.1")
Sys.setenv(R_LIBS="C:\\Users\\saiem\\Documents\\R\\win-library\\4.1")
if (!requireNamespace('pacman', quietly = TRUE)){
  install.packages('pacman', lib=Sys.getenv("R_LIBS"), repos='http://cran.us.r-project.org')
}
pacman::p_load_current_gh("BenHowell71/fastRhockey")
suppressPackageStartupMessages(suppressMessages(library(fastRhockey, lib.loc="C:\\Users\\saiem\\Documents\\R\\win-library\\4.1")))
suppressPackageStartupMessages(suppressMessages(library(dplyr, lib.loc="C:\\Users\\saiem\\Documents\\R\\win-library\\4.1")))
suppressPackageStartupMessages(suppressMessages(library(magrittr, lib.loc="C:\\Users\\saiem\\Documents\\R\\win-library\\4.1")))
suppressPackageStartupMessages(suppressMessages(library(jsonlite, lib.loc="C:\\Users\\saiem\\Documents\\R\\win-library\\4.1")))
suppressPackageStartupMessages(suppressMessages(library(furrr, lib.loc="C:\\Users\\saiem\\Documents\\R\\win-library\\4.1")))
suppressPackageStartupMessages(suppressMessages(library(purrr, lib.loc="C:\\Users\\saiem\\Documents\\R\\win-library\\4.1")))
suppressPackageStartupMessages(suppressMessages(library(progressr, lib.loc="C:\\Users\\saiem\\Documents\\R\\win-library\\4.1")))
suppressPackageStartupMessages(suppressMessages(library(data.table, lib.loc="C:\\Users\\saiem\\Documents\\R\\win-library\\4.1")))
suppressPackageStartupMessages(suppressMessages(library(qs, lib.loc="C:\\Users\\saiem\\Documents\\R\\win-library\\4.1")))
suppressPackageStartupMessages(suppressMessages(library(arrow, lib.loc="C:\\Users\\saiem\\Documents\\R\\win-library\\4.1")))


# Play-by-Play Data Pull --------------------------------------------------
season_vector <- fastRhockey::most_recent_nhl_season()
rebuild <- FALSE
rebuild_from_existing_json <- FALSE
version = packageVersion("fastRhockey")
### 1a) scrape season schedule
### 1b) save to disk
season_schedules <- purrr::map_dfr(season_vector, function(x){
  
  cli::cli_process_start("Starting scrape of {x} NHL season schedule...")
  sched <- fastRhockey::nhl_schedule(season=x) %>% 
    dplyr::tibble() %>% 
    dplyr::mutate(season=x)
  ifelse(!dir.exists(file.path("nhl/schedules")), dir.create(file.path("nhl/schedules")), FALSE)
  ifelse(!dir.exists(file.path("nhl/schedules/csv")), dir.create(file.path("nhl/schedules/csv")), FALSE)
  ifelse(!dir.exists(file.path("nhl/schedules/qs")), dir.create(file.path("nhl/schedules/qs")), FALSE)
  ifelse(!dir.exists(file.path("nhl/schedules/rds")), dir.create(file.path("nhl/schedules/rds")), FALSE)
  ifelse(!dir.exists(file.path("nhl/schedules/parquet")), dir.create(file.path("nhl/schedules/parquet")), FALSE)
  sched <- sched %>%
    fastRhockey:::make_fastRhockey_data("NHL Schedule Information from fastRhockey data repository",Sys.time())
  data.table::fwrite(sched,paste0("nhl/schedules/csv/nhl_schedule_",x,".csv"))
  qs::qsave(sched,glue::glue('nhl/schedules/qs/nhl_schedule_{x}.qs'))
  saveRDS(sched, glue::glue('nhl/schedules/rds/nhl_schedule_{x}.rds'))
  arrow::write_parquet(sched, glue::glue('nhl/schedules/parquet/nhl_schedule_{x}.parquet'))
  
  cli::cli_process_done(msg_done = "Finished scrape of {x} NHL season schedule!")
  Sys.sleep(15)
  return(sched)
})
season_schedules <- purrr::map_dfr(season_vector, function(x){
  sched <- data.table::fread(paste0("nhl/schedules/csv/nhl_schedule_",x,".csv"))
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
if(rebuild_from_existing_json == FALSE){
  cli::cli_process_start("Starting scrape of {length(season_schedules$game_id)} NHL games...")
  
  future::plan("multisession")
  scrape_games <- furrr::future_map(season_schedules$game_id, function(x){
    game <- fastRhockey::nhl_game_feed(game_id = x)
    jsonlite::write_json(game, path = glue::glue("nhl/json/{x}.json"))
  })
  cli::cli_process_done(msg_done = "Finished scrape of {length(season_schedules$game_id)} NHL games!")
}

### 3a) Build play-by-play dataset
season_pbp_compile <- purrr::map(season_vector,function(x){
  
  cli::cli_process_start("Starting NHL play-by-play compilation for {x} season...")
  sched <- data.table::fread(paste0("nhl/schedules/csv/nhl_schedule_",x,".csv"))
  sched <- sched %>% 
    dplyr::filter(.data$status_status_code == 7)
  future::plan("multisession")
  season_pbp <- furrr::future_map_dfr(sched$game_id,function(y){
    game <- jsonlite::fromJSON(paste0("nhl/json/",y,".json"))
    pbp <- game$all_plays
    pbp$game_id <- y
    return(pbp)
  })
  ifelse(!dir.exists(file.path("nhl/pbp")), dir.create(file.path("nhl/pbp")), FALSE)
  ifelse(!dir.exists(file.path("nhl/pbp/csv")), dir.create(file.path("nhl/pbp/csv")), FALSE)
  if(nrow(season_pbp)>1){
    season_pbp$season <- x
    season_pbp <- season_pbp %>%
      fastRhockey:::make_fastRhockey_data("NHL Play-by-Play Information from fastRhockey data repository",Sys.time())
    data.table::fwrite(season_pbp, file=paste0("nhl/pbp/csv/play_by_play_",x,".csv.gz"))
    
    ifelse(!dir.exists(file.path("nhl/pbp/qs")), dir.create(file.path("nhl/pbp/qs")), FALSE)
    qs::qsave(season_pbp,glue::glue("nhl/pbp/qs/play_by_play_{x}.qs"))
    
    ifelse(!dir.exists(file.path("nhl/pbp/rds")), dir.create(file.path("nhl/pbp/rds")), FALSE)
    saveRDS(season_pbp,glue::glue("nhl/pbp/rds/play_by_play_{x}.rds"))
    
    ifelse(!dir.exists(file.path("nhl/pbp/parquet")), dir.create(file.path("nhl/pbp/parquet")), FALSE)
    arrow::write_parquet(season_pbp, glue::glue("nhl/pbp/parquet/play_by_play_{x}.parquet"))
  }
  if(nrow(season_pbp)>0){
    sched <- sched %>%
      dplyr::mutate(
        PBP = ifelse(.data$game_id %in% unique(season_pbp$game_id), TRUE,FALSE))
  } else {
    sched$PBP <- FALSE
  }
  
  final_sched <- dplyr::distinct(sched) %>% dplyr::arrange(desc(.data$game_date)) %>%
    fastRhockey:::make_fastRhockey_data("NHL Schedule Information from fastRhockey data repository",Sys.time())
  data.table::fwrite(final_sched,paste0("nhl/schedules/csv/nhl_schedule_",x,".csv"))
  qs::qsave(final_sched,glue::glue('nhl/schedules/qs/nhl_schedule_{x}.qs'))
  saveRDS(final_sched, glue::glue('nhl/schedules/rds/nhl_schedule_{x}.rds'))
  arrow::write_parquet(final_sched, glue::glue('nhl/schedules/parquet/nhl_schedule_{x}.parquet'))
  
  cli::cli_process_done(msg_done = "Finished NHL play-by-play compilation for {x} season!")
  rm(sched)
  rm(final_sched)
  rm(season_pbp)
})

### 3b) Build team boxscore dataset
season_team_box_compile <- purrr::map(season_vector,function(x){
  cli::cli_process_start("Starting NHL team boxscore compilation for {x} season...")
  sched <- data.table::fread(paste0("nhl/schedules/csv/nhl_schedule_",x,".csv"))
  sched <- sched %>% 
    dplyr::filter(.data$status_status_code == 7)
  future::plan("multisession")
  season_team_box <- furrr::future_map_dfr(sched$game_id,function(y){
    game <- jsonlite::fromJSON(paste0("nhl/json/",y,".json"))
    team_box <- game$team_box
    team_box$game_id <- y
    return(team_box)
  })
  if(nrow(season_team_box)>1){
    season_team_box$season <- x
    season_team_box <- season_team_box %>%
      fastRhockey:::make_fastRhockey_data("NHL Team Boxscore Information from fastRhockey data repository",Sys.time())
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
  if(nrow(season_team_box)>0){
    sched <- sched %>%
      dplyr::mutate(
        team_box = ifelse(.data$game_id %in% unique(season_team_box$game_id), TRUE,FALSE))
  } else {
    sched$team_box <- FALSE
  }
  
  final_sched <- dplyr::distinct(sched) %>% dplyr::arrange(desc(.data$game_date)) %>%
    fastRhockey:::make_fastRhockey_data("NHL Schedule Information from fastRhockey data repository",Sys.time())
  data.table::fwrite(final_sched,paste0("nhl/schedules/csv/nhl_schedule_",x,".csv"))
  qs::qsave(final_sched,glue::glue('nhl/schedules/qs/nhl_schedule_{x}.qs'))
  saveRDS(final_sched, glue::glue('nhl/schedules/rds/nhl_schedule_{x}.rds'))
  arrow::write_parquet(final_sched, glue::glue('nhl/schedules/parquet/nhl_schedule_{x}.parquet'))
  
  cli::cli_process_done(msg_done = "Finished NHL team boxscore compilation for {x} season!")
  rm(sched)
  rm(final_sched)
  rm(season_team_box)
})

### 3c) Build player boxscore dataset
season_player_box_compile <- purrr::map(season_vector,function(x){
  cli::cli_process_start("Starting NHL player boxscore compilation for {x} season...")
  sched <- data.table::fread(paste0("nhl/schedules/csv/nhl_schedule_",x,".csv"))
  sched <- sched %>% 
    dplyr::filter(.data$status_status_code == 7)
  future::plan("multisession")
  season_player_box <- furrr::future_map_dfr(sched$game_id,function(y){
    game <- jsonlite::fromJSON(paste0("nhl/json/",y,".json"))
    player_box <- game$player_box
    player_box$game_id <- y
    return(player_box)
  })
  if(nrow(season_player_box)>1){
    season_player_box$season <- x
    season_player_box <- season_player_box %>%
      fastRhockey:::make_fastRhockey_data("NHL Player Boxscore Information from fastRhockey data repository",Sys.time())
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
  if(nrow(season_player_box)>0){
    sched <- sched %>%
      dplyr::mutate(
        player_box = ifelse(.data$game_id %in% unique(season_player_box$game_id), TRUE,FALSE))
  } else {
    sched$player_box <- FALSE
  }
  
  final_sched <- dplyr::distinct(sched) %>% dplyr::arrange(desc(.data$game_date)) %>%
    fastRhockey:::make_fastRhockey_data("NHL Schedule Information from fastRhockey data repository",Sys.time())
  data.table::fwrite(final_sched,paste0("nhl/schedules/csv/nhl_schedule_",x,".csv"))
  qs::qsave(final_sched,glue::glue('nhl/schedules/qs/nhl_schedule_{x}.qs'))
  saveRDS(final_sched, glue::glue('nhl/schedules/rds/nhl_schedule_{x}.rds'))
  arrow::write_parquet(final_sched, glue::glue('nhl/schedules/parquet/nhl_schedule_{x}.parquet'))
  cli::cli_process_done(msg_done = "Finished NHL player boxscore compilation for {x} season!")
  rm(sched)
  rm(final_sched)
  rm(season_player_box)
})

sched_list <- list.files(path = glue::glue('nhl/schedules/csv/'))
sched_g <-  purrr::map_dfr(sched_list, function(x){
  sched <- data.table::fread(paste0('nhl/schedules/csv/',x))
  return(sched)
})

sched_g <- sched_g %>%
  fastRhockey:::make_fastRhockey_data("NHL Schedule Information from fastRhockey data repository",Sys.time())
data.table::fwrite(sched_g %>% dplyr::arrange(desc(.data$game_date)), 'nhl_schedule_master.csv')
data.table::fwrite(sched_g %>% dplyr::filter(.data$PBP == TRUE) %>% dplyr::arrange(desc(.data$game_date)), 'nhl/nhl_games_in_data_repo.csv')
qs::qsave(sched_g %>% dplyr::arrange(desc(.data$game_date)), 'nhl_schedule_master.qs')
qs::qsave(sched_g %>% dplyr::filter(.data$PBP == TRUE) %>% dplyr::arrange(desc(.data$game_date)), 'nhl/nhl_games_in_data_repo.qs')
arrow::write_parquet(sched_g %>% dplyr::arrange(desc(.data$game_date)),glue::glue('nhl_schedule_master.parquet'))
arrow::write_parquet(sched_g %>% dplyr::filter(.data$PBP == TRUE) %>% dplyr::arrange(desc(.data$game_date)), 'nhl/nhl_games_in_data_repo.parquet')


rm(sched_g)
rm(sched_list)
rm(season_vector)
gc()