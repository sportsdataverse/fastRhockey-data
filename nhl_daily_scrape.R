rm(list = ls())
gc()
lib_path <- Sys.getenv("R_LIBS")
if (!requireNamespace('pacman', quietly = TRUE)){
  install.packages('pacman',lib=Sys.getenv("R_LIBS"), repos='http://cran.us.r-project.org')
}
suppressPackageStartupMessages(suppressMessages(library(dplyr, lib.loc=lib_path)))
suppressPackageStartupMessages(suppressMessages(library(magrittr, lib.loc=lib_path)))
suppressPackageStartupMessages(suppressMessages(library(jsonlite, lib.loc=lib_path)))
suppressPackageStartupMessages(suppressMessages(library(purrr, lib.loc=lib_path)))
suppressPackageStartupMessages(suppressMessages(library(progressr, lib.loc=lib_path)))
suppressPackageStartupMessages(suppressMessages(library(data.table, lib.loc=lib_path)))
suppressPackageStartupMessages(suppressMessages(library(qs, lib.loc=lib_path)))
suppressPackageStartupMessages(suppressMessages(library(arrow, lib.loc=lib_path)))
suppressPackageStartupMessages(suppressMessages(library(glue, lib.loc=lib_path)))
suppressPackageStartupMessages(suppressMessages(library(optparse, lib.loc=lib_path)))
option_list = list(
  make_option(c("-s", "--start_year"), action="store", default=fastRhockey:::most_recent_nhl_season(), type='integer', help="Start year of the seasons to process"),
  make_option(c("-e", "--end_year"), action="store", default=fastRhockey:::most_recent_nhl_season(), type='integer', help="End year of the seasons to process")
  make_option(c("-r", "--rescrape"), action="store", default=FALSE, type='logical', help="Rescrape the raw JSON files from web api")

)
opt = parse_args(OptionParser(option_list=option_list))
options(stringsAsFactors = FALSE)
options(scipen = 999)

season_vector <- opt$s:opt$e
rebuild <- opt$r
rebuild_from_existing_json <- FALSE
scrape_schedules <- TRUE
version <- packageVersion("fastRhockey")

# Play-by-Play Data Pull --------------------------------------------------
### 1a) scrape season schedule
### 1b) save to disk
if(scrape_schedules == TRUE){
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
}
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
    pbp <- jsonlite::fromJSON(paste0("nhl/json/", y, ".json"))$all_plays
    if (length(pbp) > 1) {
      pbp$game_id <- y
      return(pbp)
    }
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
    team_box <- jsonlite::fromJSON(paste0("nhl/json/", y, ".json"))$team_box
    if (length(team_box) > 1) {
      team_box$game_id <- y
      return(team_box)
    }
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
    player_box <- jsonlite::fromJSON(paste0("nhl/json/", y, ".json"))$player_box
    if (length(player_box) > 1) {
      player_box$game_id <- y
      return(player_box)
    }
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

### 3d) Build team roster dataset
season_team_roster_compile <- purrr::map(season_vector,function(x){
  cli::cli_process_start("Starting NHL Team Roster compilation for {x} season...")
  teams <- fastRhockey::nhl_teams(season = glue::glue("{x-1}{x}")) %>%
    dplyr::mutate(first_year_of_play = as.integer(.data$first_year_of_play)) %>%
    dplyr::filter(.data$first_year_of_play <= x)

  season_team_roster <- purrr::map_dfr(teams$team_id, function(y){
    try(fastRhockey::nhl_teams_roster(team_id = y, season = glue::glue("{x-1}{x}")))
  })
  if(nrow(season_team_roster)>1){
    season_team_roster$season <- x
    season_team_roster <- season_team_roster %>%
      fastRhockey:::make_fastRhockey_data("NHL Team Roster Information from fastRhockey data repository",Sys.time())
    ifelse(!dir.exists(file.path("nhl/rosters")), dir.create(file.path("nhl/rosters")), FALSE)
    ifelse(!dir.exists(file.path("nhl/rosters/csv")), dir.create(file.path("nhl/rosters/csv")), FALSE)
    data.table::fwrite(season_team_roster, file=paste0("nhl/rosters/csv/rosters_",x,".csv.gz"))

    ifelse(!dir.exists(file.path("nhl/rosters/qs")), dir.create(file.path("nhl/rosters/qs")), FALSE)
    qs::qsave(season_team_roster,glue::glue("nhl/rosters/qs/rosters_{x}.qs"))

    ifelse(!dir.exists(file.path("nhl/rosters/rds")), dir.create(file.path("nhl/rosters/rds")), FALSE)
    saveRDS(season_team_roster,glue::glue("nhl/rosters/rds/rosters_{x}.rds"))

    ifelse(!dir.exists(file.path("nhl/rosters/parquet")), dir.create(file.path("nhl/rosters/parquet")), FALSE)
    arrow::write_parquet(season_team_roster, glue::glue("nhl/rosters/parquet/rosters_{x}.parquet"))
  }
  cli::cli_process_done(msg_done = "Finished NHL Team Roster compilation for {x} season!")

  rm(season_team_roster)
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