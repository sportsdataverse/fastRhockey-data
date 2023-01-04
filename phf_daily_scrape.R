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
  make_option(c("-s", "--start_year"), action="store", default=fastRhockey:::most_recent_phf_season(), type='integer', help="Start year of the seasons to process"),
  make_option(c("-e", "--end_year"), action="store", default=fastRhockey:::most_recent_phf_season(), type='integer', help="End year of the seasons to process")
)
opt = parse_args(OptionParser(option_list=option_list))
options(stringsAsFactors = FALSE)
options(scipen = 999)
season_vector <- 2021:2021
rebuild <- TRUE
version = packageVersion("fastRhockey")


# Play-by-Play Data Pull --------------------------------------------------
sched <- purrr::map_dfr(season_vector, function(x){
                        sched <- fastRhockey::phf_schedule(season=x) %>%
                          tidyr::unnest("home_team_logo_url",names_sep = "_") %>%
                          tidyr::unnest("away_team_logo_url",names_sep = "_") %>%
                          dplyr::mutate(season = x)
                        return(sched)
                        })
### 1a) scrape season schedule
### 1b) save to disk
season_schedules <- purrr::map_dfr(season_vector, function(x){

  sched <- fastRhockey::phf_schedule(season=x)  %>%
    tidyr::unnest("home_team_logo_url",names_sep = "_") %>%
    tidyr::unnest("away_team_logo_url",names_sep = "_") %>%
    dplyr::mutate(season = x) %>%
    dplyr::tibble()
  ifelse(!dir.exists(file.path("phf/schedules")), dir.create(file.path("phf/schedules")), FALSE)
  ifelse(!dir.exists(file.path("phf/schedules/csv")), dir.create(file.path("phf/schedules/csv")), FALSE)
  ifelse(!dir.exists(file.path("phf/schedules/qs")), dir.create(file.path("phf/schedules/qs")), FALSE)
  ifelse(!dir.exists(file.path("phf/schedules/rds")), dir.create(file.path("phf/schedules/rds")), FALSE)
  ifelse(!dir.exists(file.path("phf/schedules/parquet")), dir.create(file.path("phf/schedules/parquet")), FALSE)
  sched <- sched %>%
    fastRhockey:::make_fastRhockey_data("PHF Schedule Information from fastRhockey data repository",Sys.time())
  data.table::fwrite(sched,paste0("phf/schedules/csv/phf_schedule_",x,".csv"))
  qs::qsave(sched,glue::glue('phf/schedules/qs/phf_schedule_{x}.qs'))
  saveRDS(sched, glue::glue('phf/schedules/rds/phf_schedule_{x}.rds'))
  arrow::write_parquet(sched, glue::glue('phf/schedules/parquet/phf_schedule_{x}.parquet'))

  return(sched)
})

pbp_list <- as.integer(gsub(".json","",list.files(path = glue::glue('phf/json/'))))

season_schedules <- season_schedules %>%
  dplyr::filter(.data$status == "Final",
                .data$has_play_by_play == TRUE,
                !(.data$game_id %in% c(301699, 368721)))

if(rebuild == FALSE){
  pbp_list <- as.integer(gsub(".json","",list.files(path = glue::glue('phf/json/'))))
  season_schedules <- season_schedules %>%
    dplyr::filter(!(.data$game_id %in% pbp_list),
                  .data$has_play_by_play == TRUE,
                  !(.data$game_id %in% c(301699, 368721)))
}
### 2a) scrape game json
### 2b) save json to disk
cli::cli_process_start("Starting scrape of {length(season_schedules$game_id)} games...")


scrape_games <- purrr::map(season_schedules$game_id, function(x){
  game <- fastRhockey::phf_game_all(game_id = x)
  jsonlite::write_json(game, path = glue::glue("phf/json/{x}.json"))
})
cli::cli_process_done(msg_done = "Finished scrape of {length(season_schedules$game_id)} games!")




###---- 3a) Build play-by-play dataset ----
season_pbp_compile <- purrr::map(season_vector,function(x){
  cli::cli_process_start("Starting PHF play-by-play compilation for {x} season...")
  sched <- data.table::fread(paste0("phf/schedules/csv/phf_schedule_",x,".csv"))
  sched_pull <- sched %>%
    dplyr::filter(.data$has_play_by_play == TRUE,
                  .data$status == "Final",
                  !(.data$game_id %in% c(301699, 368721)))

  season_pbp <- purrr::map_dfr(sched_pull$game_id,function(y){
    pbp <- jsonlite::fromJSON(paste0("phf/json/", y, ".json"))$plays
    if (length(pbp) > 1) {
      pbp$game_id <- y
      return(pbp)
    }
  })
  ifelse(!dir.exists(file.path("phf/pbp")), dir.create(file.path("phf/pbp")), FALSE)
  ifelse(!dir.exists(file.path("phf/pbp/csv")), dir.create(file.path("phf/pbp/csv")), FALSE)
  if(nrow(season_pbp)>1){
    season_pbp$season <- x
    season_pbp <- season_pbp %>%
      fastRhockey:::make_fastRhockey_data("PHF Play-by-Play Information from fastRhockey data repository",Sys.time())
    data.table::fwrite(season_pbp, file=paste0("phf/pbp/csv/play_by_play_",x,".csv"))

    ifelse(!dir.exists(file.path("phf/pbp/qs")), dir.create(file.path("phf/pbp/qs")), FALSE)
    qs::qsave(season_pbp,glue::glue("phf/pbp/qs/play_by_play_{x}.qs"))

    ifelse(!dir.exists(file.path("phf/pbp/rds")), dir.create(file.path("phf/pbp/rds")), FALSE)
    saveRDS(season_pbp,glue::glue("phf/pbp/rds/play_by_play_{x}.rds"))

    ifelse(!dir.exists(file.path("phf/pbp/parquet")), dir.create(file.path("phf/pbp/parquet")), FALSE)
    arrow::write_parquet(season_pbp, glue::glue("phf/pbp/parquet/play_by_play_{x}.parquet"))
  }
  if(nrow(season_pbp)>0){
    sched <- sched %>%
      dplyr::mutate(
        PBP = ifelse(.data$game_id %in% unique(season_pbp$game_id), TRUE,FALSE))
  } else {
    sched$PBP <- FALSE
  }

  final_sched <- dplyr::distinct(sched) %>% dplyr::arrange(desc(.data$datetime)) %>%
    fastRhockey:::make_fastRhockey_data("PHF Schedule Information from fastRhockey data repository",Sys.time())
  data.table::fwrite(final_sched,paste0("phf/schedules/csv/phf_schedule_",x,".csv"))
  qs::qsave(final_sched,glue::glue('phf/schedules/qs/phf_schedule_{x}.qs'))
  saveRDS(final_sched, glue::glue('phf/schedules/rds/phf_schedule_{x}.rds'))
  arrow::write_parquet(final_sched, glue::glue('phf/schedules/parquet/phf_schedule_{x}.parquet'))
  cli::cli_process_done(msg_done = "Finished PHF play-by-play compilation for {x} season!")
  rm(sched)
  rm(final_sched)
  rm(season_pbp)
  rm(sched_pull)
})

sched <- purrr::map_dfr(season_vector, function(x){
  sched <- fastRhockey::phf_schedule(season=x) %>%
    tidyr::unnest("home_team_logo_url",names_sep = "_") %>%
    tidyr::unnest("away_team_logo_url",names_sep = "_") %>%
    dplyr::mutate(season = x)
  return(sched)
})



### 3b) Build team boxscore dataset
season_team_box_compile <- purrr::map(season_vector,function(x){
  cli::cli_process_start("Starting PHF team boxscore compilation for {x} season...")
  sched <- data.table::fread(paste0("phf/schedules/csv/phf_schedule_",x,".csv"))
  sched <- sched %>%
    dplyr::filter(.data$status == "Final",
                  !(.data$game_id %in% c(301699,368721)))

  season_team_box <- purrr::map_dfr(sched$game_id,function(y){
    team_box <- fastRhockey::phf_team_box(game_id = y)

    if(!("overtime_shots" %in% colnames(team_box))){
      team_box$overtime_shots <- NA_integer_
    }
    suppressWarnings(
      team_box <- team_box %>%
        dplyr::mutate_at(c("period_1_shots","period_2_shots","period_3_shots", "overtime_shots"),
                         function(x){
                           as.integer(x)
                         })
    )
    return(team_box)
  })
  if(nrow(season_team_box)>1){
    season_team_box$season <- x
    season_team_box <- season_team_box %>%
      fastRhockey:::make_fastRhockey_data("PHF Team Boxscore Information from fastRhockey data repository",Sys.time())
    ifelse(!dir.exists(file.path("phf/team_box")), dir.create(file.path("phf/team_box")), FALSE)
    ifelse(!dir.exists(file.path("phf/team_box/csv")), dir.create(file.path("phf/team_box/csv")), FALSE)
    data.table::fwrite(season_team_box, file=paste0("phf/team_box/csv/team_box_",x,".csv"))

    ifelse(!dir.exists(file.path("phf/team_box/qs")), dir.create(file.path("phf/team_box/qs")), FALSE)
    qs::qsave(season_team_box,glue::glue("phf/team_box/qs/team_box_{x}.qs"))

    ifelse(!dir.exists(file.path("phf/team_box/rds")), dir.create(file.path("phf/team_box/rds")), FALSE)
    saveRDS(season_team_box,glue::glue("phf/team_box/rds/team_box_{x}.rds"))

    ifelse(!dir.exists(file.path("phf/team_box/parquet")), dir.create(file.path("phf/team_box/parquet")), FALSE)
    arrow::write_parquet(season_team_box, glue::glue("phf/team_box/parquet/team_box_{x}.parquet"))
  }
  if(nrow(season_team_box)>0){
    sched <- sched %>%
      dplyr::mutate(
        team_box = ifelse(.data$game_id %in% unique(season_team_box$game_id), TRUE,FALSE))
  } else {
    sched$team_box <- FALSE
  }

  final_sched <- dplyr::distinct(sched) %>% dplyr::arrange(desc(.data$datetime)) %>%
    fastRhockey:::make_fastRhockey_data("PHF Schedule Information from fastRhockey data repository",Sys.time())
  data.table::fwrite(final_sched,paste0("phf/schedules/csv/phf_schedule_",x,".csv"))
  qs::qsave(final_sched,glue::glue('phf/schedules/qs/phf_schedule_{x}.qs'))
  saveRDS(final_sched, glue::glue('phf/schedules/rds/phf_schedule_{x}.rds'))
  arrow::write_parquet(final_sched, glue::glue('phf/schedules/parquet/phf_schedule_{x}.parquet'))
  cli::cli_process_done(msg_done = "Finished PHF team boxscore compilation for {x} season!")
  rm(sched)
  rm(final_sched)
  rm(season_team_box)
})


### 3c) Build player boxscore dataset
season_player_box_compile <- purrr::map(season_vector,function(x){
  cli::cli_process_start("Starting PHF player boxscore compilation for {x} season...")
  sched <- data.table::fread(paste0("phf/schedules/csv/phf_schedule_",x,".csv"))
  sched <- sched %>%
    dplyr::filter(.data$status == "Final",
                  !(.data$game_id %in% c(301699,368721)))

  season_player_box <- purrr::map_dfr(sched$game_id,function(y){
    player_box <- fastRhockey::phf_player_box(game_id = y)
    skaters <- player_box$skaters %>%
      dplyr::mutate_at(c("position","faceoffs_won_lost"),
                       function(x){as.character(x)}) %>%
      dplyr::mutate(minutes_played = NA_character_)
    goalies <- player_box$goalies
    goalies <- goalies %>%
      dplyr::mutate(position = "G") %>%
      dplyr::mutate_at(c("minutes_played"),
                       function(x){as.character(x)})
    player_box_combined <- dplyr::bind_rows(skaters,goalies)
    return(player_box_combined)
  })
  if(nrow(season_player_box)>1){
    season_player_box$season <- x
    season_player_box <- season_player_box %>%
      fastRhockey:::make_fastRhockey_data("PHF Player Boxscore Information from fastRhockey data repository",Sys.time())
    ifelse(!dir.exists(file.path("phf/player_box")), dir.create(file.path("phf/player_box")), FALSE)
    ifelse(!dir.exists(file.path("phf/player_box/csv")), dir.create(file.path("phf/player_box/csv")), FALSE)
    data.table::fwrite(season_player_box, file=paste0("phf/player_box/csv/player_box_",x,".csv"))

    ifelse(!dir.exists(file.path("phf/player_box/qs")), dir.create(file.path("phf/player_box/qs")), FALSE)
    qs::qsave(season_player_box,glue::glue("phf/player_box/qs/player_box_{x}.qs"))

    ifelse(!dir.exists(file.path("phf/player_box/rds")), dir.create(file.path("phf/player_box/rds")), FALSE)
    saveRDS(season_player_box,glue::glue("phf/player_box/rds/player_box_{x}.rds"))

    ifelse(!dir.exists(file.path("phf/player_box/parquet")), dir.create(file.path("phf/player_box/parquet")), FALSE)
    arrow::write_parquet(season_player_box, glue::glue("phf/player_box/parquet/player_box_{x}.parquet"))
  }
  if(nrow(season_player_box)>0){
    sched <- sched %>%
      dplyr::mutate(
        player_box = ifelse(.data$game_id %in% unique(season_player_box$game_id), TRUE,FALSE))
  } else {
    sched$player_box <- FALSE
  }

  final_sched <- dplyr::distinct(sched) %>% dplyr::arrange(desc(.data$datetime)) %>%
    fastRhockey:::make_fastRhockey_data("PHF Schedule Information from fastRhockey data repository",Sys.time())
  data.table::fwrite(final_sched,paste0("phf/schedules/csv/phf_schedule_",x,".csv"))
  qs::qsave(final_sched,glue::glue('phf/schedules/qs/phf_schedule_{x}.qs'))
  saveRDS(final_sched, glue::glue('phf/schedules/rds/phf_schedule_{x}.rds'))
  arrow::write_parquet(final_sched, glue::glue('phf/schedules/parquet/phf_schedule_{x}.parquet'))
  cli::cli_process_done(msg_done = "Finished PHF player boxscore compilation for {x} season!")
  rm(sched)
  rm(final_sched)
  rm(season_player_box)
})


### 3d) Build team roster dataset
season_team_roster_compile <- purrr::map(season_vector,function(x){
  cli::cli_process_start("Starting PHF team roster compilation for {x} season...")
  if (x >= 2021){
    teams <- fastRhockey::phf_league_info(season = x)$teams

    future::plan("multisession")
    season_team_roster <- furrr::future_map_dfr(teams$name, function(y){
      fastRhockey::phf_team_roster(team = y, season = x)$roster
    })
    if(nrow(season_team_roster)>1){
      season_team_roster$season <- x
      season_team_roster <- season_team_roster %>%
        fastRhockey:::make_fastRhockey_data("PHF Team Roster Information from fastRhockey data repository",Sys.time())
      ifelse(!dir.exists(file.path("phf/rosters")), dir.create(file.path("phf/rosters")), FALSE)
      ifelse(!dir.exists(file.path("phf/rosters/csv")), dir.create(file.path("phf/rosters/csv")), FALSE)
      data.table::fwrite(season_team_roster, file=paste0("phf/rosters/csv/rosters_",x,".csv.gz"))

      ifelse(!dir.exists(file.path("phf/rosters/qs")), dir.create(file.path("phf/rosters/qs")), FALSE)
      qs::qsave(season_team_roster,glue::glue("phf/rosters/qs/rosters_{x}.qs"))

      ifelse(!dir.exists(file.path("phf/rosters/rds")), dir.create(file.path("phf/rosters/rds")), FALSE)
      saveRDS(season_team_roster,glue::glue("phf/rosters/rds/rosters_{x}.rds"))

      ifelse(!dir.exists(file.path("phf/rosters/parquet")), dir.create(file.path("phf/rosters/parquet")), FALSE)
      arrow::write_parquet(season_team_roster, glue::glue("phf/rosters/parquet/rosters_{x}.parquet"))
    }
    rm(season_team_roster)
  }
  cli::cli_process_done(msg_done = "Finished PHF team roster compilation for {x} season!")
})


sched_list <- list.files(path = glue::glue('phf/schedules/csv/'))
sched_g <-  purrr::map_dfr(sched_list, function(x){
  sched <- data.table::fread(paste0('phf/schedules/csv/',x))
  return(sched)
})
sched_g <- sched_g %>%
  fastRhockey:::make_fastRhockey_data("PHF Schedule Information from fastRhockey data repository",Sys.time())
data.table::fwrite(sched_g %>% dplyr::arrange(desc(.data$datetime)), 'phf_schedule_master.csv')
data.table::fwrite(sched_g %>% dplyr::filter(.data$PBP == TRUE) %>% dplyr::arrange(desc(.data$datetime)), 'phf/phf_games_in_data_repo.csv')
qs::qsave(sched_g %>% dplyr::arrange(desc(.data$datetime)), 'phf_schedule_master.qs')
qs::qsave(sched_g %>% dplyr::filter(.data$PBP == TRUE) %>% dplyr::arrange(desc(.data$datetime)), 'phf/phf_games_in_data_repo.qs')
arrow::write_parquet(sched_g %>% dplyr::arrange(desc(.data$datetime)),glue::glue('phf_schedule_master.parquet'))
arrow::write_parquet(sched_g %>% dplyr::filter(.data$PBP == TRUE) %>% dplyr::arrange(desc(.data$datetime)), 'phf/phf_games_in_data_repo.parquet')


rm(sched_g)
rm(sched_list)
rm(season_vector)
gc()
