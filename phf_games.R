if (!requireNamespace('pacman', quietly = TRUE)){
  install.packages('pacman')
}
pacman::p_load_current_gh("BenHowell71/fastRhockey")

library(fastRhockey)
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


options(stringsAsFactors = FALSE)
options(scipen = 999)
years_vec <- fastRhockey::most_recent_phf_season()
# --- compile into play_by_play_{year}.parquet ---------
phf_pbp_games <- function(y){
  cli::cli_process_start("Starting phf play_by_play parse for {y}!")
  pbp_g <- data.frame()
  pbp_list <- list.files(path = glue::glue('phf/{y}/'))
  pbp_g <- purrr::map_dfr(pbp_list, function(x){
    pbp <- jsonlite::fromJSON(glue::glue('phf/{y}/{x}'))$plays
    if(length(pbp)>1){
      pbp$game_id <- gsub(".json","", x)
    }
    return(pbp)
  })
  if(nrow(pbp_g)>0 && length(pbp_g)>1){
    pbp_g <- pbp_g %>% janitor::clean_names()
    pbp_g <- pbp_g %>% 
      dplyr::mutate(
        game_id = as.integer(.data$game_id)
      )
  }
  ifelse(!dir.exists(file.path("phf/pbp")), dir.create(file.path("phf/pbp")), FALSE)
  ifelse(!dir.exists(file.path("phf/pbp/csv")), dir.create(file.path("phf/pbp/csv")), FALSE)
  if(nrow(pbp_g)>1){
    data.table::fwrite(pbp_g, file=paste0("phf/pbp/csv/play_by_play_",y,".csv.gz"))
    
    ifelse(!dir.exists(file.path("phf/pbp/qs")), dir.create(file.path("phf/pbp/qs")), FALSE)
    qs::qsave(pbp_g,glue::glue("phf/pbp/qs/play_by_play_{y}.qs"))
    
    ifelse(!dir.exists(file.path("phf/pbp/rds")), dir.create(file.path("phf/pbp/rds")), FALSE)
    saveRDS(pbp_g,glue::glue("phf/pbp/rds/play_by_play_{y}.rds"))
    
    ifelse(!dir.exists(file.path("phf/pbp/parquet")), dir.create(file.path("phf/pbp/parquet")), FALSE)
    arrow::write_parquet(pbp_g, glue::glue("phf/pbp/parquet/play_by_play_{y}.parquet"))
  }
  sched <- data.table::fread(paste0('phf/schedules/csv/phf_schedule_',y,'.csv'))
  sched <- sched %>%
    dplyr::mutate(
      game_id = as.integer(.data$id),
      status.displayClock = as.character(.data$status.displayClock))
  if(nrow(pbp_g)>0){
    sched <- sched %>%
      dplyr::mutate(
        PBP = ifelse(.data$game_id %in% unique(pbp_g$game_id), TRUE,FALSE))
  } else {
    sched$PBP <- FALSE
  }
  
  final_sched <- dplyr::distinct(sched) %>% dplyr::arrange(desc(.data$date))
  data.table::fwrite(final_sched,paste0("phf/schedules/csv/phf_schedule_",y,".csv"))
  qs::qsave(final_sched,glue::glue('phf/schedules/qs/phf_schedule_{y}.qs'))
  saveRDS(final_sched, glue::glue('phf/schedules/rds/phf_schedule_{y}.rds'))
  arrow::write_parquet(final_sched, glue::glue('phf/schedules/parquet/phf_schedule_{y}.parquet'))
  rm(sched)
  rm(final_sched)
  rm(pbp_g)
  gc()
  cli::cli_process_done(msg_done = "Finished phf play_by_play parse for {y}!")
  return(NULL)
}

all_games <- purrr::map(years_vec, function(y){
  phf_pbp_games(y)
})

sched_list <- list.files(path = glue::glue('phf/schedules/csv/'))
sched_g <-  purrr::map_dfr(sched_list, function(x){
  sched <- data.table::fread(paste0('phf/schedules/csv/',x)) %>%
    dplyr::mutate(
      status.displayClock = as.character(.data$status.displayClock)
    )
  return(sched)
})

data.table::fwrite(sched_g %>% dplyr::arrange(desc(.data$date)), 'phf_schedule_master.csv')
data.table::fwrite(sched_g %>% dplyr::filter(.data$PBP == TRUE) %>% dplyr::arrange(desc(.data$date)), 'phf/phf_games_in_data_repo.csv')
qs::qsave(sched_g %>% dplyr::arrange(desc(.data$date)), 'phf_schedule_master.qs')
qs::qsave(sched_g %>% dplyr::filter(.data$PBP == TRUE) %>% dplyr::arrange(desc(.data$date)), 'phf/phf_games_in_data_repo.qs')
arrow::write_parquet(sched_g %>% dplyr::arrange(desc(.data$date)),glue::glue('phf_schedule_master.parquet'))
arrow::write_parquet(sched_g %>% dplyr::filter(.data$PBP == TRUE) %>% dplyr::arrange(desc(.data$date)), 'phf/phf_games_in_data_repo.parquet')


rm(sched_g)
rm(sched_list)
rm(years_vec)
gc()
