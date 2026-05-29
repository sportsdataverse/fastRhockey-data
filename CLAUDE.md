# CLAUDE.md — fastRhockey-data Development Guide

## Repo Overview

`fastRhockey-data` is the legacy R-side raw cache for ESPN/NHL and PHF
hockey play-by-play data, compiled with the
[`fastRhockey`](https://github.com/sportsdataverse/fastRhockey) package.
It pulls per-season schedules then per-game JSON via `fastRhockey::nhl_*()`
and `fastRhockey::phf_*()` helpers, persists them under `nhl/` and `phf/`,
and commits the results back to this repo on a daily cron cadence.

This repo predates the current `*-raw` / `*-data` split — it bundles the
scrape, the compilation step, and the committed CSV/parquet/qs/rds outputs
in one place. New NHL work flows through the modern
[`fastRhockey-nhl-data`](https://github.com/sportsdataverse/fastRhockey-nhl-data)
repo (which uses the api-web.nhle.com endpoints + sportsdataverse-data
releases). This repo is kept alive for the historical PHF dataset (the
league is now defunct) and the older NHL JSON archive.

## Pipeline Position

```
NHL Stats API (legacy) --[R + fastRhockey scrape]--> fastRhockey-data [HERE]
ESPN PHF endpoints     --[R + fastRhockey scrape]--> fastRhockey-data [HERE]

         |
         v
   committed CSV / parquet / qs / rds files under nhl/ and phf/
         |
         v
   external consumers pulling raw.githubusercontent.com/sportsdataverse/fastRhockey-data/main/...
```

Modern NHL work flows separately through
`fastRhockey-nhl-data` → `sportsdataverse-data` releases → `fastRhockey`
loaders. PHF datasets are frozen here at season-end 2023.

## Build & Development Commands

Two shell entry points drive the daily flow. Both pull `main`, run their
respective Rscript, then commit + push:

```sh
# NHL daily flow (CI entry point) — defaults to most_recent_nhl_season()
bash daily_nhl_scraper.sh -s 2025 -e 2025 -r false

# PHF daily flow — historical, kept for back-fill / forensics
bash daily_phf_scraper.sh -s 2023 -e 2023 -r false

# Or call the Rscripts directly when iterating
Rscript nhl_daily_scrape.R -s 2025 -e 2025 -r false
Rscript phf_daily_scrape.R -s 2023 -e 2023 -r false
```

`-r true` forces re-scrape of games already on disk; `-r false` skips
existing files. The wrapper shell scripts also do `git pull / add / commit
/ push` around the Rscript call, so don't double-commit when running
locally.

## Repo Layout

```
nhl_daily_scrape.R              # NHL full pipeline: schedules -> json -> pbp -> boxscores
phf_daily_scrape.R              # PHF full pipeline: schedules -> json -> pbp -> boxscores
phf_pbp_repo.R                  # PHF pbp aggregator (compiles per-season CSVs)
phf_boxscore_repo.R             # PHF boxscore aggregator
daily_nhl_scraper.sh            # NHL CI entry point (cron caller)
daily_phf_scraper.sh            # PHF CI entry point
daily_nhl.out                   # last NHL cron run summary
daily_phf.out                   # last PHF cron run summary

nhl/                            # NHL committed scraped output
  json/{game_id}.json           # raw NHL Stats API game feeds
  schedules/{csv,qs,rds,parquet}/nhl_schedule_{year}.{ext}
  pbp/{csv,qs,rds,parquet}/nhl_pbp_{year}.{ext}
  team_box/{csv,qs,rds,parquet}/nhl_team_box_{year}.{ext}
  player_box/{csv,qs,rds,parquet}/nhl_player_box_{year}.{ext}
  rosters/{csv,qs,rds,parquet}/nhl_rosters_{year}.{ext}
  nhl_games_in_data_repo.{csv,parquet,qs}

phf/                            # PHF committed scraped output (frozen post-2023)
  json/                         # raw PHF JSON
  schedules/                    # per-season schedules
  pbp/                          # play-by-play
  player_box/                   # per-game player box
  rosters/                      # per-season rosters
  phf_play_by_play.csv          # consolidated PBP
  phf_boxscore.csv              # consolidated boxscore
  phf_meta_data.csv             # consolidated game meta
  phf_games_in_data_repo.{csv,parquet,qs}

nhl_schedule_master.{csv,parquet,qs}    # consolidated NHL schedule
phf_schedule_master.{csv,parquet,qs}    # consolidated PHF schedule
nhl_teams_colors_logos.csv              # team styling reference
themes/                                 # branding/graphics
```

## Coding Conventions

- All R scripts source `fastRhockey` for the actual API parsing. **Bug
  fixes to NHL or PHF parsing belong upstream in
  [`fastRhockey`](https://github.com/sportsdataverse/fastRhockey)**, not
  here. This repo is a thin caller + on-disk cache.
- Library imports use `lib.loc=Sys.getenv("R_LIBS")` (the cron-tuned
  R_LIBS path on the scraper host). Don't change to default lib paths
  without checking the cron setup.
- Each scraper persists in four formats per season: `csv`, `qs`, `rds`,
  `parquet`. Keep all four in sync — downstream consumers pick whichever
  format suits them.
- `fastRhockey:::make_fastRhockey_data(<desc>, Sys.time())` is called on
  every dataset before save to attach the `fastRhockey_data` class +
  timestamp.
- `fastRhockey:::most_recent_nhl_season()` is the canonical default for
  `--start_year` / `--end_year` (both for nhl_daily_scrape.R; PHF defaults
  to the last live season).
- `furrr::future_map()` with `future::plan("multisession")` is used for
  per-game JSON scraping and per-game compile steps. The plan is reset
  inside each map block.
- `dplyr::filter(.data$status_status_code == 7)` selects only completed
  games before per-game scrape. Don't change this filter without checking
  what `7` means in the upstream Stats API schema.

## Daily Cadence

Both shell scripts assume external cron (the SDV scraper host). On each
run:

1. `git pull` to sync with `main`.
2. `Rscript <pipeline>.R -s $START -e $END -r $RESCRAPE` to fan out
   schedule + per-game JSON scrape, then compile pbp / box.
3. `git add . && git commit -m "<sport> Play-by-Play and Schedules update
   (Start: $START End: $END)"` — note the commit message format is
   load-bearing for downstream year-detection scripts.
4. `git push`.

## Commit Convention

Daily umbrella commits follow the load-bearing format:

```
NHL Play-by-Play and Schedules update (Start: 2025 End: 2025)
PHF Play-by-Play and Schedules update (Start: 2023 End: 2023)
```

The `(Start: YYYY End: YYYY)` substring is parsed by downstream tooling —
do **not** alter it.

For non-daily manual work (refactors, README edits, helper changes), use
[Conventional Commits](https://www.conventionalcommits.org/):

```
feat(nhl): add status_status_code filter to skip postponed games
fix(phf): correct JSONP callback regex in phf_daily_scrape
chore: refresh nhl_teams_colors_logos.csv
ci: update daily_nhl_scraper.sh to use most_recent_nhl_season default
docs: clarify -r rescrape semantics in README
```

Use `type!:` or `BREAKING CHANGE:` footer for breaking changes. Split
unrelated work into separate commits.

**Important: Never include AI agents or assistants (e.g., Claude, Copilot,
Cursor, GPT, Gemini) as co-authors on commits.** Omit all `Co-Authored-By`
trailers referencing AI tools. The human author is the sole attributable
contributor regardless of whether the change was generated, refactored,
or reviewed with AI assistance.

## Cross-Repo References

- Upstream R package: <https://github.com/sportsdataverse/fastRhockey>
- Modern NHL data repo (newer api-web.nhle.com path):
  <https://github.com/sportsdataverse/fastRhockey-nhl-data>
- SDV release manifest the modern data repo pushes to:
  <https://github.com/sportsdataverse/sportsdataverse-data>

## Project-Specific Gotchas

- This repo's NHL section uses the **legacy NHL Stats API** path inside
  `fastRhockey::nhl_schedule()` / `fastRhockey::nhl_game_feed()`. The
  modern `api-web.nhle.com/v1/` Web API path is handled by the newer
  `fastRhockey-nhl-data` repo; do not retrofit it here.
- PHF endpoints are stable but the league ceased operations in 2023.
  PHF outputs are effectively frozen — don't expect new games beyond
  the existing schedule files.
- `nhl_schedule_master.{csv,parquet,qs}` and the per-sport
  `*_games_in_data_repo.*` indexes are rebuilt by the daily scripts and
  committed alongside the dataset updates. They're the index downstream
  consumers use to know which game IDs are covered.
- The shell scripts `git pull` before every step; if you push a manual
  edit while the cron is running you may collide. Avoid commits during
  the cron window.
- Library install path is `Sys.getenv("R_LIBS")` — locally you'll likely
  need to either set that env var or strip `lib.loc=` when iterating.
