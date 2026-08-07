# CLAUDE.md — fastRhockey-data

**ARCHIVE — do not add new scrapers, builders, or datasets here.** New work
goes to the successor repos: fastRhockey-nhl-data (NHL) and
fastRhockey-pwhl-data (PWHL). This repo exists for history and for any
still-referenced release tags.

Legacy R-side raw cache of **NHL** (legacy Stats API) and **PHF** hockey data,
scraped with the [`fastRhockey`](https://github.com/sportsdataverse/fastRhockey)
package and committed back to this repo on a daily cron. Predates the modern
`*-raw`/`*-data` split: it bundles scrape + compile + committed outputs in one
place. New NHL work flows through `fastRhockey-nhl-data` (api-web.nhle.com +
`sportsdataverse-data` releases → `fastRhockey` loaders); this repo survives for
the historical PHF dataset (league defunct, frozen at 2023) and the older NHL
JSON archive.

## Commands

```sh
# Daily flow: git pull -> Rscript -> git add/commit/push (the .sh wrappers do the git)
bash daily_nhl_scraper.sh -s 2025 -e 2025 -r false   # NHL
bash daily_phf_scraper.sh -s 2023 -e 2023 -r false   # PHF (historical)
bash daily_hockey_scraper.sh -s 2025 -e 2025 -r false # both (PHF then NHL)

# Iterate on the pipeline directly (no git side effects)
Rscript nhl_daily_scrape.R -s 2025 -e 2025 -r false
Rscript phf_daily_scrape.R -s 2023 -e 2023 -r false
```

`-s`/`-e` start/end year (default `fastRhockey:::most_recent_nhl_season()`);
`-r true` re-scrapes games already on disk, `-r false` skips them. There are no
GitHub Actions workflows here — scheduling is external cron on the SDV host.

## Inputs / outputs

- **Input:** `fastRhockey::nhl_*()` / `phf_*()` (the legacy NHL Stats API +
  ESPN PHF endpoints). Per-game raw JSON lands in `nhl/json/{game_id}.json`
  and `phf/json/`.
- **Output:** per-season files written in **four formats** (`csv`, `qs`, `rds`,
  `parquet`) under `nhl/` and `phf/` subdirs `schedules/ pbp/ team_box/
  player_box/ rosters/`. PBP/box CSVs are gzipped — filenames are
  `play_by_play_{year}.csv.gz`, `team_box_{year}.csv.gz`, etc. (not prefixed
  `nhl_`); schedules are `nhl_schedule_{year}.{ext}`.
- **Indexes:** `{nhl,phf}/*_games_in_data_repo.{csv,parquet,qs}` and
  `{nhl,phf}_schedule_master.{csv,parquet,qs}` are rebuilt each run and
  committed alongside — downstream consumers read these to know coverage.
- Consumed by pulling `raw.githubusercontent.com/sportsdataverse/fastRhockey-data/main/...`.

## Conventions & gotchas

- Thin caller + on-disk cache: **NHL/PHF parsing bugs belong upstream in
  `fastRhockey`**, not here. Keep all four output formats in sync.
- Scripts load packages with `lib.loc = Sys.getenv("R_LIBS")` (cron-tuned host
  path) — locally set `R_LIBS` or strip `lib.loc=` when iterating.
- `dplyr::filter(.data$status_status_code == 7)` selects completed games before
  per-game scrape; `7` is the upstream Stats-API "final" code — don't change
  it blind. Every dataset gets `fastRhockey:::make_fastRhockey_data()` before save.
- This is a `-data` repo: committing CSV/parquet/qs/rds is intentional — do not
  warn about repo size.
- The daily umbrella commit subject is load-bearing — downstream year-detection
  parses the `(Start: YYYY End: YYYY)` substring, so keep the exact format:
  `NHL Play-by-Play and Schedules update (Start: 2025 End: 2025)`. For manual
  work use Conventional Commits (`fix(nhl): ...`, `chore: ...`, scopes nhl/phf).
- The `.sh` wrappers `git pull` before each step — avoid manual commits during
  the cron window to prevent collisions.
- **Never** add AI co-author/author trailers to commits.
