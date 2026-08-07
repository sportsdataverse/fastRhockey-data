# fastRhockey-data Copilot Instructions

**ARCHIVE — do not add new scrapers, builders, or datasets here.** New work
goes to the successor repos: fastRhockey-nhl-data (NHL) and
fastRhockey-pwhl-data (PWHL). This repo exists for history and for any
still-referenced release tags.

## Project Context

This repo is the legacy R-side raw cache for NHL (via the legacy NHL
Stats API path inside `fastRhockey`) and PHF play-by-play data. Per-season
schedules are scraped, then per-game JSON is fetched and persisted under
`nhl/json/{game_id}.json` and `phf/json/...`, then compiled into pbp /
boxscore / roster CSVs (plus `qs`, `rds`, `parquet` mirrors) committed to
`main`. The PHF league has ceased operations, so PHF outputs are
effectively frozen post-2023; NHL keeps refreshing on the daily cron.

Pipeline: `NHL Stats API / PHF endpoints -> fastRhockey scraper -> fastRhockey-data [HERE] -> external raw.githubusercontent.com consumers`.

The modern NHL Web API path (`api-web.nhle.com/v1/`) lives in
`fastRhockey-nhl-data` — do not retrofit it here.

## Repository Workflow

- `main` is the default branch; commit directly to `main` (no PRs for
  daily cron commits).
- The CI entry points are `daily_nhl_scraper.sh -s <START> -e <END> -r
  <true|false>` and `daily_phf_scraper.sh -s <START> -e <END> -r
  <true|false>`. External cron drives them.
- Scrapers shell out to `fastRhockey::nhl_*()` / `fastRhockey::phf_*()`.
  Fix parser bugs upstream in the `fastRhockey` package, not here.
- Do not reorganize the `nhl/` / `phf/` output tree without aligning the
  external consumers (per-format folders: `csv/`, `qs/`, `rds/`,
  `parquet/`).

## Build & Development Commands

```sh
bash daily_nhl_scraper.sh -s 2025 -e 2025 -r false
bash daily_phf_scraper.sh -s 2023 -e 2023 -r false

Rscript nhl_daily_scrape.R -s 2025 -e 2025 -r false
Rscript phf_daily_scrape.R -s 2023 -e 2023 -r false
```

`-r true` forces re-scrape of games already on disk; `-r false` skips
existing files. Outputs:

- `nhl/json/{game_id}.json` — raw NHL Stats API game feeds.
- `nhl/schedules/{csv,qs,rds,parquet}/nhl_schedule_{year}.{ext}`.
- `nhl/{pbp,team_box,player_box,rosters}/{csv,qs,rds,parquet}/...` —
  compiled per-season tables.
- `phf/json/`, `phf/{pbp,player_box,rosters,schedules}/...` — PHF
  analogue (frozen post-2023).
- `nhl_schedule_master.{csv,parquet,qs}`,
  `phf_schedule_master.{csv,parquet,qs}` — consolidated schedule
  index across all seasons.
- `nhl_games_in_data_repo.{csv,parquet,qs}`,
  `phf_games_in_data_repo.{csv,parquet,qs}` — list of game IDs covered
  by the compiled datasets.

## Code Style

- R; `library(..., lib.loc = Sys.getenv("R_LIBS"))` because the host
  pins a custom lib path for cron.
- 2-space indent, snake_case, magrittr `%>%` pipes (this is legacy R
  code — don't reflow into the native `|>` pipe).
- Filter completed games with `dplyr::filter(.data$status_status_code
  == 7)` before scraping per-game JSON.
- `furrr::future_map()` with `future::plan("multisession")` for
  per-game JSON scrape and per-game compile.
- Always run `fastRhockey:::make_fastRhockey_data(<desc>, Sys.time())`
  before persisting, to attach class + timestamp metadata.
- Keep all four formats (`csv`, `qs`, `rds`, `parquet`) in sync — the
  scrapers do this; new datasets should follow suit.

## Commit Convention

Daily cron commit messages are load-bearing for downstream year
detection:

```
NHL Play-by-Play and Schedules update (Start: 2025 End: 2025)
PHF Play-by-Play and Schedules update (Start: 2023 End: 2023)
```

Keep the `(Start: YYYY End: YYYY)` substring intact. For non-daily work
use Conventional Commits: `type(scope): description`. Common types:
`feat`, `fix`, `chore`, `ci`, `docs`, `refactor`. Use `type!:` or a
`BREAKING CHANGE:` footer for breaking changes.

**Important: Never include AI agents or assistants (e.g., Claude,
Copilot, Cursor, GPT, Gemini) as co-authors on commits.** Omit all
`Co-Authored-By` trailers referencing AI tools. This applies whether
the change was generated, refactored, or reviewed with AI assistance —
the human author is the sole attributable contributor.

## Cross-Repo References

- Upstream R package: <https://github.com/sportsdataverse/fastRhockey>
- Modern NHL data repo: <https://github.com/sportsdataverse/fastRhockey-nhl-data>
- SDV release manifest: <https://github.com/sportsdataverse/sportsdataverse-data>
