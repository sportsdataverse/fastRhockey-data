#!/bin/bash
git pull
git add .
Rscript phf_daily_scrape.R
Rscript nhl_daily_scrape.R
git add .
git commit -m "PHF and NHL Play-by-play and Schedules update" || echo "No changes to commit"
git pull
git push