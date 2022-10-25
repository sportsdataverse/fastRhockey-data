#!/bin/bash
git pull
git add .
R phf_daily_scrape.R
R nhl_daily_scrape.R
git add .
git commit -m "PHF and NHL Play-by-play and Schedules update" || echo "No changes to commit"
git pull
git push