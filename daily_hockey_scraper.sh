#!/bin/bash
git pull
git add .
"C:\Program Files\R\R-4.2.0\bin\Rscript.exe" phf_daily_scrape.R
"C:\Program Files\R\R-4.2.0\bin\Rscript.exe" nhl_daily_scrape.R
git add .
git commit -m "PHF and NHL Play-by-play and Schedules update" || echo "No changes to commit"
git pull
git push