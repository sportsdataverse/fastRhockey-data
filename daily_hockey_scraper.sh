#!/bin/bash
while getopts s:e:r: flag
do
    case "${flag}" in
        s) START_YEAR=${OPTARG};;
        e) END_YEAR=${OPTARG};;
        r) RESCRAPE=${OPTARG};;
    esac
done
git pull
git add .
Rscript phf_daily_scrape.R -s $START_YEAR -e $END_YEAR
Rscript nhl_daily_scrape.R -s $START_YEAR -e $END_YEAR
git add .
git commit -m "PHF and NHL Play-by-play and Schedules update" || echo "No changes to commit"
git pull
git push