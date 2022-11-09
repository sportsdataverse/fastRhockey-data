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
Rscript nhl_daily_scrape.R -s $START_YEAR -e $END_YEAR
git add .
git pull
git commit -m "NHL Play-by-Play and Schedules update (Start: $START_YEAR End: $END_YEAR)" || echo "No changes to commit"
git pull
git push