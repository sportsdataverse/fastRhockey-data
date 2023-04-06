# fastRhockey-data
 PHF & NHL Data

This repository holds historical boxscore and play-by-play data for the
Premier Hockey Federation (PHF, formerly known as NWHL), which was
compiled with the
[**`fastRhockey`**](https://github.com/BenHowell71/fastRhockey/) package
from [GitHub](https://github.com/BenHowell71/fastRhockey).

You can find
[**`fastRhockey`**](https://github.com/BenHowell71/fastRhockey/) here:
[BenHowell71/fastRhockey](https://github.com/BenHowell71/fastRhockey/)

The scraper was created to increase access to play-by-play and boxscore
data for the PHF, which has historically been one of the bigger barriers
to entry regarding women’s hockey analytics.

<center>

<img src="themes/fastRhockey_full_holographic_graphic.png" style="width:50.0%" />

</center>

------------------------------------------------------------------------

## Data

This repo contains three main CSVs of data, each of which is outlined in
a little more detail below.

-   `phf_meta_data.csv`: this csv contains all the data that you’d want
    on an individual game in one row. Contains home/away teams, arena
    information, game IDs, league IDs, and more  
-   `boxscore.csv`: this csv contains all the boxscore information from
    the PHF for the games in `phf_meta_data.csv`. Contains data on game
    ID, scoring by period, shots be period, power play numbers, and
    more, all broken down by each team involved in a game  
-   `play_by_play.csv`: this csv contains all the play-by-play data from
    the PHF. It includes information on events, how many skaters were on
    the ice, penalties, shots, etc. This data is essentially complete
    for the more recent PHF seasons, while it is spottier, usually just
    goals and penalties, from the early seasons of the league

The best way to get familiar with this data is to use it! You can either
download directly from this repo or use
[**`fastRhockey`**](https://github.com/BenHowell71/fastRhockey/) to
scrape the data yourself.

------------------------------------------------------------------------

## **Follow [SportsDataverse](https://twitter.com/sportsdataverse) on Twitter and star fastRhockey**

[![Twitter
Follow](https://img.shields.io/twitter/follow/sportsdataverse?color=blue&label=%40sportsdataverse&logo=twitter&style=for-the-badge)](https://twitter.com/sportsdataverse)

[![GitHub
stars](https://img.shields.io/github/stars/BenHowell71/fastRhockey.svg?color=eee&logo=github&style=for-the-badge&label=Star%20fastRhockey&maxAge=2592000)](https://github.com/BenHowell71/fastRhockey/stargazers/)

## **Our Authors**

-   [Ben Howell](https://twitter.com/BenHowell71)  
    <a href="https://twitter.com/BenHowell71" target="blank"><img src="https://img.shields.io/twitter/follow/BenHowell71?color=blue&label=%40BenHowell71&logo=twitter&style=for-the-badge" alt="@BenHowell71" /></a>
    <a href="https://github.com/BenHowell71" target="blank"><img src="https://img.shields.io/github/followers/BenHowell71?color=eee&logo=Github&style=for-the-badge" alt="@BenHowell71" /></a>

## **Our Contributors (they’re awesome)**

-   [Saiem Gilani](https://twitter.com/saiemgilani)  
    <a href="https://twitter.com/saiemgilani" target="blank"><img src="https://img.shields.io/twitter/follow/saiemgilani?color=blue&label=%40saiemgilani&logo=twitter&style=for-the-badge" alt="@saiemgilani" /></a>
    <a href="https://github.com/saiemgilani" target="blank"><img src="https://img.shields.io/github/followers/saiemgilani?color=eee&logo=Github&style=for-the-badge" alt="@saiemgilani" /></a>  
-   [Alyssa Longmuir](https://twitter.com/alyssastweeting)  
    <a href="https://twitter.com/alyssastweeting" target="blank"><img src="https://img.shields.io/twitter/follow/alyssastweeting?color=blue&label=%40alyssastweeting&logo=twitter&style=for-the-badge" alt="@alyssastweeting" /></a>
    <a href="https://github.com/Aklongmuir" target="blank"><img src="https://img.shields.io/github/followers/Aklongmuir?color=eee&logo=Github&style=for-the-badge" alt="@Aklongmuir" /></a>
-   [Tan Ho](https://twitter.com/_TanHo)  
    <a href="https://twitter.com/_TanHo" target="blank"></a>
    <a href="https://github.com/tanho63" target="blank"><img src="https://img.shields.io/github/followers/tanho63?color=eee&logo=Github&style=for-the-badge" alt="@tanho63" /></a>

## **Citations**

To cite the
[**`fastRhockey`**](https://benhowell71.github.io/fastRhockey/) R
package in publications, use:

BibTex Citation

``` bibtex
@misc{howell_fastRhockey_2021,
  author = {Ben Howell},
  title = {fastRhockey: The SportsDataverse's R Package for Women's Hockey Data.},
  url = {https://benhowell71.github.io/fastRhockey/},
  year = {2021}
}
```
