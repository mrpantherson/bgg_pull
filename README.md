# bgg_pull
This module supports grabbing information from the board game geek website (https://boardgamegeek.com/),
output is a csv file.  I couldn't find a way to get games by rank through the api, so I scrape the game ranks
then use the api to get more details on each game.  The frist step is scraping, once the first 5K games are
grabbed and saved out, user can call get from api to fill out all details

- call ScrapeRanks, this should take a while (~10 min) and gets the urls and rank of all games from the site
- call GetFromApi, this should also take a while (~10 min) and gets the remaining information through API calls

# usage
From the command line use the following arguments, the output of this is a dataframe.

- -s or --scrape to get the names and id of the top 5K games
- -a or --api to get information about the top 5K games