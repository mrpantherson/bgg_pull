import requests
import bs4
import pandas
import time
import numpy as np
from xml.etree import ElementTree
import argparse
import errno
import sys

'''
This module supports grabbing information from the board game geek website (https://boardgamegeek.com/),
output is a csv file.  I couldn't find a way to get games by rank through the api, so I scrape the game ranks
then use the api to get more details on each game.  The frist step is scraping, once the first 5K games are
grabbed and saved out, user can call get from api to fill out all details

- call ScrapeRanks, this should take a while (~10 min) and gets the urls and rank of all games from the site
- call GetFromApi, this should also take a while (~10 min) and gets the remaining information through API calls
'''

# for whatever reason this game has no rank and is missing data, could add code to delete, or whack manually
# https://boardgamegeek.com/boardgame/57161/showdown

# for easy lookup later we can pair the xml tags with the respective dataframe columns
tag_col_lookup = [('name', 'names'),
                 ('minplayers', 'min_players'),
                 ('maxplayers', 'max_players'),
                 ('playingtime', 'avg_time'),
                 ('minplaytime', 'min_time'),
                 ('maxplaytime', 'max_time'),
                 ('yearpublished', 'year'),
                 ('statistics/ratings/average', 'avg_rating'),
                 ('statistics/ratings/bayesaverage', 'geek_rating'),
                 ('statistics/ratings/usersrated', 'num_votes'),
                 ('image', 'image_url'),
                 ('age', 'age'),
                 ('boardgamemechanic', 'mechanic'),
                 ('statistics/ratings/owned', 'owned'),
                 ('boardgamecategory', 'category'),
                 ('boardgamedesigner', 'designer'),
                 ('boardgamepublisher', 'publisher'),
                 ('statistics/ratings/averageweight', 'weight')]


def ScrapeRanks(page_start=1, page_end=51, tags_cols=tag_col_lookup, output_name='bgg_db.csv'):
    '''
    :param page_start: which page to start grabbing data from (1 is first page)
    :param page_end: which page to stop grabbing data from (not inclusive, 51 should be the stop point)
    :param tags_cols: uses the column names to build dataframe
    :param output_name: which name to use for CSV, GetFromApi is expecting bgg_db.csv as a default
    '''
    game_id, rank_list, bgg_url = [], [], []
    for index in range(page_start, page_end):
        print("Scraping Page: {}".format(index))
        url = "https://boardgamegeek.com/search/boardgame/page/{}?sort=rank&advsearch=1&q=&include%5Bdesignerid%5D=&include" \
              "%5Bpublisherid%5D=&geekitemname=&range%5Byearpublished%5D%5Bmin%5D=&range%5Byearpublished%5D%5Bmax%5D=&range%5B" \
              "minage%5D%5Bmax%5D=&range%5Bnumvoters%5D%5Bmin%5D=&range%5Bnumweights%5D%5Bmin%5D=&range%5Bminplayers%5D%5Bmax%" \
              "5D=&range%5Bmaxplayers%5D%5Bmin%5D=&range%5Bleastplaytime%5D%5Bmin%5D=&range%5Bplaytime%5D%5Bmax%5D=&floatrange%" \
              "5Bavgrating%5D%5Bmin%5D=&floatrange%5Bavgrating%5D%5Bmax%5D=&floatrange%5Bavgweight%5D%5Bmin%5D=&floatrange%" \
              "5Bavgweight%5D%5Bmax%5D=&colfiltertype=&playerrangetype=normal&B1=Submit&sortdir=asc".format(index)
        req = requests.get(url)
        soup = bs4.BeautifulSoup(req.text, "html.parser")
        for iter in range(1, 101):
            soup_iter = "results_objectname{}".format(iter)
            url = soup.find_all("div", id=soup_iter)
            for sub_url in url:
                for tag in sub_url:
                    if 'a' == tag.name:
                        bgg_url.append("https://boardgamegeek.com{}".format(tag["href"]))
                        game_id.append(bgg_url[-1].split('/')[4])
        ranks = soup.find_all("td", class_="collection_rank")
        for rank in ranks:
            tempo = rank.get_text().strip('\n\t ')
            rank_list.append(tempo)
        time.sleep(5)

    str_names = ['names', 'image_url', 'mechanic', 'category', 'designer', 'publisher']
    df_dict = {'bgg_url':bgg_url, 'game_id':game_id}
    df = pandas.DataFrame(df_dict, index=rank_list).rename_axis('rank')
    for tag, col in tags_cols:
        df[col] = 'x' if col in str_names else np.nan
    df.to_csv(output_name)


# https://www.boardgamegeek.com/xmlapi
def GetFromApi(loops=100, tags_cols=tag_col_lookup, name_in='bgg_db.csv', name_out='bgg_db.csv'):
    '''
    :param loops: how many games to try and grab, advised to do 100, 50 times
    :param tags_cols: 
    :param name_in: 
    :param name_out: 
    '''
    df = pandas.read_csv(name_in, encoding='utf8')
    # batch up a bunch of ids so we don't have to do so many api calls
    ids_todo = []
    for index, row in df.iterrows():
        if (len(ids_todo) >= loops):
            break
        # row must be empty
        if np.isnan(row['min_players']):
            ids_todo.append(str(row['game_id']))
    url = 'https://www.boardgamegeek.com/xmlapi/boardgame/{}?&stats=1&marketplace=1'.format(','.join(ids_todo))
    print('Grabbing info from {}'.format(url))
    response = requests.get(url)
    if response.status_code != 200:
        print('Problem grabbing from API:  {}'.format(response.status_code))
        quit()

    # these tags will return multiple results, will need to be handled slightly differently
    multi_tags = ['mechanic', 'category', 'designer']
    tree = ElementTree.fromstring(response.content)
    for game in tree:
        id = game.attrib['objectid']
        print('Inserting id:{}'.format(id))
        df_index = df[df['game_id'] == int(id)].index
        for tag, var in tags_cols:
            if var == 'names':
                for sub in game.findall(tag):
                    if 'primary' in sub.attrib: #grab the english name
                        df.set_value(df_index, var, sub.text if sub != None else 'none')
                        break
            elif var in multi_tags:
                multi = []
                for sub in game.findall(tag):
                    multi.append(sub.text if sub != None else 'none')
                df.set_value(df_index, var, ', '.join(multi) if len(multi) else 'none')
            else:
                node = game.find(tag)
                df.set_value(df_index, var, node.text if node != None else 'none')
    df.to_csv(name_out, index=False)
    time.sleep(5)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Collects information for the top 5K games on BGG')
    parser.add_argument('-s', '--scrape', dest='do_scrape', type=bool, default=False, help='True, or skip entirely for False')
    parser.add_argument('-a', '--api', dest='api_grabs', type=int, default=0, help='how many groups of 100 to grab')
    args = parser.parse_args()

    # validate input
    if not 0 <= args.api_grabs <= 50:
        print('invalid value for api_grabs [0, 50]')
        sys.exit(1)

    if args.do_scrape:
        ScrapeRanks()
    
    for i in range(args.api_grabs):
        GetFromApi()
