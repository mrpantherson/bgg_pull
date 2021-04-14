import requests
import bs4
import pandas
import time
import numpy as np
from xml.etree import ElementTree
import argparse
import errno
import sys
from io import BytesIO
from PIL import Image
import requests
import os
import logging
import json


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
                 ('thumbnail', 'thumb_url'),
                 ('age', 'age'),
                 ('boardgamemechanic', 'mechanic'),
                 ('statistics/ratings/owned', 'owned'),
                 ('boardgamecategory', 'category'),
                 ('boardgamedesigner', 'designer'),
                 ('boardgamepublisher', 'publisher'),
                 ('statistics/ratings/averageweight', 'weight')]


def ScrapeRanks(page_start=1, page_end=51, tags_cols=tag_col_lookup):
    '''
    :param page_start: which page to start grabbing data from (1 is first page)
    :param page_end: which page to stop grabbing data from (not inclusive, 51 should be the stop point)
    :param tags_cols: uses the column names to build dataframe
    :param output_name: which name to use for CSV, GetFromApi is expecting bgg_db.csv as a default
    '''
    game_id, rank_list, bgg_url = [], [], []
    for index in range(page_start, page_end):
        args.logger.info(f'Grabbing page {index}')
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

    str_names = ['names', 'image_url', 'thumb_url', 'mechanic', 'category', 'designer', 'publisher']
    df_dict = {'bgg_url':bgg_url, 'game_id':game_id}
    df = pandas.DataFrame(df_dict, index=rank_list).rename_axis('rank')
    for tag, col in tags_cols:
        df[col] = 'x' if col in str_names else np.nan
    path = os.path.join(args.out_path, args.out_name)
    df.to_csv(path)


def GetFromApi(loops=100, tags_cols=tag_col_lookup):
    '''
    :param loops: how many games to try and grab, advised to do 100, 50 times
    https://www.boardgamegeek.com/xmlapi
    :param tags_cols: 
    :param name_in: 
    :param name_out: 
    '''

    path = os.path.join(args.out_path, args.out_name)
    df = pandas.read_csv(path, encoding='utf8')
    # search through df for null entries, add these to batch list
    ids_todo = []
    for index, row in df.iterrows():
        if len(ids_todo) >= loops:
            break
        if np.isnan(row['min_players']):
            ids_todo.append(str(row['game_id']))
    url = 'https://www.boardgamegeek.com/xmlapi/boardgame/{}?&stats=1'.format(','.join(ids_todo))
    args.logger.info(f'Grabbing info from {url}')
    response = requests.get(url)
    if response.status_code != 200:
        args.logger.error(f'Problem grabbing from API:  {response.status_code}')
        sys.exit(1)

    # these tags will return multiple results, will need to be handled slightly differently
    multi_tags = ['mechanic', 'category', 'designer']
    tree = ElementTree.fromstring(response.content)
    args.logger.info('Inserting games')
    for game in tree:
        id = game.attrib['objectid']
        df_index = df[df['game_id'] == int(id)].index
        for tag, var in tags_cols:
            # special case for grabbing english name
            if var == 'names':
                for sub in game.findall(tag):
                    if 'primary' in sub.attrib: #grab the english name
                        df.at[df_index, var] = sub.text if sub else 'none'
                        break
            # multi tag items need to be handled slightly different
            elif var in multi_tags:
                multi = []
                for sub in game.findall(tag):
                    multi.append(sub.text if sub else 'none')
                df.at[df_index, var] = ', '.join(multi) if len(multi) else 'none'
            # all normal nodes handled here
            else:
                node = game.find(tag)
                df.at[df_index, var] = node.text if node else 'none'

    # save results out
    path = os.path.join(args.out_path, args.out_name)
    df.to_csv(path, index=False)


def VizIt(args):
    """ This function generates a viz using pictures of top rated board games
    
    Arguments:
        args -- command line arguments
    """
    path = os.path.join(args.out_path, args.out_name)
    df = pandas.read_csv(path)
    df = df.loc[:args.n_total, 'thumb_url']

    _x, _y = 0, 0
    new_im = Image.new('RGB', (args.out_width, args.out_height))
    for index, item in enumerate(df):
        pic_req = requests.get(item)
        im = Image.open(BytesIO(pic_req.content))
        pic_w, pic_h = im.size
        new_im.paste(im, (_x, _y))
        _x += pic_w + 10
        if index % args.n_cols == 0 and index > 0:
            _y += args.thumb_w
            _x = 0
        time.sleep(5)

    path = os.path.join(args.out_path, args.viz_name)
    new_im.save(path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Collects information for the top 5K games on BGG')
    parser.add_argument('-s', '--scrape', dest='do_scrape', action='store_true', help='Specify if you want to scrape')
    parser.add_argument('-a', '--api', dest='api_grabs', type=int, default=0, help='how many groups of 100 to grab, keep doing this until db full')
    parser.add_argument('-v', '--viz', dest='do_viz', action='store_true', help='Specify if you want to generate viz')
    args = parser.parse_args()

    # add extra useful stuff
    args.cfgpath = "../config.json"
    with open(args.cfgpath, 'r') as file:
        args.config = json.load(file)
    args.thumb_w = args.config['thumb_w']
    args.thumb_h = args.config['thumb_h']
    args.n_rows = args.config['n_rows']
    args.n_cols = args.config['n_cols']
    args.n_total = args.n_rows * args.n_cols
    args.out_width = args.n_cols * args.thumb_w
    args.out_height = args.n_rows * args.thumb_h
    args.out_name = args.config['out_name']
    args.viz_name = args.config['viz_name']
    args.log_path = args.config['log_path']
    args.out_path = args.config['out_path']

    # critical - error - warning - info - debug
    args.logger = logging.getLogger(__name__)
    args.logger.setLevel(logging.DEBUG)
    # file handler
    path = os.path.join(args.log_path, 'log.txt')
    fh = logging.FileHandler(path, mode='a')
    fh.setLevel(logging.INFO)
    args.logger.addHandler(fh)
    # console handler
    sh = logging.StreamHandler()
    sh.setLevel(logging.INFO)
    args.logger.addHandler(sh)

    # log run commands 
    sep = '-' * 80
    arg_str = ' '.join(sys.argv)
    args.logger.info(f'{sep}\npython {arg_str}\n')

    # validate input
    if not 0 <= args.api_grabs <= 50:
        args.logger.error('invalid value for api_grabs [0, 50]')
        sys.exit(1)

    if args.do_scrape:
        args.logger.info('Begining scrape')
        ScrapeRanks()
    
    for i in range(args.api_grabs):
        args.logger.info(f'Api grab {i}')
        GetFromApi()
        time.sleep(5)

    if args.do_viz:
        args.logger.info('Generating viz')
        VizIt(args)
