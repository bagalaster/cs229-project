'''
File: gd_parser.py
-----------------------
A scraper that acquires data from MLB Advanced Media's Gameday Servers.
This scraper is a modified version of a script written by johnchoiniere.
His repo can be found at this github link

https://github.com/johnchoiniere/pfx_parser
'''
import re
from bs4 import BeautifulSoup, UnicodeDammit
from urllib.request import urlopen
import itertools
import os, sys
import datetime
import time
import sqlite3
import multiprocessing as mp
import pdb

NWORKERS = 16

STARTDATES = [datetime.date(2007 + 3*i,1,1) for i in range(4)]
ENDDATES = [datetime.date(2009 + 3*i,12,31) for i in range(4)]
LEAGUES = ['mlb','aaa','aax','afa','afx','asx','rok']

CONN = sqlite3.connect('elo.db')
curr = CONN.cursor()

TABLE_NAMES = ["atbats_" + l.lower() for l in LEAGUES]

# for table_name in TABLE_NAMES:
	#curr.execute("DROP TABLE IF EXISTS {0}".format(table_name))
	#curr.execute("CREATE TABLE {0} (\
        #game_id VARCHAR(20) NOT NULL,\
        #game_date DATE NOT NULL,\
        #ab_number INTEGER NOT NULL,\
        #league CHAR(3) NOT NULL,\
        #BatterID INTEGER NOT NULL,\
        #PitcherID INTEGER NOT NULL,\
        #EventCode INTEGER NOT NULL\
        #);".format(table_name))

def gd_scrape(startdate, enddate, table_name, conn):
        db = conn.cursor()
        league = table_name[-3:]

        base_url = "http://gdx.mlb.com/components/game/" + league.lower() + "/"

        delta = enddate - startdate
        prior_d_url = ""

        for i in range(delta.days+1):
                entries = []
                active_date = (startdate+datetime.timedelta(days=i))
                print(base_url+"year_"+str((startdate+datetime.timedelta(days=i)).year)+"/month_"+active_date.strftime('%m')+"/day_"+active_date.strftime('%d'))
                try:
                        urlopen(base_url+"year_"+str((startdate+datetime.timedelta(days=i)).year)+"/month_"+active_date.strftime('%m')+"/day_"+active_date.strftime('%d'))
                        d_url = base_url+"year_"+str((startdate+datetime.timedelta(days=i)).year)+"/month_"+active_date.strftime('%m')+"/day_"+active_date.strftime('%d')
                except:
                        print("excepted")
                        d_url = prior_d_url
                if d_url!=prior_d_url:
                        day_soup = BeautifulSoup(urlopen(d_url),"lxml")
                        myList = day_soup.find_all("a", href=re.compile("gid_.*"))
                        for game in myList:
                                g = game.get_text().strip()
                                if type(game.get_text().strip()[len(game.get_text().strip())-2:len(game.get_text().strip())-1])==type(int(1)):
                                        game_number = game.get_text().strip()[len(game.get_text().strip())-2:len(game.get_text().strip())-1]
                                else:
                                        game_number = 1
                                g_url = d_url+ "/" + g
                                print(g)
                                try:
                                        if BeautifulSoup(urlopen(g_url),"lxml").find("a", href="game.xml"):
                #                                time.sleep(1)
                                                detail_soup = BeautifulSoup(urlopen(g_url+"game.xml"), "lxml")

                                                # Team code
                                                if detail_soup.find("team"):
                                                        home_team_id = detail_soup.find("team", type="home")["code"]
                                                        away_team_id = detail_soup.find("team", type="away")["code"]
                                                        home_team_lg = detail_soup.find("team", type="home")["league"]
                                                        away_team_lg = detail_soup.find("team", type="away")["league"]
                                                else:
                                                        home_team_id = "unknown"
                                                        away_team_id = "unknown"
                                                        home_team_lg = "unknown"
                                                        away_team_lg = "unknown"

                                        # Game ID
                                        retro_game_id=home_team_id.upper()+str(active_date.year)+str(active_date.strftime('%m'))+str(active_date.strftime('%d'))+str(int(game_number)-1)

                                        # url stuff
                                        inn_url = g_url+"inning/"
                                        try:
                                                urlopen(inn_url)
                                                tested_inn_url = inn_url
                                        except:
                                                continue
                                        
                                        game_hits = 0
                                        for inning in BeautifulSoup(urlopen(tested_inn_url),"lxml").find_all("a", href=re.compile("inning_\d*.xml")):
                                                inn_soup = BeautifulSoup(urlopen(inn_url+inning.get_text().strip()), "xml")

                                                if inn_soup.inning.find("top"):
                                                        for ab in inn_soup.inning.top.find_all("atbat"):
                                                                # Get batter/pitcher id/handedness
                                                                if 'batter' in ab.attrs:
                                                                        bat_mlbid = ab["batter"]
                                                                else:
                                                                        bat_mlbid = ""

                                                                if 'pitcher' in ab.attrs:
                                                                        pit_mlbid = ab["pitcher"]
                                                                else:
                                                                        pit_mlbid = ""

                                                                if 'des' in ab.attrs:
                                                                        ab_des = ab["des"]
                                                                else:
                                                                        ab_des = ""
                                                                if 'num' in ab.attrs:
                                                                        ab_number = ab["num"]
                                                                else:
                                                                        ab_number = ""

                                                                ab_id = retro_game_id + "AB" + str(ab_number)

                                                                # Event descriptions (!!!)
                                                                if 'event' in ab.attrs:
                                                                        event_tx = ab["event"]
                                                                else:
                                                                        event_tx = ""
                                                                event_cd=""
                                                                if event_tx=="Flyout" or event_tx=="Fly Out" or event_tx=="Sac Fly" or event_tx=="Sac Fly DP":
                                                                        event_cd=2
                                                                        battedball_cd="F"
                                                                elif event_tx=="Lineout" or event_tx=="Line Out" or event_tx=="Bunt Lineout":
                                                                        event_cd=2
                                                                        battedball_cd="L"
                                                                elif event_tx=="Pop out" or event_tx=="Pop Out" or event_tx=="Bunt Pop Out":
                                                                        event_cd=2
                                                                        battedball_cd="P"
                                                                elif event_tx=="Groundout" or event_tx=="Ground Out" or event_tx=="Sac Bunt" or event_tx=="Bunt Groundout":
                                                                        event_cd=2
                                                                        battedball_cd="G"
                                                                elif event_tx=="Grounded Into DP":
                                                                        event_cd=2
                                                                        battedball_cd="G"
                                                                elif event_tx=="Forceout":
                                                                        event_cd=2
                                                                        if ab_des.lower().count("grounds")>0:
                                                                                battedball_cd="G"
                                                                        elif ab_des.lower().count("lines")>0:
                                                                                battedball_cd="L"
                                                                        elif ab_des.lower().count("flies")>0:
                                                                                battedball_cd="F"
                                                                        elif ab_des.lower().count("pops")>0:
                                                                                battedball_cd="P"
                                                                        else:
                                                                                battedball_cd="U"
                                                                elif event_tx=="Double Play" or event_tx=="Triple Play" or event_tx=="Sacrifice Bunt D":
                                                                        event_cd=2
                                                                        if ab_des.lower().count("ground")>0:
                                                                                battedball_cd="G"
                                                                        elif ab_des.lower().count("lines")>0:
                                                                                battedball_cd="L"
                                                                        elif ab_des.lower().count("flies")>0:
                                                                                battedball_cd="F"
                                                                        elif ab_des.lower().count("pops")>0:
                                                                                battedball_cd="P"
                                                                        else:
                                                                                battedball_cd="U"
                                                                elif event_tx=="Strikeout" or event_tx=="Strikeout - DP":
                                                                        event_cd=3
                                                                elif event_tx=="Walk":
                                                                        event_cd=14
                                                                elif event_tx=="Intent Walk":
                                                                        event_cd=15
                                                                elif event_tx=="Hit By Pitch":
                                                                        event_cd=16
                                                                elif event_tx.lower().count("interference")>0:
                                                                        event_cd=17
                                                                elif event_tx[-5:]=="Error":
                                                                        event_cd=18
                                                                        battedball_cd = 'U'
                                                                elif event_tx=="Fielders Choice Out" or event_tx=="Fielders Choice":
                                                                        event_cd=19
                                                                        battedball_cd = 'U'
                                                                elif event_tx=="Single":
                                                                        event_cd=20
                                                                        if ab_des.count("on a line drive")>0:
                                                                                battedball_cd="L"
                                                                        elif ab_des.count("fly ball")>0:
                                                                                battedball_cd="F"
                                                                        elif ab_des.count("ground ball")>0:
                                                                                battedball_cd="G"
                                                                        elif ab_des.count("pop up")>0:
                                                                                battedball_cd="P"
                                                                        else:
                                                                                battedball_cd="U"
                                                                elif event_tx=="Double":
                                                                        event_cd=21
                                                                        if ab_des.count("line drive")>0:
                                                                                battedball_cd="L"
                                                                        elif ab_des.count("fly ball")>0:
                                                                                battedball_cd="F"
                                                                        elif ab_des.count("ground ball")>0:
                                                                                battedball_cd="G"
                                                                        elif ab_des.count("pop up")>0:
                                                                                battedball_cd="P"
                                                                        else:
                                                                                battedball_cd="U"
                                                                elif event_tx=="Triple":
                                                                        event_cd=22
                                                                        if ab_des.count("line drive")>0:
                                                                                battedball_cd="L"
                                                                        elif ab_des.count("fly ball")>0:
                                                                                battedball_cd="F"
                                                                        elif ab_des.count("ground ball")>0:
                                                                                battedball_cd="G"
                                                                        elif ab_des.count("pop up")>0:
                                                                                battedball_cd="P"
                                                                        else:
                                                                                battedball_cd="U"
                                                                elif event_tx=="Home Run":
                                                                        event_cd=23
                                                                        if ab_des.count("on a line drive")>0:
                                                                                battedball_cd="L"
                                                                        elif ab_des.count("fly ball")>0:
                                                                                battedball_cd="F"
                                                                        elif ab_des.count("ground ball")>0:
                                                                                battedball_cd="G"
                                                                        elif ab_des.count("pop up")>0:
                                                                                battedball_cd="P"
                                                                        else:
                                                                                battedball_cd="U"
                                                                elif event_tx=="Runner Out":
                                                                        if ab_des.lower().count("caught stealing")>0:
                                                                                event_cd=6
                                                                        elif ab_des.lower().count("picks off")>0:
                                                                                event_cd=8
                                                                else:
                                                                        event_cd=99

                                                                entries.append((retro_game_id, active_date.strftime("%Y-%m-%d"), ab_number, league, bat_mlbid, pit_mlbid, event_cd))
                                                                
                                                if inn_soup.inning.find("bottom"):
                                                        for ab in inn_soup.inning.bottom.find_all("atbat"):
                                                                # Batter/pitcher id/handedness
                                                                bat_home_id=1
                                                                if 'batter' in ab.attrs:
                                                                        bat_mlbid = ab["batter"]
                                                                else:
                                                                        bat_mlbid = ""

                                                                if 'pitcher' in ab.attrs:
                                                                        pit_mlbid = ab["pitcher"]
                                                                else:
                                                                        pit_mlbid = ""

                                                                # Other stuff, unknown?
                                                                if 'des' in ab.attrs:
                                                                        ab_des = ab["des"]
                                                                else:
                                                                        ab_des = ""
                                                                if 'num' in ab.attrs:
                                                                        ab_number = ab["num"]
                                                                else:
                                                                        ab_number = ""

                                                                ab_id = retro_game_id + "AB" + str(ab_number)

                                                                # Event description (!!!)
                                                                if 'event' in ab.attrs:
                                                                        event_tx = ab["event"]
                                                                else:
                                                                        event_tx = ""
                                                                if event_tx=="Flyout" or event_tx=="Fly Out" or event_tx=="Sac Fly" or event_tx=="Sac Fly DP":
                                                                        event_cd=2
                                                                        battedball_cd="F"
                                                                elif event_tx=="Lineout" or event_cd=="Line Out" or event_tx=="Bunt Lineout":
                                                                        event_cd=2
                                                                        battedball_cd="L"
                                                                elif event_tx=="Pop out" or event_tx=="Pop Out" or event_tx=="Bunt Pop Out":
                                                                        event_cd=2
                                                                        battedball_cd="P"
                                                                elif event_tx=="Groundout" or event_tx=="Ground Out" or event_tx=="Sac Bunt" or event_tx=="Bunt Groundout":
                                                                        event_cd=2
                                                                        battedball_cd="G"
                                                                elif event_tx=="Grounded Into DP":
                                                                        event_cd=2
                                                                        battedball_cd="G"
                                                                elif event_tx=="Forceout" or event_tx=="Force Out":
                                                                        event_cd=2
                                                                        if ab_des.lower().count("grounds")>0:
                                                                                battedball_cd="G"
                                                                        elif ab_des.lower().count("lines")>0:
                                                                                battedball_cd="L"
                                                                        elif ab_des.lower().count("flies")>0:
                                                                                battedball_cd="F"
                                                                        elif ab_des.lower().count("pops")>0:
                                                                                battedball_cd="P"
                                                                        else:
                                                                                battedball_cd="U"
                                                                elif event_tx=="Double Play" or event_tx=="Triple Play" or event_tx=="Sacrifice Bunt D":
                                                                        event_cd=2
                                                                        if ab_des.lower().count("ground")>0:
                                                                                battedball_cd="G"
                                                                        elif ab_des.lower().count("lines")>0:
                                                                                battedball_cd="L"
                                                                        elif ab_des.lower().count("flies")>0:
                                                                                battedball_cd="F"
                                                                        elif ab_des.lower().count("pops")>0:
                                                                                battedball_cd="P"
                                                                        else:
                                                                                battedball_cd="U"
                                                                elif event_tx=="Strikeout" or event_tx=="Strikeout - DP":
                                                                        event_cd=3
                                                                elif event_tx=="Walk":
                                                                        event_cd=14
                                                                elif event_tx=="Intent Walk":
                                                                        event_cd=15
                                                                elif event_tx=="Hit By Pitch":
                                                                        event_cd=16
                                                                elif event_tx.lower().count("interference")>0:
                                                                        event_cd=17
                                                                elif event_tx[-5:]=="Error":
                                                                        event_cd=18
                                                                        battedball_cd = 'U'
                                                                elif event_tx=="Fielders Choice Out" or event_tx=="Fielders Choice":
                                                                        event_cd=19
                                                                        battedball_cd = 'U'
                                                                elif event_tx=="Single":
                                                                        event_cd=20
                                                                        if ab_des.count("on a line drive")>0:
                                                                                battedball_cd="L"
                                                                        elif ab_des.count("fly ball")>0:
                                                                                battedball_cd="F"
                                                                        elif ab_des.count("ground ball")>0:
                                                                                battedball_cd="G"
                                                                        elif ab_des.count("pop up")>0:
                                                                                battedball_cd="P"
                                                                        else:
                                                                                battedball_cd="U"
                                                                elif event_tx=="Double":
                                                                        event_cd=21
                                                                        if ab_des.count("line drive")>0:
                                                                                battedball_cd="L"
                                                                        elif ab_des.count("fly ball")>0:
                                                                                battedball_cd="F"
                                                                        elif ab_des.count("ground ball")>0:
                                                                                battedball_cd="G"
                                                                        elif ab_des.count("pop up")>0:
                                                                                battedball_cd="P"
                                                                        else:
                                                                                battedball_cd="U"
                                                                elif event_tx=="Triple":
                                                                        event_cd=22
                                                                        if ab_des.count("line drive")>0:
                                                                                battedball_cd="L"
                                                                        elif ab_des.count("fly ball")>0:
                                                                                battedball_cd="F"
                                                                        elif ab_des.count("ground ball")>0:
                                                                                battedball_cd="G"
                                                                        elif ab_des.count("pop up")>0:
                                                                                battedball_cd="P"
                                                                        else:
                                                                                battedball_cd="U"
                                                                elif event_tx=="Home Run":
                                                                        event_cd=23
                                                                        if ab_des.count("on a line drive")>0:
                                                                                battedball_cd="L"
                                                                        elif ab_des.count("fly ball")>0:
                                                                                battedball_cd="F"
                                                                        elif ab_des.count("ground ball")>0:
                                                                                battedball_cd="G"
                                                                        elif ab_des.count("pop up")>0:
                                                                                battedball_cd="P"
                                                                        else:
                                                                                battedball_cd="U"
                                                                elif event_tx=="Runner Out":
                                                                        if ab_des.lower().count("caught stealing")>0:
                                                                                event_cd=6
                                                                        elif ab_des.lower().count("picks off")>0:
                                                                                event_cd=8
                                                                else:
                                                                        event_cd=99

                                                                entries.append((retro_game_id, active_date.strftime("%Y-%m-%d"), ab_number, league, bat_mlbid, pit_mlbid, event_cd))
                                except:
                                        print('Error occured while scraping this game.')
                db.executemany("INSERT INTO {0} VALUES (\
                        ?,\
                        ?,\
                        ?,\
                        ?,\
                        ?,\
                        ?,\
                        ?\
                );".format(table_name), entries)
                conn.commit()
                prior_d_url = d_url

if __name__ == "__main__":
        def ab_table(dates, table):
                gd_scrape(*dates, table, CONN)

        #DATES = zip(STARTDATES, ENDDATES)
        #iterator = itertools.product(DATES, TABLE_NAMES)
        starts = [datetime.date(2013,7,22),datetime.date(2008,4,7),datetime.date(2011,8,9),datetime.date(2010,3,7)]
        ends = [datetime.date(2015,12,31),datetime.date(2010,12,31),datetime.date(2012,12,31),datetime.date(2012,12,31)]
        levels = ['afa','afx','aaa','mlb']
        tables = ['atbats_' + l for l in levels]

        iterator = zip(zip(starts,ends),tables)
        # pdb.set_trace()
        with mp.Pool(NWORKERS) as pool:
                pool.starmap(ab_table, iterator)
