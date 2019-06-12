'''
File: calc_elo.py
By Mac Bagwell
---------------------
This file constructs the point-in-time elo database from the database
created by gd_parsing.py
'''

import sqlite3
import datetime
import numpy as np
import multiprocessing as mp
import itertools as it
import time
import pdb

# Global constants
CONN = sqlite3.connect('elo.db')
PLAYER_TABLE_NAME = "player_id"
GAMMA = np.log(2)
BATTER_WIN = set([
        14, # Walk
        16, # HBP
        20, # 1B
        21, # 2B
        22, # 3B
        23  # HR
])
PITCHER_WIN = set([
        2,  # Generic Out
        3,  # K
        18, # Error
        19  # FC
])

# Creates a new table with the desired table_name. Drops any existing table with
# the name table_name before creation.
def create_table(conn, table_name):
        curr = conn.cursor()

        drop_query = '''
        DROP TABLE IF EXISTS {0};
        '''.format(table_name)

        create_query = '''
        CREATE TABLE {0} (
                date DATE,
                mlbam_id INTEGER,
                elo INTEGER,
                ew_opp_rating INTEGER,
                league CHAR(3),
                pos CHAR(1)
        )
        '''.format(table_name)

        curr.execute(drop_query)
        curr.execute(create_query)
        conn.commit()

# Returns the mlbam_ids of all of the players who ended their careers before year
def find_retirees(conn, year):
        curr = conn.cursor()

        query = '''
        SELECT key_mlbam FROM {0} WHERE pro_played_last <= {1}
        '''.format(PLAYER_TABLE_NAME, year)

        result = curr.execute(query).fetchall()
        out = []
        for r in result:
                if r[0] is not None and r[0] != "":
                        out.append(r[0])
        return out

# Determines the outcome of the atbat based on the retrosheet event code
def parse_event_code(code):
        if code in BATTER_WIN:
                return 1
        elif code in PITCHER_WIN:
                return 0

        return None

# Creates an empty elos dict with the desired hyperparameters
def create_elos_dict(K = 1, lamda = 0.01, beta = 0.99, offset = 1):
        return {
                'K': K,
                'lambda': lamda,
                'beta': beta,
                'offset': offset,
                'loss': 0,
                'bat': dict(),
                'pit': dict()
        }

# def calc_new_elo(batter_entry, pitcher_entry, batter_win = 1, K = 0.1, lamda = 0.01, beta = 0.99):
#         r_b, r_p = batter_entry['elo'], pitcher_entry['elo']
#         a_b, a_p = batter_entry['ew_opp_rating'], pitcher_entry['ew_opp_rating']
#         ev_batter = 1. / (1 + np.exp(r_p - (r_b - GAMMA)))

#         new_r_b = r_b + K * ((batter_win - ev_batter) - lamda * (r_b - a_b))
#         new_r_p = r_p + K * (-(batter_win - ev_batter) - lamda * (r_p - a_p))

#         new_a_b = (1 - beta) * r_p + beta * a_b
#         new_a_p = (1 - beta) * r_b + beta * a_p

#         ll = batter_win * np.log(ev_batter) + (1 - batter_win) * np.log(1 - ev_batter)

#         if new_r_b < 0:
#                 pdb.set_trace()

#         return new_r_b, new_r_p, new_a_b, new_a_p

# Updates the database. Starts from scratch from with the specified hyperparameters
# if elos_dict is None. Otherwise, starts with the supplied elos_dict
def sim_elos(start_date, end_date, conn, table_name = None, 
        K = 1, lamda = 0.01, beta = 0.99, offset = 1, 
        elos_dict = None):
        curr = conn.cursor()

        start = start_date.strftime("%Y-%m-%d")
        end = end_date.strftime("%Y-%m-%d")
        year = start_date.year

        if elos_dict is None:
                elos_dict = create_elos_dict(K = K, lamda = lamda, beta = beta, offset = offset)
        print("Started calc with K = {}, lambda = {}, beta = {}, offset = {}".format(elos_dict['K'], elos_dict['lambda'], elos_dict['beta'], elos_dict['offset']))
        # Creates entry for new player
        def new_entry(league, year, batter = True):
                return {
                        'elo': league_elos(league), 
                        'ew_opp_rating' : league_elos(league),
                        'league': league, 
                        'pos': 'B' if batter else 'P'
                }

        # Gets the league 
        def league_elos(league):
                o = elos_dict['offset']
                # return {'afa': 100 - 2*o, 'aax': 100 - o, 'aaa': 100, 'mlb': 100 + o}
                if league.lower() == 'rok':
                        return 100 - 5 * o
                elif league.lower() == 'asx':
                        return 100 - 4 * o
                elif league.lower() == 'afx':
                        return 100 - 3 * o
                elif league.lower() == 'afa':
                        return 100 - 2 * o
                elif league.lower() == 'aax':
                        return 100 - o
                elif league.lower() == 'aaa':
                        return 100
                elif league.lower() == 'mlb':
                        return 100 + o

                return None

        # Caclulates new elo and opposition rating
        def calc_new_elo(batter_entry, pitcher_entry, batter_win = 1):
                r_b, r_p = batter_entry['elo'], pitcher_entry['elo']
                a_b, a_p = batter_entry['ew_opp_rating'], pitcher_entry['ew_opp_rating']
                K, lamda, beta = elos_dict['K'], elos_dict['lambda'], elos_dict['beta']
                ev_batter = 1. / (1 + np.exp(r_p - (r_b - GAMMA)))

                new_r_b = r_b + K * ((batter_win - ev_batter) - lamda * (r_b - a_b))
                new_r_p = r_p + K * (-(batter_win - ev_batter) - lamda * (r_p - a_p))

                new_a_b = (1 - beta) * r_p + beta * a_b
                new_a_p = (1 - beta) * r_b + beta * a_p

                eps = 1e-10
                ll = batter_win * np.log(np.clip(ev_batter,eps,1-eps)) + (1 - batter_win) * np.log(np.clip(1-ev_batter,eps,1-eps))
                elos_dict['loss'] += ll

                if new_r_b < 0:
                        pdb.set_trace()

                return new_r_b, new_r_p, new_a_b, new_a_p

        # Main loop
        active_date = start_date
        total_ab = 0
        while active_date <= end_date:
                query = '''SELECT * FROM atbats_all 
                                                WHERE game_date = \"{0}\"
                                                ORDER BY game_id, ab_number;'''.format(active_date.strftime("%Y-%m-%d"))
                for game_id, game_date, ab_number, league, batter_id, pitcher_id, event_code in curr.execute(query).fetchall():
                        total_ab += 1
                        batter_win = parse_event_code(event_code)

                        # Update elos_dict
                        if batter_win is not None:
                                if batter_id not in elos_dict['bat'].keys():
                                        elos_dict['bat'][batter_id] = new_entry(league, year)
                                batter = elos_dict['bat'][batter_id]
                                if league.lower() != batter['league'].lower():
                                        batter['league'] = league  

                                if pitcher_id not in elos_dict['pit'].keys():
                                        elos_dict['pit'][pitcher_id] = new_entry(league, year, False)
                                pitcher = elos_dict['pit'][pitcher_id]
                                if league.lower() != pitcher['league'].lower():
                                        pitcher['league'] = league  

                                # pdb.set_trace()
                                new_batter_elo, new_pitcher_elo, new_batter_a, new_pitcher_a = \
                                        calc_new_elo(batter, pitcher, batter_win)

                                batter['elo'] = new_batter_elo
                                pitcher['elo'] = new_pitcher_elo

                                batter['ew_opp_rating'] = new_batter_a
                                pitcher['ew_opp_rating'] = new_pitcher_a

                # Snapshot of elos_dict state
                if table_name is not None:
                        rows_to_insert = [[active_date] + flatten_item(i) for i in elos_dict['bat'].items()] +\
                                                         [[active_date] + flatten_item(i) for i in elos_dict['pit'].items()]
                        if len(rows_to_insert) > 0:
                                insert_query = '''
                                INSERT INTO {0} VALUES (
                                        ?,?,?,?,?,?
                                );
                                '''.format(table_name)
                                curr.executemany(insert_query, rows_to_insert)
                                conn.commit()
                if active_date.day == 1:
                        print(active_date)
                active_date += datetime.timedelta(days = 1)

        print("Finished calc with K = {}, lambda = {}, beta = {}, offset = {}".format(elos_dict['K'], elos_dict['lambda'], elos_dict['beta'], elos_dict['offset']))
        return elos_dict, total_ab

def flatten_item(i):
        Id = i[0]
        d = i[1]
        return [Id, d['elo'], d['ew_opp_rating'], d['league'], d['pos']]

def main():
        begin = time.time()
        db_name = 'elo.db'
        table_name = 'elo'
        conn = sqlite3.connect(db_name)
        create_table(conn, table_name)

        total_ab = 0
        elos_dict = None
        for year in range(2007, 2019):
                start_date = datetime.date(year,1,1)
                end_date = datetime.date(year,12,31)

                elos_dict, ab = sim_elos(start_date, end_date, conn, table_name, \
                        K = 0.01, lamda = 0.01, beta = 0.9, offset = 0, elos_dict = elos_dict)
                total_ab += ab
                retirees = find_retirees(conn, year)
                for r in retirees:
                        if r in elos_dict['bat'].keys():
                                del elos_dict['bat'][r]
                        if r in elos_dict['pit'].keys():
                                del elos_dict['pit'][r]

        conn.close()
        finish = time.time()

        duration = finish - begin
        duration_min = duration / 60
        duration_hr = duration_min / 60
        print("Processed {0} plate-appearances in {1} seconds.".format(total_ab, duration))
        print("Time per 1,000,000 plate-appearances: {:0.2f} minutes".format(duration_min * 1000000  / total_ab))
        print("Elo Ratings System With Params:\n\tK = {}\n\tlambda = {}\n\tbeta = {}\n\toffset = {}\nFinished with average log-loss {:0.6f}".format(elos_dict['K'],elos_dict['lambda'],elos_dict['beta'],elos_dict['offset'],elos_dict['loss']/total_ab))

def f_proc_tuple(start_date, end_date, hp):
        return sim_elos(start_date, end_date, CONN, None, *hp)[0]

def f_proc_dict(start_date, end_date, elos_dict):
        return sim_elos(start_date, end_date, CONN, elos_dict = elos_dict)[0]

def find_best_params():
        Ks = [0.0001,0.001,0.01]
        lambdas = [0.0001,0.001, 0.01]
        betas = [0.95, 0.9, 0.85, 0.8]
        offsets = [0,0.125,0.25]

        year = 2007
        output = None
        while (output is None or len(output) > 5) and year < 2019:
                start_date = datetime.date(year,1,1)
                end_date = datetime.date(year, 12, 31)
                if output is None:
                        hp = [t for t in it.product(Ks, lambdas, betas, offsets)]
                        inpt = zip([start_date for i in range(len(hp))],\
                                [end_date for i in range(len(hp))],\
                                hp)
                        f_proc = f_proc_tuple
                else:
                        inpt = zip([start_date for i in range(len(output))],\
                                [end_date for i in range(len(output))],\
                                output)
                        f_proc = f_proc_dict 

                print("===== NEXT ITERATION =====")
                with mp.Pool(processes = mp.cpu_count()) as pool:
                        output = pool.starmap(f_proc, inpt)
                output = sorted(output, key = lambda o: o['loss'], reverse = True)[:(len(output) // 2)]
                year += 1
        return output
        
if __name__ == "__main__":
        #out = find_best_params()
        #print("Found Best Parameters as:")
        #for o in out:
        #        print("K = {}, lambda = {}, beta = {}, offset = {} --> log-loss: {:0.4f}".format(o['K'], o['lambda'], o['beta'], o['offset'], o['loss']))
        main()
