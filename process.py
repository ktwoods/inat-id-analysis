import csv
from collections import defaultdict
from copy import copy
from glob import glob
import json
import re

import pandas as pd
import numpy as np
from dateutil.parser import isoparse
from rich import print
from rich.traceback import install
install(show_locals=True)
install(suppress=[pd, np])

def jload(fname) -> str:
    with open(fname) as f:
        js = json.load(f)
    return js

def jwrite(fname) -> None:
    with open(fname, 'w') as f:
        json.dump(f)


def del_fields(doc: dict, fields: str) -> dict:
    for field in fields.split(", "):
        if field in doc:
            del doc[field]
    # prune user object
    if 'user' in doc:
        doc['user'] = dict(id=doc['user']['id'], login=doc['user']['login'])
    return doc


# DEFUNCT?
def build_identifier_activity_key():
    # build int:list[dict] identifier key, with activity history indexed by user ID 
    IDer_key = {}
    user_key = jload('import final/users.json')
    action_names = 'identifications, comments, flags, votes'.split(", ")
    with open('import final/obs.json') as f:
        observations = json.load(f)
    # check activities attached to each observation
    for obs in observations:
        for act_name in action_names:
            for act_obj in obs[act_name]:
                user_id = act_obj['user']['id']
                log = dict(obs=obs['id'], time=act_obj['created_at'], action=act_name[:-1])

                if user_id not in IDer_key and act_name == 'identifications' and user_id != obs['user']['id']:
                    # update user entry and transfer to IDer_key
                    IDer_key[user_id] = user_key[user_id]
                    if 'coccinellidae_ids_count' not in IDer_key[user_id]:
                        IDer_key[user_id]['coccinellidae_ids_count'] = 0
                    if 'activity' not in IDer_key[user_id]:
                        IDer_key[user_id]['activity'] = []
                    IDer_key[user_id]['activity'].append(log)
                    IDer_key[user_id]['coccinellidae_ids_count'] += 1
                    del user_key[user_id]

                elif user_id not in IDer_key and act_name != 'identifications':
                    if 'activity' not in IDer_key[user_id]:
                        IDer_key[user_id]['activity'] = []
                    user_key[user_id]['activity'].append(log)
                
                elif user_id in IDer_key:
                    if act_name == 'identifications':
                        IDer_key[user_id]['coccinellidae_ids_count'] += 1
                    IDer_key[user_id]['activity'].append(log)
    del observations, obs, user_key

    # rearrange for pandas so that the count and activity log are user properties
    with open('import final/users.json') as f:
        users = json.load(f)
    IDer_list = []
    for user in users:
        if user['id'] in IDer_key:
            user['activity'] = IDer_key[user['id']]
            user['coccinellidae_ids_count'] = IDer_key[user['id']]
            IDer_list.append(user)
    del users

    for user in IDer_list:
        try:
            user['activity'].sort(key=lambda a: isoparse(a['time']))
        except Exception as e:
            print(f"Problem with user {user_id}:", e)
            # one error, "can't compare offset-naive and offset-aware datetimes"; problem appears to be with "2021-09-09T01:48:05"? it's not even an ID it's a vote, just ignore it
    with open('import final/identifiers_pandas2.json', 'w') as f:
        json.dump(IDer_list, f, indent=2)


def ids_to_csv():
    print("Loading file")
    observations = jload('observations/obs.json')

    with open('idents_expanded.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        print("Scanning")
        writer.writerow(['observation', 'identifier', 'username', 'date', 'taxon_id', 'taxon', 'rank', 'rank_level', 'previous_taxon_id', 'current', 'disagreement', 'category', 'vision', 'hidden', 'latitude', 'longitude', 'places'])
        for obs in observations:
            for id in obs['identifications']:
                # Ignore IDs that observers are adding as part of their own submission
                if id['user']['id'] != obs['user']['id']:
                    writer.writerow([obs['id'], 
                                    id['user']['id'],
                                    id['user']['login'],
                                    id['created_at'],
                                    id['taxon']['id'],
                                    id['taxon']['name'],
                                    id['taxon']['rank'],
                                    id['taxon']['rank_level'],
                                    id['previous_observation_taxon_id'],
                                    id['current'],
                                    id['disagreement'],
                                    id['category'],
                                    id['vision'],
                                    id['hidden'],
                                    obs['geospatial']['geojson']['coordinates'][0],
                                    obs['geospatial']['geojson']['coordinates'][1],
                                    obs['geospatial']['place_ids']])
        print("Saving")


def obs_to_csv():
    print("Loading file")
    observations = jload('observations/obs.json')
    with open('observations.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        print("Scanning")
        writer.writerow(['obs_id', 'user_id', 'created_on', 'observed_on', 'updated_on', 'quality_grade', 'rank', 'rank_level', 'taxon_id', 'geojson', 'place_ids'])
        for obs in observations:
            writer.writerow([obs['id'], 
                            obs['user']['id'],
                            obs['created_at'],
                            obs['observed_on'],
                            obs['updated_at'],
                            obs['quality_grade'],
                            obs['taxon']['rank'],
                            obs['taxon']['rank_level'],
                            obs['taxon']['id'],
                            obs['geospatial']['geojson'],
                            obs['geospatial']['place_ids']])
        print("Saving")


# ids_to_csv()


def count_objects():
    # Primary datasets:
    # OBSERVATIONS: 
    print(len(jload('observations/obs.json')), "total observations of Coccinellidae downloaded")
    # IDENTIFICATIONS: 'identifications.csv'
    print("426,528 total identifications on those observations (excluding self-IDs)")
    print("421,059 identifications for Coccinellidae and below")
    # IDENTIFIERS: ('stats.csv')
    print("11,743 identifiers for Coccinellidae and below")
    print("Top 10 most prolific: 65,109 / 37,487 / 19,244 / etc")

    # TAXA: 'coccinellidae.csv'
    print(len(pd.read_csv('coccinellidae.csv')), "taxa within Coccinellidae")

    # also 'stats.csv'
    # 'coccinellidae.csv' = worldwide taxa
    # 'identifications.csv' = total identifiers

count_objects()


# this will take a sec
def build_stats_table(ids, identifier_stats):
    # acts on each row of the de-duplicated ids
    def generate_stats(row):
        rank = row['rank']
        if rank not in identifier_stats.columns:
            rank = 'species'
        identifier = identifier_stats.loc[row['identifier']]
        identifier['username'] = row['username']
        identifier[rank] += 1
        identifier['total'] += 1
        identifier_stats.loc[row['identifier']] = identifier
    #     print(identifier_stats.loc[row['identifier']])

    # acts on each row of identifier_stats
    def generate_proportions(row):
        try:
            row['frac_species'] = row['species']/row['total']
            row['frac_genus'] = row['genus']/row['total']
            row['frac_tribe'] = row['tribe']/row['total']
            row['frac_subfamily'] = row['subfamily']/row['total']
            row['frac_family'] = row['family']/row['total']
        except ZeroDivisionError:
            print("Error processing", row.name, row['username'], "- total is 0")
            identifier_stats.to_csv('stats.csv')
            exit()
        return row

    # for calculating who's the most prolific, we want each observation to count just once per identifier
    # keep the most recent ID for its rank, in case of genuine revisions
    print("Sorting by date...")
    ids_nodup = ids.sort_values(by='date')
    print("Dropping duplicates...")
    ids_nodup.drop_duplicates(subset=['identifier', 'observation'], keep='last', ignore_index=True, inplace=True)
    # print(ids_nodup[34728])
    # print(identifier_stats.loc[34728])
    
    print("Counting...")
    # use ids array to update the counts
    ids_nodup.apply(generate_stats, axis='columns')

    # add proportions
    identifier_stats = identifier_stats.apply(generate_proportions, axis='columns')

    identifier_stats.sort_values(by='total', ascending=False, inplace=True)
    identifier_stats['total'] = identifier_stats['total'].astype(int)
    identifier_stats['species'] = identifier_stats['species'].astype(int)
    identifier_stats['genus'] = identifier_stats['genus'].astype(int)
    identifier_stats['tribe'] = identifier_stats['tribe'].astype(int)
    identifier_stats['subfamily'] = identifier_stats['subfamily'].astype(int)
    identifier_stats['family'] = identifier_stats['family'].astype(int)

    # export
    identifier_stats.to_csv('stats.csv')


def cocci_id_stats_to_csv():
    ids = pd.read_csv('identifications.csv')
    taxonomy = pd.read_csv('coccinellidae.csv')

    # only look at identifiers who're working within Coccinellidae
    cocci_fam_and_below = taxonomy[taxonomy.level <= 30]
    orig_len = len(ids)
    ids = ids.loc[ids['taxon_id'].isin(cocci_fam_and_below['id'])]
    print(orig_len - len(ids), "extraneous identifiers removed")

    ids_nodup = ids.sort_values(by='date')
    ids_nodup.drop_duplicates(subset=['identifier', 'observation'], keep='last', ignore_index=True, inplace=True)

    # initialize stats array
    identifier_ids = ids_nodup['identifier'].unique()
    columns = ['total', 'frac_species', 'species', 'frac_genus', 'genus', 'frac_tribe', 'tribe', 'frac_subfamily', 'subfamily', 'frac_family', 'family']
    shape = (len(identifier_ids), len(columns))
    identifier_stats = pd.DataFrame(np.zeros(shape), index=identifier_ids, columns=columns)
    identifier_stats.insert(0, 'username', 'n/a')

    build_stats_table(ids_nodup, identifier_stats)


# cocci_id_stats_to_csv()




# 2022-09-31, 6 months after final obs creation date
def date_cutoff(cutoff='2022-09-31'):
    with open('import final/obs.json') as f:
        observations = json.load(f)
    for obs in observations:
        non_id = 'comments, flags, votes'.split(", ")
        for log in obs[non_id]:
            if 'created_at' in log and log['created_at'][:10] > cutoff:
                del log
        for id in reversed(obs['identifications']):
            if id['created_at'][:10] > cutoff:
                # TODO
                pass
