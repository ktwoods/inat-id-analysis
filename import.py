import csv
from glob import glob
import json
from math import ceil
from os import mkdir
from os.path import exists, basename

from pyinaturalist import get_observations, get_taxa, get_taxa_by_id
from rich import print
from rich.prompt import Confirm

# Main process:
# 1.  import_obs() 
# 2.  prune_obs_folder() to reduce file size and reclaim disk space by eliminating redundant data
# 3.  repeat 1 & 2 as needed until out of pages to fetch
# 4.  merge_final() to combine into the three final collections
# 5.  import_ancestry()

def jload(fname):
    with open(fname) as f:
        js = json.load(f)
    return js

def jwrite(data, fname) -> None:
    with open(fname, 'w') as f:
        json.dump(data, f, indent=2, default=str)


# Fetches observations from API and writes them to disk as JSON files in 20k-obs increments
def import_obs(filters=dict(created_d2="2022-03-31", taxon_id=48486, place_id=66741), start_from_id=None, fname_prefix='obs'):
    OBS_PER_PAGE = 200
    PAGES_PER_FILE = 100

    if exists(fname_prefix):
        n = 2
        while exists(fname_prefix + str(n)):
            n += 1
        fname_prefix += str(n)
    mkdir(fname_prefix)
    fname_prefix = fname_prefix + "/" + fname_prefix
    
    try:
        pgcount = 1
        print(f"Requesting page {pgcount}...", end=" ")
        page = get_observations(**filters, verifiable=True, per_page=OBS_PER_PAGE, order_by='id', order='asc', id_above=start_from_id)
        obs = page['results']
        print(f"First {len(obs)} observations retrieved, checking total...")

        num_requests = ceil(page['total_results']/OBS_PER_PAGE)
        if num_requests > 1:
            print(f"Fetching all {page['total_results']} results will require {num_requests} API requests;", end=" ")
            if not Confirm.ask("continue?"):
                page['total_results'] = 0
        
        while page['total_results'] > OBS_PER_PAGE:
            pgcount += 1
            last_id = page['results'][-1]['id']
            print(f"Requesting page {pgcount} (IDs after {last_id})...", end=" ")
            page = get_observations(**filters, verifiable=True, per_page=OBS_PER_PAGE, order_by='id', order='asc', id_above=last_id)
            print(f"{len(page['results'])} observations retrieved")
            obs.extend(page['results'])
            # Write to file and free up memory space every 100 pages
            if pgcount % PAGES_PER_FILE == 0:
                fname = f"{fname_prefix}_{pgcount // PAGES_PER_FILE}.json"
                print(f"Saving progress as '{fname}'...")
                with open(fname, 'w') as f:
                    json.dump(obs, f, indent=2, default=str)
                del obs
                obs = []

    except KeyboardInterrupt:
        print("\nExiting...", end="")
    finally:
        fname = f"{fname_prefix}_{ceil(pgcount / PAGES_PER_FILE)}.json"
        with open(fname, 'w') as f:
            json.dump(obs, f, indent=2, default=str)
        print(f"\n{len(obs)} observations saved as '{fname}'")



# (helper) Adds to the ongoing list, and returns a minimum stub for the parent data object
def prune_taxon(orig, taxon_key):
    if orig['id'] not in taxon_key:
        taxon_fields = 'id, current_synonymous_taxon_ids, ancestor_ids, name, preferred_common_name, rank, rank_level, atlas_id, endemic, threatened, native, introduced, is_active, created_at, observations_count, complete_species_count'
        new_taxon = {}
        for field in taxon_fields.split(", "):
            if field in orig:
                new_taxon[field] = orig[field]
        taxon_key[orig['id']] = new_taxon
    return dict(id=orig['id'], name=orig['name'], rank=orig['rank'], rank_level=orig['rank_level'], observations_count=orig['observations_count'])


# (helper)
def drop_fields(doc: dict, fields: str, hard_prune_user=True) -> dict:
    for field in fields.split(", "):
        if field in doc:
            del doc[field]
    # prune user object, if present
    if 'user' in doc:
        user = doc['user']
        if hard_prune_user:
            doc['user'] = dict(id=user['id'], login=user['login'])
        else:
            user_fields = 'id, login, created_at, roles, observations_count, identifications_count, journal_posts_count, species_count'
            new_user = {}
            for field in user_fields.split(", "):
                if field in user:
                    new_user[field] = user[field]
            doc['user'] = new_user
    return doc


# (helper) Drops irrelevant fields and splits off users and taxa into separate collections for less duplication
def prune_file(fname):
    print(f"Loading '{basename(fname)}'")
    observations = jload(fname)
    print("Processing...")

    identifier_key = {}
    observer_key = {}
    taxon_key = {}
    for obs in observations:
        # Just save number of photos, for now
        obs['num_photos'] = len(obs['photos'])
        # Drop irrelevant fields
        obs = drop_fields(obs, 'uuid, photos, captive, sounds, faves, faves_count, time_zone_offset, observed_on_string, observed_on_details, observed_time_zone, created_time_zone, uri, observation_photos, oauth_application_id, observed_on_details, created_at_details, non_owner_ids, location, project_ids, project_ids_with_curator_id, project_ids_without_curator_id, project_observations, ident_taxon_ids, identifications_count, comments_count, id_please, site_id, preferences, outlinks, license_code, spam', hard_prune_user=False)
        observer_key[obs['user']['id']] = obs['user']
        obs['user'] = drop_fields(obs['user'], '')
        # Group geospatial fields
        geospatial_fields = 'geojson, positional_accuracy, public_positional_accuracy, obscured, geoprivacy, taxon_geoprivacy, context_user_geoprivacy, place_ids, place_guess, mappable, map_scale'
        obs['geospatial'] = {}
        for field in geospatial_fields.split(", "):
            if field in obs:
                obs['geospatial'][field] = obs[field]
                del obs[field]
        # Taxon
        obs['taxon'] = prune_taxon(obs['taxon'], taxon_key)
        # Identifications: trim info, replace user and taxon objects
        for ident in obs['identifications']:
            if not ident['own_observation']:
                ident = drop_fields(ident, 'uuid, own_observation, created_at_details', hard_prune_user=False)
                identifier_key[ident['user']['id']] = ident['user']
                ident['user'] = drop_fields(ident['user'], '')
            else:
                ident = drop_fields(ident, 'uuid, own_observation, created_at_details')
            ident['taxon'] = prune_taxon(ident['taxon'], taxon_key)
            if 'previous_observation_taxon' in ident:
                ident['previous_observation_taxon'] = prune_taxon(ident['previous_observation_taxon'], taxon_key)
        # Comments
        for com in obs['comments']:
            com = drop_fields(com, 'uuid, id, login, moderator_actions, flags, created_at_details, previous_observation_taxon_id')
        # Annotations
        for an in obs['annotations']:
            an = drop_fields(an, 'uuid, controlled_value_id, controlled_attribute_id, vote_score')
            an['votes'] = drop_fields(an['votes'], '')
        # Flags
        for flag in obs['flags']:
            flag = drop_fields(flag, '')
        # Ofvs
        for obs_field in obs['ofvs']:
            obs_field = drop_fields(obs_field, 'id, uuid, name_ci, value_ci')
            if 'taxon' in obs_field:
                obs_field['taxon'] = obs_field['taxon']['id']
        # Votes
        for vote in obs['votes']:
            vote = drop_fields(vote, 'id')
        # Quality metrics
        for metric in obs['quality_metrics']:
            metric = drop_fields(metric, 'user_id, id')
    return observations, identifier_key, observer_key, taxon_key


# Prunes each file in this directory and puts the results in "<path>_condensed", combining their contents into one set of four files
def prune_obs_folder(path):
    # Create as a separate set of files, don't overwrite
    write_dir = path + '_condensed'
    if exists(write_dir):
        print("Folder '" + basename(path) + "_condensed' already exists in this directory.", end=" ")
        if not Confirm.ask("Do you want to overwrite its contents?"):
            exit()
    else:
        mkdir(write_dir)

    fnames = sorted(glob(path+"/*.json"))
    observations = []
    identifier_key = {}
    observer_key = {}
    taxon_key = {}
    for name in fnames:
        if basename(name)[-6:] != '_clean':
            obs, identifiers, observers, taxa = prune_file(name)
            observations.extend(obs)
            identifier_key = {**identifier_key, **identifiers}
            observer_key = {**observer_key, **observers}
            taxon_key = {**taxon_key, **taxa}

    print("Re-sorting observations by ID...")
    observations.sort(key=lambda obs: int(obs['id']))
    print("Writing taxa to disk...")
    jwrite(taxon_key, write_dir + '/taxa.json')
    del taxon_key
    print("Writing identifiers to disk...")
    jwrite(identifier_key, write_dir + '/identifiers.json')
    del identifier_key
    print("Writing observers to disk...")
    jwrite(observer_key, write_dir + '/observers.json')
    del observer_key
    print("Writing observations to disk...")
    jwrite(observations, write_dir + '/obs.json')
    del observations
    

def merge_final():
    fnames = glob("obs/*/*.json")
    obs = []
    taxa = {}
    users = {}
    for name in fnames:
        with open(name) as f:
            n = basename(name).split('.')[0]
            if n == 'taxa':
                taxa = {**taxa, **json.load(f)}
            elif n == 'users':
                users = {**users, **json.load(f)}
            else:
                obs.extend(json.load(f))
    if not exists("Coccinellidae"): mkdir("Coccinellidae")
    with open("Coccinellidae/obs.json", 'w') as f:
        json.dump(obs, f, indent=2)
    with open("Coccinellidae/taxa.json", 'w') as f:
        json.dump(taxa, f, indent=2)
    with open("Coccinellidae/users.json", 'w') as f:
        json.dump(users, f, indent=2)


# get descendants
coccinellidae = get_taxa(taxon_id=48486, page='all')['results']
# get ancestry
coccinellidae.extend(get_taxa_by_id([48460,1,47120,372739,47158,184884,47208,71130,372852,471714])['results'])
with open('coccinellidae.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    print("Scanning")
    writer.writerow(['id', 'synonyms', 'parent', 'name', 'common', 'rank', 'level', 'active', 'obs_worldwide', 'num_species'])
    for tax in coccinellidae:
        parent = tax['ancestor_ids'][-2] if len(tax['ancestor_ids']) > 1 else 0
        common = tax.get('preferred_common_name', tax['name'])
        writer.writerow([tax['id'], tax['current_synonymous_taxon_ids'], parent, tax['name'], common, tax['rank'], tax['rank_level'], tax['is_active'], tax['observations_count'], tax['complete_species_count']])


    # for id,taxon in taxa.items():
    #     for ancestor in taxon['ancestor_ids']:
    #         if ancestor not in taxa and ancestor not in to_fetch:
    #             to_fetch.add(ancestor)
    #         else:
    #             already_have += 1
    # print(f"{len(to_fetch)} taxa needed.", end=" ")
    # if Confirm.ask("View?"):
    #     print(to_fetch)
    # else:
    #     with open('fetch_taxa.json', 'w') as f:
    #         json.dump(sorted(list(to_fetch)), f)


# (useful args for testing:)
# defaults: dict(created_d2="2022-03-31", taxon_id=48486, place_id=66741)
# 463 results: dict(taxon_id=48486, place_id=48816)

# import_obs(start_from_id=84868113, fname_prefix='obs3')
