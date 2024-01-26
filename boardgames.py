# Import libraries
import csv
from bs4 import BeautifulSoup
import requests
import time
import os
import pandas as pd

# CSV file saving function
def save_to_csv(filename, rows, csv_header):
    with open(filename, 'a', encoding='UTF8') as f:
        dictwriter_object = csv.DictWriter(f, fieldnames=csv_header)
        if f.tell() == 0:
            dictwriter_object.writeheader()
        dictwriter_object.writerows(rows)

# Define request url headers
headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Accept-Language": "en-GB, en-US, q=0.9, en"
}

# Define sleep timer value between requests
SLEEP_BETWEEN_REQUEST = 5

# Define file path to store boardgames info
bgg_data_folder = 'data/'
bgg_file_path = bgg_data_folder+'boardgames.csv'
bgg_detail_file_path = bgg_data_folder+'boardgames_details.csv'

link_type_exclusions = ["boardgameaccessory", "boardgameversion", "language", "boardgamecompilation", "boardgameexpansion", "boardgamefamily", "boardgameimplementation", "boardgameintegration"]

# Check if the file exists
bgg_ids_file_path = bgg_data_folder+'bgg_game_ids.csv'
if not os.path.exists(bgg_ids_file_path):
    print("File with boardgame_ids doesn't exist!")
else:
    # Read game_ids from the file
    with open(bgg_ids_file_path, 'r') as ids_file:
        game_ids = [line.strip() for line in ids_file]

    # Define batch_size
    batch_size = 500
    total_games = len(game_ids)
    # total_games = 6  # For testing purposes
    base_url = "https://boardgamegeek.com/xmlapi2/thing?id="

    games_header = [
        'game_id', 'name', 'type', 'year_published', 'min_players', 'max_players', 'min_play_time', 'max_play_time', 'min_age', 'thumbnail'
    ]

    # Read existing game IDs from the bgg.csv file
    existing_game_ids = set()
    if os.path.exists(bgg_file_path):
        df_existing = pd.read_csv(bgg_file_path)
        existing_game_ids.update(df_existing['game_id'])

    for batch_start in range(0, total_games, batch_size):
        # Extract a batch of game_ids
        batch_ids = game_ids[batch_start:batch_start + batch_size]
        # ids = [376696, 374871, 374730, 372211, 370401, 246018]  # For testing purposes

        # Join and append to the URL the IDs within batch size
        ids = ",".join(batch_ids)
        url = f"{base_url}{ids}&stats=1&versions=1"

        # If by any chance there is an error, this will throw the exception and continue to the next batch
        try:
            response = requests.get(url, headers=headers)

            if response.status_code == 200:
                soup = BeautifulSoup(response.text, features="html.parser")
                items = soup.find_all("item")
                games = {}
                games_detail = []
                link_type_data = {}
                version_link_type_data = {}

                if not items:
                    print(f">>> No more items. Exiting.")
                    break

                for item in items:
                    if item['type'] == 'boardgame':
                        game_id = int(item['id'])

                        try:
                            # Find values in the XML for boardgames_details
                            avg_rating = item.find("average")['value'] if item.find("average") is not None else 0
                            avg_bayes_rating = item.find("bayesaverage")['value'] if item.find("bayesaverage") is not None else 0
                            num_users_rated = item.find("usersrated")['value'] if item.find("usersrated") is not None else 0
                            weight = item.find("averageweight")['value'] if item.find("averageweight") is not None else 0
                            owned = item.find("owned")['value'] if item.find("owned") is not None else 0

                            # Extract suggested_numplayers poll results
                            suggested_numplayers_poll = item.find('poll', {'name': 'suggested_numplayers'})
                            if int(suggested_numplayers_poll['totalvotes']) != 0:
                                results = suggested_numplayers_poll.find_all('results')
                                # Find numplayers with the most total numvotes for value="Best"
                                best_numplayers = max(results, key=lambda x: int(x.find('result', {'value': 'Best'})['numvotes']))
                                best_numplayers_value = best_numplayers['numplayers']
                                 # Find numplayers with the most total numvotes for value="Not Recommended"
                                not_recommended_numplayers = max(results, key=lambda x: int(x.find('result', {'value': 'Not Recommended'})['numvotes']))
                                not_recommended_numplayers_value = not_recommended_numplayers['numplayers']
                            else:
                                best_numplayers_value = 0
                                not_recommended_numplayers_value = 0

                            # Extract language_dependence poll results
                            language_dependence_poll = item.find('poll', {'name': 'language_dependence'})
                            if int(language_dependence_poll['totalvotes']) != 0:
                                results = language_dependence_poll.find_all('result')

                                # Find language dependence value with the most total numvotes
                                best_language_dependence = max(results, key=lambda x: int(x['numvotes']))
                                best_language_dependence_value = best_language_dependence['value']
                                best_language_dependence_votes = best_language_dependence['numvotes']
                            else:
                                best_language_dependence_value = "Unknown"

                            ranks = item.find_all("rank")

                            # Append value(s) for each rank type
                            for rank in ranks:
                                rank_type = rank['name']
                                rank_value = rank['value']
                                game_detail = {
                                "game_id": item['id'],
                                "rank_type": rank_type,
                                "rank": rank_value,
                                "best_num_players": best_numplayers_value,
                                "not_recommended_num_players": not_recommended_numplayers_value,
                                "language_dependency": best_language_dependence_value,
                                "avg_rating": avg_rating,
                                "avg_bayes_rating": avg_bayes_rating,
                                "users_rated": num_users_rated,
                                "avg_weight": weight,
                                "total_owners": owned,
                                "updated_at": time.strftime("%Y-%m-%d %H:%M:%S")
                                }
                                games_detail.append(game_detail)

                        except TypeError:
                            print(game_id)
                            print(">>> NoneType error. Continued on the next item.")
                            continue

                        # Check if the game ID is already processed
                        if game_id in existing_game_ids:
                            print(f">>> Skipping game {game_id} as it's already processed.")
                            continue
                        try:
                            # Find values in the XML
                            name = item.find("name")['value'] if item.find("name") is not None else 0
                            year_published = item.find("yearpublished")['value'] if item.find("yearpublished") is not None else 0
                            min_players = item.find("minplayers")['value'] if item.find("minplayers") is not None else 0
                            max_players = item.find("maxplayers")['value'] if item.find("maxplayers") is not None else 0
                            min_play_time = item.find("minplaytime")['value'] if item.find("minplaytime") is not None else 0
                            max_play_time = item.find("maxplaytime")['value'] if item.find("maxplaytime") is not None else 0
                            min_age = item.find("minage")['value'] if item.find("minage") is not None else 0
                            thumbnail = item.find("thumbnail").text if item.find("thumbnail") is not None else 0

                            links = item.find_all("link")


                            # Append value(s) for each link type
                            for link in links:
                                link_type_name = link['type']
                                link_type_value = link['value']
                                link_type_id = link['id']

                                if link_type_name not in link_type_exclusions:

                                    if link_type_name not in link_type_data:
                                        link_type_data[link_type_name] = []

                                    link_type_data[link_type_name].append({
                                        "game_id": item['id'],
                                        "id": link_type_id,
                                        "value": link_type_value
                                    })
    

                            # Find all boardgameversion items inside the boardgame item
                            boardgameversion_items = item.find_all('item', {'type': 'boardgameversion'})

                            for boardgameversion_item in boardgameversion_items:
                                boardgame_version_id = boardgameversion_item['id']

                                # Extract links from the boardgameversion item
                                version_links = boardgameversion_item.find_all("link")
                                # Append value(s) for each link type
                                for version_link in version_links:
                                    if version_link['type'] != "boardgameversion":
                                        version_link_type_name = version_link['type']
                                        version_link_type_value = version_link['value']
                                        version_link_type_id = version_link['id']

                                        if boardgameversion_item['type'] not in version_link_type_data:
                                            version_link_type_data[boardgameversion_item['type']] = []

                                        version_link_type_data[boardgameversion_item['type']].append({
                                            "game_id": item['id'],
                                            "boardgame_version_id": boardgameversion_item['id'],
                                            version_link_type_name: version_link_type_value,
                                            f"{version_link_type_name}_id": version_link['id']
                                        })

                            game = {
                                "game_id": item['id'],
                                "name": name,
                                "type": item['type'],
                                "year_published": year_published,
                                "min_players": min_players,
                                "max_players": max_players,
                                "min_play_time": min_play_time,
                                "max_play_time": max_play_time,
                                "min_age": min_age,
                                'thumbnail': thumbnail
                            }

                            # Append current item to games dictionary
                            games[item['id']] = game

                            # Find values in the XML for boardgames_details
                            avg_rating = item.find("average")['value'] if item.find("average") is not None else 0
                            avg_bayes_rating = item.find("bayesaverage")['value'] if item.find("bayesaverage") is not None else 0
                            num_users_rated = item.find("usersrated")['value'] if item.find("usersrated") is not None else 0
                            weight = item.find("averageweight")['value'] if item.find("averageweight") is not None else 0
                            owned = item.find("owned")['value'] if item.find("owned") is not None else 0

                            # Extract suggested_numplayers poll results
                            suggested_numplayers_poll = item.find('poll', {'name': 'suggested_numplayers'})
                            if int(suggested_numplayers_poll['totalvotes']) != 0:
                                results = suggested_numplayers_poll.find_all('results')
                                # Find numplayers with the most total numvotes for value="Best"
                                best_numplayers = max(results, key=lambda x: int(x.find('result', {'value': 'Best'})['numvotes']))
                                best_numplayers_value = best_numplayers['numplayers']
                                 # Find numplayers with the most total numvotes for value="Not Recommended"
                                not_recommended_numplayers = max(results, key=lambda x: int(x.find('result', {'value': 'Not Recommended'})['numvotes']))
                                not_recommended_numplayers_value = not_recommended_numplayers['numplayers']
                            else:
                                best_numplayers_value = 0
                                not_recommended_numplayers_value = 0

                            # Extract language_dependence poll results
                            language_dependence_poll = item.find('poll', {'name': 'language_dependence'})
                            if int(language_dependence_poll['totalvotes']) != 0:
                                results = language_dependence_poll.find_all('result')

                                # Find language dependence value with the most total numvotes
                                best_language_dependence = max(results, key=lambda x: int(x['numvotes']))
                                best_language_dependence_value = best_language_dependence['value']
                                best_language_dependence_votes = best_language_dependence['numvotes']
                            else:
                                best_language_dependence_value = "Unknown"

                            ranks = item.find_all("rank")

                            # Append value(s) for each rank type
                            for rank in ranks:
                                rank_type = rank['name']
                                rank_value = rank['value']
                                game_detail = {
                                "game_id": item['id'],
                                "rank_type": rank_type,
                                "rank": rank_value,
                                "best_num_players": best_numplayers_value,
                                "not_recommended_num_players": not_recommended_numplayers_value,
                                "language_dependency": best_language_dependence_value,
                                "avg_rating": avg_rating,
                                "avg_bayes_rating": avg_bayes_rating,
                                "users_rated": num_users_rated,
                                "avg_weight": weight,
                                "total_owners": owned,
                                "updated_at": time.strftime("%Y-%m-%d %H:%M:%S")
                                }
                                games_detail.append(game_detail)

                        except TypeError:
                            print(game["game_id"])
                            print(">>> NoneType error. Continued on the next item.")
                            continue

                save_to_csv(bgg_file_path, list(games.values()), games_header)  # Save the common data to bgg.csv
                save_to_csv(bgg_detail_file_path, games_detail, games_detail[0].keys())  # Save the detail data to bgg_detail.csv

                # Save to CSV based on link type
                for link_type_name, link_type_value in link_type_data.items():
                    link_type_filename = bgg_data_folder+f'bgg_{link_type_name}.csv'
                    link_type_header = link_type_value[0].keys()
                    save_to_csv(link_type_filename, link_type_value, link_type_header)

                # Save to CSV based on link type
                for version_link_type_name, version_link_type_value in version_link_type_data.items():
                    version_link_type_filename = bgg_data_folder+f'bgg_{version_link_type_name}.csv'
                    version_link_type_header = set(key for d in version_link_type_value for key in d.keys())
                    save_to_csv(version_link_type_filename, version_link_type_value, version_link_type_header)

                print(f">>> Request successful for batch {batch_start}-{batch_start + batch_size - 1}")
            else:
                print(f">>> FAILED batch {batch_start}-{batch_start + batch_size - 1}")
        except Exception as err:
            print(f">>> Error: {err}. Continuing to the next batch.")

        # Pause between requests
        time.sleep(SLEEP_BETWEEN_REQUEST)
